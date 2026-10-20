import json
import logging
import time
from decimal import Decimal, InvalidOperation

from config import (
    MAIN_QUEUE,
    MAX_DELIVERIES,
    RETRY_DELAY_SECONDS,
    create_connection,
)
from topology import declare_topology

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("consumer")


def parse_pedido(body: bytes) -> tuple[int, str, Decimal]:
    data = json.loads(body.decode("utf-8"))
    return int(data["id_pedido"]), str(data["cliente"]), Decimal(str(data["valor"]))


def failure_reason(valor: Decimal) -> str | None:
    if valor < 0:
        return "valor negativo"
    if valor == valor.to_integral_value() and int(valor) % 2 == 0:
        return "valor inteiro par"
    return None


def delivery_attempt(properties) -> int:
    """Obtém o número desta entrega a partir dos headers do RabbitMQ."""
    headers = properties.headers or {}

    # x-acquired-count existe no RabbitMQ >= 4.3 e conta cada entrega,
    # inclusive NACK com requeue. x-delivery-count é o fallback para versões
    # anteriores, nas quais NACK também incrementava esse contador.
    # O header só aparece a partir da segunda entrega e contém a quantidade
    # de entregas anteriores; por isso somamos 1 para obter a tentativa atual.
    entregas_anteriores = headers.get("x-acquired-count")
    if entregas_anteriores is None:
        entregas_anteriores = headers.get("x-delivery-count")

    return int(entregas_anteriores or 0) + 1


def reject_message(channel, method, properties, id_pedido, motivo: str) -> None:
    tentativa = delivery_attempt(properties)

    if tentativa >= MAX_DELIVERIES:
        logger.error(
            "[DLQ] id_pedido=%s tentativa=%s/%s motivo=%s; "
            "NACK(requeue=False) encaminha para a DLQ.",
            id_pedido,
            tentativa,
            MAX_DELIVERIES,
            motivo,
        )
        # A mensagem não volta para a fila principal: segue para o DLX e,
        # por meio do binding, para a fila notificacoes.dlq.
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    logger.warning(
        "[NACK] id_pedido=%s tentativa=%s/%s motivo=%s; "
        "NACK(requeue=True) solicita redelivery.",
        id_pedido,
        tentativa,
        MAX_DELIVERIES,
        motivo,
    )
    # Falha proposital: NACK com requeue=True devolve a mensagem à fila para
    # demonstrar o redelivery.
    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    time.sleep(RETRY_DELAY_SECONDS)


def callback(channel, method, properties, body: bytes) -> None:
    try:
        id_pedido, cliente, valor = parse_pedido(body)
        motivo = failure_reason(valor)
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        reject_message(
            channel,
            method,
            properties,
            id_pedido="desconhecido",
            motivo=f"mensagem inválida: {exc!r}",
        )
        return

    if motivo:
        logger.warning(
            "Falha simulada: id_pedido=%s cliente=%s valor=%s tentativa=%s.",
            id_pedido,
            cliente,
            valor,
            delivery_attempt(properties),
        )
        reject_message(channel, method, properties, id_pedido, motivo)
        return

    logger.info(
        "[ACK] id_pedido=%s cliente=%s valor=%s tentativa=%s processado.",
        id_pedido,
        cliente,
        valor,
        delivery_attempt(properties),
    )
    # Acknowledgement manual: só removemos a mensagem da fila após o
    # processamento ter sido concluído com sucesso.
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    connection = create_connection()
    try:
        channel = connection.channel()
        declare_topology(channel)

        # Com prefetch=1 o broker entrega uma mensagem por vez e só envia a
        # próxima após o ACK/NACK. Isso torna o redelivery fácil de observar.
        channel.basic_qos(prefetch_count=1)

        # auto_ack=False é obrigatório para o acknowledgement manual: a
        # mensagem permanece pendente até basic_ack ou basic_nack.
        channel.basic_consume(
            queue=MAIN_QUEUE,
            on_message_callback=callback,
            auto_ack=False,
            exclusive=False,
            consumer_tag="consumer-notificacoes",
        )

        logger.info(
            "Aguardando mensagens em '%s' (quorum, limite=%s tentativas). "
            "Ctrl+C para encerrar.",
            MAIN_QUEUE,
            MAX_DELIVERIES,
        )
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Encerrando consumer...")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
