import json
import logging
from decimal import Decimal, InvalidOperation

from config import FANOUT_EXCHANGE, create_connection
from topology import declare_fanout_exchange

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("consumer.notificacoes")


def parse_pedido(body: bytes) -> tuple[int, str, Decimal]:
    data = json.loads(body.decode("utf-8"))
    return int(data["id_pedido"]), str(data["cliente"]), Decimal(str(data["valor"]))


def callback(channel, method, properties, body: bytes) -> None:
    try:
        id_pedido, cliente, valor = parse_pedido(body)
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        logger.error(
            "[Notificações] Mensagem inválida descartada: erro=%r corpo=%r",
            exc,
            body,
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    logger.info(
        "[Notificações] Processando id_pedido=%s cliente=%s valor=%s",
        id_pedido,
        cliente,
        valor,
    )

    # Acknowledgement manual: a cópia desta fila só é removida após o
    # processamento do domínio de notificações.
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    connection = create_connection()
    try:
        channel = connection.channel()
        declare_fanout_exchange(channel)

        # Fila temporária, exclusiva e com nome gerado pelo RabbitMQ.
        # Ela é removida automaticamente quando esta conexão terminar.
        result = channel.queue_declare(
            queue="",
            exclusive=True,
            auto_delete=True,
        )
        queue_name = result.method.queue

        # O binding sem routing key faz esta fila receber todas as cópias
        # publicadas no exchange fanout.
        channel.queue_bind(
            queue=queue_name,
            exchange=FANOUT_EXCHANGE,
            routing_key="",
        )
        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=False,
            consumer_tag="consumer-notificacoes-fanout",
        )

        logger.info(
            "[Notificações] Aguardando eventos na fila temporária '%s' "
            "(exchange '%s'). Ctrl+C para encerrar.",
            queue_name,
            FANOUT_EXCHANGE,
        )
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("[Notificações] Encerrando consumer...")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
