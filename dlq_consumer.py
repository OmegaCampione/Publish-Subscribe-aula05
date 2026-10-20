import json
import logging

from config import DLQ_QUEUE, create_connection
from topology import declare_topology

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("dlq-consumer")


def callback(channel, method, properties, body: bytes) -> None:
    headers = properties.headers or {}
    x_death = headers.get("x-death", [])

    try:
        data = json.loads(body.decode("utf-8"))
        id_pedido = data.get("id_pedido", "desconhecido")
        cliente = data.get("cliente", "desconhecido")
        valor = data.get("valor", "desconhecido")
    except (UnicodeDecodeError, json.JSONDecodeError):
        id_pedido = "desconhecido"
        cliente = "desconhecido"
        valor = "desconhecido"

    logger.warning(
        "[DLQ] id_pedido=%s cliente=%s valor=%s x-death=%s",
        id_pedido,
        cliente,
        valor,
        json.dumps(x_death, ensure_ascii=False, default=str),
    )

    # Acknowledgement manual também na DLQ: a mensagem só é removida após
    # ser lida e registrada.
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    connection = create_connection()
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
            queue=DLQ_QUEUE,
            on_message_callback=callback,
            auto_ack=False,
            consumer_tag="consumer-dlq",
        )

        logger.info("Aguardando mensagens na DLQ '%s'. Ctrl+C para encerrar.", DLQ_QUEUE)
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Encerrando consumidor da DLQ...")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
