import argparse
import json
import logging
import uuid
from decimal import Decimal, InvalidOperation

import pika

from config import MAIN_EXCHANGE, MAIN_ROUTING_KEY, create_connection
from topology import declare_topology

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("producer")

DEMO_MESSAGES = (
    {"id_pedido": 123, "cliente": "Ana", "valor": Decimal("99.90")},
    {"id_pedido": 124, "cliente": "Bruno", "valor": Decimal("150.00")},
    {"id_pedido": 125, "cliente": "Carla", "valor": Decimal("-10.00")},
    {"id_pedido": 126, "cliente": "Diego", "valor": Decimal("75.50")},
)


def publish(channel, pedido: dict) -> None:
    payload = {
        "id_pedido": pedido["id_pedido"],
        "cliente": pedido["cliente"],
        "valor": float(pedido["valor"]),
    }
    body = json.dumps(payload, ensure_ascii=False)

    properties = pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2,
        message_id=str(uuid.uuid4()),
    )

    channel.basic_publish(
        exchange=MAIN_EXCHANGE,
        routing_key=MAIN_ROUTING_KEY,
        body=body,
        properties=properties,
    )
    logger.info(
        "Mensagem publicada: id_pedido=%s cliente=%s valor=%s",
        payload["id_pedido"],
        payload["cliente"],
        pedido["valor"],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publica notificações de pedidos no RabbitMQ."
    )
    parser.add_argument("--id-pedido", type=int, help="ID do pedido")
    parser.add_argument("--cliente", help="Nome do cliente")
    parser.add_argument("--valor", type=Decimal, help="Valor do pedido")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Publica quatro mensagens de exemplo (sucessos e falhas).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.demo:
        pedidos = list(DEMO_MESSAGES)
    else:
        if args.id_pedido is None or args.cliente is None or args.valor is None:
            raise SystemExit(
                "Informe --id-pedido, --cliente e --valor, ou use --demo."
            )
        try:
            pedidos = [
                {
                    "id_pedido": args.id_pedido,
                    "cliente": args.cliente,
                    "valor": Decimal(args.valor),
                }
            ]
        except InvalidOperation as exc:
            raise SystemExit(f"Valor inválido: {exc}") from exc

    connection = create_connection()
    try:
        channel = connection.channel()
        declare_topology(channel)

        for pedido in pedidos:
            publish(channel, pedido)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
