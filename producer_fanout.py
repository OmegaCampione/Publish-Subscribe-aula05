import argparse
import json
import logging
import time
import uuid
from decimal import Decimal

import pika

from config import FANOUT_EXCHANGE, create_connection
from topology import declare_fanout_exchange

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("producer-fanout")

DEMO_MESSAGES = (
    {"id_pedido": 201, "cliente": "Ana", "valor": Decimal("99.90")},
    {"id_pedido": 202, "cliente": "Bruno", "valor": Decimal("149.90")},
    {"id_pedido": 203, "cliente": "Carla", "valor": Decimal("210.00")},
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

    # Em um exchange fanout a routing key é ignorada. Cada fila vinculada
    # recebe uma cópia independente do evento.
    channel.basic_publish(
        exchange=FANOUT_EXCHANGE,
        routing_key="",
        body=body,
        properties=properties,
    )
    logger.info(
        "Evento publicado em '%s': id_pedido=%s cliente=%s valor=%s",
        FANOUT_EXCHANGE,
        payload["id_pedido"],
        payload["cliente"],
        pedido["valor"],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publica eventos de pedidos em um exchange fanout."
    )
    parser.add_argument("--id-pedido", type=int, help="ID do pedido")
    parser.add_argument("--cliente", help="Nome do cliente")
    parser.add_argument("--valor", type=Decimal, help="Valor do pedido")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Publica três eventos de exemplo.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Intervalo em segundos entre eventos do modo --demo.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.interval < 0:
        raise SystemExit("--interval não pode ser negativo.")

    if args.demo:
        pedidos = list(DEMO_MESSAGES)
    else:
        if args.id_pedido is None or args.cliente is None or args.valor is None:
            raise SystemExit(
                "Informe --id-pedido, --cliente e --valor, ou use --demo."
            )
        pedidos = [
            {
                "id_pedido": args.id_pedido,
                "cliente": args.cliente,
                "valor": args.valor,
            }
        ]

    connection = create_connection()
    try:
        channel = connection.channel()
        declare_fanout_exchange(channel)

        for index, pedido in enumerate(pedidos):
            publish(channel, pedido)
            if index < len(pedidos) - 1 and args.interval > 0:
                time.sleep(args.interval)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
