import os

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "app")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "app")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")

MAIN_EXCHANGE = "notificacoes.exchange"
MAIN_QUEUE = "notificacoes"
MAIN_ROUTING_KEY = "notificacoes"

DLX_EXCHANGE = "notificacoes.dlx"
DLQ_QUEUE = "notificacoes.dlq"
DLQ_ROUTING_KEY = "notificacoes.dlq"

MAX_DELIVERIES = int(os.getenv("MAX_DELIVERIES", "3"))
RETRY_DELAY_SECONDS = float(os.getenv("RETRY_DELAY_SECONDS", "1"))


def create_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(parameters)
