from config import (
    DLQ_QUEUE,
    DLQ_ROUTING_KEY,
    DLX_EXCHANGE,
    MAIN_EXCHANGE,
    MAIN_QUEUE,
    MAIN_ROUTING_KEY,
    MAX_DELIVERIES,
)


def declare_topology(channel) -> None:
    """Declara exchanges, filas e bindings de forma idempotente."""

    # O exchange principal recebe os pedidos publicados pelo producer.
    channel.exchange_declare(
        exchange=MAIN_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    # O DLX é o exchange para onde o RabbitMQ encaminha mensagens mortas.
    channel.exchange_declare(
        exchange=DLX_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    # A DLQ precisa existir antes da fila principal para que o encaminhamento
    # de mensagens mortas possa ser configurado com segurança.
    channel.queue_declare(
        queue=DLQ_QUEUE,
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    channel.queue_bind(
        queue=DLQ_QUEUE,
        exchange=DLX_EXCHANGE,
        routing_key=DLQ_ROUTING_KEY,
    )

    # Declaração da fila principal:
    # - x-queue-type=quorum: habilita o suporte a x-delivery-limit.
    # - x-dead-letter-exchange/routing-key: definem o destino das mensagens
    #   rejeitadas ou que atingirem o limite de falhas.
    # - x-delivery-limit: proteção do broker para mensagens devolvidas por
    #   falhas de consumidor (crash/reject). O consumer também controla o
    #   limite e envia à DLQ com NACK(requeue=False) na última tentativa.
    main_queue_arguments = {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": DLX_EXCHANGE,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
        "x-delivery-limit": MAX_DELIVERIES,
    }
    channel.queue_declare(
        queue=MAIN_QUEUE,
        durable=True,
        arguments=main_queue_arguments,
    )
    channel.queue_bind(
        queue=MAIN_QUEUE,
        exchange=MAIN_EXCHANGE,
        routing_key=MAIN_ROUTING_KEY,
    )
