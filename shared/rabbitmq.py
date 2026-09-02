"""RabbitMQ topology: the classic three-queue retry pattern.

- `orders` exchange (direct): where new-order events get published.
- `orders.process`: the main work queue, bound to `orders` on
  routing key `order.created`.
- `orders.retry`: not consumed directly. Messages land here with a
  per-message TTL; once the TTL expires, RabbitMQ's dead-letter
  mechanism automatically re-routes them back to the `orders` exchange
  (and so back into `orders.process`) -- this is what gives retries a
  delay without the consumer sleeping in-process.
- `orders.dlq`: the terminal resting place for a message that's failed
  more than MAX_RETRIES times. Nothing auto-retries out of here; a human
  (or a separate reprocessing tool, not built here) has to look at it.

Every queue is declared durable and every publish is persistent, so a
RabbitMQ restart doesn't silently drop in-flight orders.
"""
import os

import pika

EXCHANGE = "orders"
QUEUE_MAIN = "orders.process"
QUEUE_RETRY = "orders.retry"
QUEUE_DLQ = "orders.dlq"
ROUTING_KEY = "order.created"
RETRY_TTL_MS = 5000
MAX_RETRIES = 3


def get_connection():
    url = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    return pika.BlockingConnection(pika.URLParameters(url))


def declare_topology(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)

    channel.queue_declare(queue=QUEUE_MAIN, durable=True)
    channel.queue_bind(queue=QUEUE_MAIN, exchange=EXCHANGE, routing_key=ROUTING_KEY)

    # Retry queue: TTL + dead-letter back to the main exchange. No
    # consumer ever reads from this queue directly; RabbitMQ's own
    # dead-letter mechanism is what moves messages out of it.
    channel.queue_declare(
        queue=QUEUE_RETRY,
        durable=True,
        arguments={
            "x-message-ttl": RETRY_TTL_MS,
            "x-dead-letter-exchange": EXCHANGE,
            "x-dead-letter-routing-key": ROUTING_KEY,
        },
    )

    channel.queue_declare(queue=QUEUE_DLQ, durable=True)


def publish_order_created(channel, order_id, retry_count=0):
    declare_topology(channel)
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=ROUTING_KEY,
        body=str(order_id).encode(),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent
            headers={"x-retry-count": retry_count},
        ),
    )


def publish_to_retry(channel, order_id, retry_count):
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_RETRY,
        body=str(order_id).encode(),
        properties=pika.BasicProperties(
            delivery_mode=2,
            headers={"x-retry-count": retry_count},
        ),
    )


def publish_to_dlq(channel, order_id, retry_count):
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_DLQ,
        body=str(order_id).encode(),
        properties=pika.BasicProperties(
            delivery_mode=2,
            headers={"x-retry-count": retry_count},
        ),
    )
