"""Consumer: processes `order.created` events off the main queue.

Idempotent by design: before doing any work, it checks the order's
current status in Postgres. If it's already `completed`, the handler
returns immediately with no side effects. This matters because message
delivery is at-least-once, not exactly-once -- if the consumer commits
the DB update but crashes before acking the message back to RabbitMQ,
the same message gets redelivered. Without the status check, that
redelivery would "process" (and, in a real system, double-charge or
double-ship) an already-completed order.

Retry/DLQ: a simulated processing failure doesn't requeue the message
directly (which would spin-retry with no delay) -- it publishes to the
`orders.retry` queue, which holds the message for RETRY_TTL_MS before
RabbitMQ automatically dead-letters it back into the main queue. After
MAX_RETRIES, it goes to the DLQ instead and the order is marked failed.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.models import Order, get_session_factory
from shared.rabbitmq import (
    MAX_RETRIES,
    QUEUE_MAIN,
    declare_topology,
    get_connection,
    publish_to_dlq,
    publish_to_retry,
)

SessionLocal = get_session_factory()


class SimulatedProcessingError(Exception):
    pass


def process_order(order_id, attempt_retry_count):
    """Returns one of: 'completed', 'already_done', 'failed_will_retry',
    'failed_dlq'. Raises nothing -- failure is a return value, not an
    exception, so the caller doesn't need a try/except around DB state
    changes it already committed."""
    session = SessionLocal()
    try:
        order = session.query(Order).filter_by(id=order_id).first()
        if order is None:
            return "unknown_order"

        if order.status == "completed":
            return "already_done"

        order.status = "processing"
        order.retry_count = attempt_retry_count
        session.commit()

        if order.simulate_failures > attempt_retry_count:
            if attempt_retry_count >= MAX_RETRIES:
                order.status = "failed"
                session.commit()
                return "failed_dlq"
            return "failed_will_retry"

        # "Real" processing: inventory check + payment simulation. Both
        # deterministic stand-ins -- the point of this project is the
        # pipeline's delivery/retry/idempotency guarantees, not a real
        # payment integration.
        time.sleep(0.05)
        order.status = "completed"
        session.commit()
        return "completed"
    finally:
        session.close()


def on_message(channel, method, properties, body):
    order_id = body.decode()
    retry_count = (properties.headers or {}).get("x-retry-count", 0)

    result = process_order(order_id, retry_count)

    if result in ("completed", "already_done", "unknown_order"):
        channel.basic_ack(delivery_tag=method.delivery_tag)
    elif result == "failed_will_retry":
        publish_to_retry(channel, order_id, retry_count + 1)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    elif result == "failed_dlq":
        publish_to_dlq(channel, order_id, retry_count)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    print(f"[worker] order={order_id} retry_count={retry_count} -> {result}", flush=True)


def run():
    connection = get_connection()
    channel = connection.channel()
    declare_topology(channel)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_MAIN, on_message_callback=on_message)
    print("[worker] waiting for orders...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    run()
