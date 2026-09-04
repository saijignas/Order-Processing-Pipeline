"""Consumer logic tests. process_order() is tested directly (it's the
part with actual business logic); the real-queue round-trip test at the
bottom proves the RabbitMQ wiring itself, not just the Python logic."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
os.environ.setdefault("DATABASE_URL", "postgresql://orderuser:orderpass@localhost:5432/orders_test")

from consumer.worker import on_message, process_order
from shared.rabbitmq import MAX_RETRIES, QUEUE_DLQ, QUEUE_MAIN, QUEUE_RETRY, publish_order_created


def test_successful_order_is_marked_completed(make_order, db_session):
    order = make_order(simulate_failures=0)
    result = process_order(order.id, attempt_retry_count=0)
    assert result == "completed"
    db_session.refresh(order)
    assert order.status == "completed"


def test_redelivery_of_an_already_completed_order_is_a_no_op(make_order, db_session):
    # Simulates the exact failure mode idempotency protects against: the
    # consumer processed this order and committed "completed" to the DB,
    # then crashed before acking -- RabbitMQ redelivers the same message.
    order = make_order(simulate_failures=0, status="completed")
    result = process_order(order.id, attempt_retry_count=0)
    assert result == "already_done"
    db_session.refresh(order)
    assert order.status == "completed"  # unchanged, no reprocessing


def test_order_that_fails_then_succeeds_within_max_retries(make_order, db_session):
    order = make_order(simulate_failures=2)
    assert process_order(order.id, attempt_retry_count=0) == "failed_will_retry"
    assert process_order(order.id, attempt_retry_count=1) == "failed_will_retry"
    assert process_order(order.id, attempt_retry_count=2) == "completed"
    db_session.refresh(order)
    assert order.status == "completed"


def test_order_that_always_fails_ends_up_at_dlq_after_max_retries(make_order, db_session):
    order = make_order(simulate_failures=999)  # never succeeds
    for attempt in range(MAX_RETRIES):
        assert process_order(order.id, attempt_retry_count=attempt) == "failed_will_retry"
    assert process_order(order.id, attempt_retry_count=MAX_RETRIES) == "failed_dlq"
    db_session.refresh(order)
    assert order.status == "failed"


def test_unknown_order_id_is_handled_without_raising(db_session):
    import uuid
    result = process_order(uuid.uuid4(), attempt_retry_count=0)
    assert result == "unknown_order"


def test_concurrent_redelivery_of_the_same_order_completes_exactly_once(make_order, db_session):
    """Simulates two consumer instances (or two redelivered copies of the
    same message, arriving before either has committed) calling
    process_order() for the SAME order_id at the same time. Each call
    opens its own DB session/connection -- exactly how two real consumer
    processes would -- so this exercises the actual row lock in Postgres,
    not just in-process logic.

    Without the SELECT ... FOR UPDATE fix, both threads can read
    status != "completed" before either writes, and both proceed to run
    the "real processing" step -- this test would then intermittently
    (racily) observe two "completed" results instead of one "completed"
    and one "already_done". With the fix, the second thread blocks on the
    row lock and, once unblocked, correctly sees the first thread's
    committed "completed" status.
    """
    import threading

    order = make_order(simulate_failures=0)
    results = [None, None]

    def run(slot):
        results[slot] = process_order(order.id, attempt_retry_count=0)

    t1 = threading.Thread(target=run, args=(0,))
    t2 = threading.Thread(target=run, args=(1,))
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert sorted(results) == ["already_done", "completed"], (
        f"expected exactly one 'completed' and one 'already_done', got {results} -- "
        "if this shows two 'completed's, the order was double-processed"
    )
    db_session.refresh(order)
    assert order.status == "completed"


class _FakeMethod:
    def __init__(self, tag=1):
        self.delivery_tag = tag


class _FakeProperties:
    def __init__(self, headers=None):
        self.headers = headers or {}


class _RecordingChannel:
    """Records basic_ack / basic_publish calls instead of hitting a real
    broker -- used only for the on_message routing-decision tests below,
    where the point is "did it ack/route correctly", not the broker
    itself (that's covered by the real round-trip test)."""
    def __init__(self):
        self.acked = []
        self.published = []

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.published.append((routing_key, body, properties))


def test_on_message_acks_and_does_not_republish_when_order_completes(make_order):
    order = make_order(simulate_failures=0)
    channel = _RecordingChannel()
    on_message(channel, _FakeMethod(), _FakeProperties({"x-retry-count": 0}), str(order.id).encode())
    assert channel.acked == [1]
    assert channel.published == []


def test_on_message_publishes_to_retry_queue_and_acks_original_on_failure(make_order):
    order = make_order(simulate_failures=5)
    channel = _RecordingChannel()
    on_message(channel, _FakeMethod(), _FakeProperties({"x-retry-count": 0}), str(order.id).encode())
    assert channel.acked == [1]
    assert len(channel.published) == 1
    routing_key, body, properties = channel.published[0]
    assert routing_key == QUEUE_RETRY
    assert properties.headers["x-retry-count"] == 1


def test_on_message_publishes_to_dlq_after_max_retries(make_order):
    order = make_order(simulate_failures=999)
    channel = _RecordingChannel()
    on_message(channel, _FakeMethod(), _FakeProperties({"x-retry-count": MAX_RETRIES}), str(order.id).encode())
    routing_key, body, properties = channel.published[0]
    assert routing_key == QUEUE_DLQ


def test_full_round_trip_through_the_real_queue(make_order, rabbit_channel):
    """Publishes for real (not a fake channel), pulls the message back
    off the real orders.process queue, and runs it through on_message --
    proving the topology declaration, routing key, and header
    serialization all actually work, not just process_order()'s logic."""
    order = make_order(simulate_failures=0)
    publish_order_created(rabbit_channel, order.id)

    method, properties, body = rabbit_channel.basic_get(QUEUE_MAIN, auto_ack=False)
    assert method is not None
    on_message(rabbit_channel, method, properties, body)
