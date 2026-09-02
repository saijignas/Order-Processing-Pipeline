import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.models import Base, Order, get_session_factory
from shared.rabbitmq import QUEUE_DLQ, QUEUE_MAIN, QUEUE_RETRY, declare_topology, get_connection

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://orderuser:orderpass@localhost:5432/orders_test"
)


@pytest.fixture(scope="function")
def db_session():
    from sqlalchemy import create_engine
    engine = create_engine(TEST_DATABASE_URL, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    SessionLocal = get_session_factory(engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.query(Order).delete()
        session.commit()
        session.close()


@pytest.fixture(scope="function")
def rabbit_channel():
    connection = get_connection()
    channel = connection.channel()
    declare_topology(channel)
    # Start every test with empty queues -- a leftover message from a
    # previous test run (or a previous failed run) shouldn't make an
    # unrelated test flaky.
    channel.queue_purge(QUEUE_MAIN)
    channel.queue_purge(QUEUE_RETRY)
    channel.queue_purge(QUEUE_DLQ)
    try:
        yield channel
    finally:
        connection.close()


@pytest.fixture()
def make_order(db_session):
    def _make(customer_id="cust-1", items=None, simulate_failures=0, status="pending"):
        order = Order(
            id=uuid.uuid4(),
            customer_id=customer_id,
            items=items or [{"sku": "WIDGET-1", "quantity": 1}],
            simulate_failures=simulate_failures,
            status=status,
        )
        db_session.add(order)
        db_session.commit()
        db_session.refresh(order)
        return order
    return _make
