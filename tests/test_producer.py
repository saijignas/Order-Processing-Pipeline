"""Producer API tests, against a real Postgres and a real RabbitMQ --
not mocks. Sets DATABASE_URL to the test database *before* importing
producer.app, since it builds its SessionLocal at import time."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
os.environ.setdefault("DATABASE_URL", "postgresql://orderuser:orderpass@localhost:5432/orders_test")

import pytest
from fastapi.testclient import TestClient

from producer.app import app
from shared.rabbitmq import QUEUE_MAIN

client = TestClient(app)


@pytest.fixture(autouse=True)
def _clean_db(db_session):
    # db_session fixture (from conftest) already creates the tables and
    # wipes them after each test; just depending on it here is enough to
    # make sure the schema exists before the API's own session touches it.
    yield


def test_create_order_returns_201_with_pending_status():
    resp = client.post("/orders", json={
        "customer_id": "cust-1",
        "items": [{"sku": "WIDGET-1", "quantity": 2}],
    })
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "pending"
    assert body["customer_id"] == "cust-1"


def test_get_order_returns_created_order():
    created = client.post("/orders", json={
        "customer_id": "cust-2",
        "items": [{"sku": "WIDGET-2", "quantity": 1}],
    }).json()
    resp = client.get(f"/orders/{created['id']}")
    assert resp.status_code == 200
    assert resp.json()["id"] == created["id"]


def test_get_unknown_order_returns_404():
    resp = client.get("/orders/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


def test_repeated_idempotency_key_returns_the_same_order_not_a_duplicate():
    key = "idem-key-abc"
    first = client.post(
        "/orders",
        json={"customer_id": "cust-3", "items": [{"sku": "WIDGET-3", "quantity": 1}]},
        headers={"Idempotency-Key": key},
    ).json()
    second = client.post(
        "/orders",
        json={"customer_id": "cust-3", "items": [{"sku": "WIDGET-3", "quantity": 1}]},
        headers={"Idempotency-Key": key},
    ).json()
    assert first["id"] == second["id"]


def test_different_idempotency_keys_create_different_orders():
    first = client.post(
        "/orders",
        json={"customer_id": "cust-4", "items": [{"sku": "WIDGET-4", "quantity": 1}]},
        headers={"Idempotency-Key": "key-1"},
    ).json()
    second = client.post(
        "/orders",
        json={"customer_id": "cust-4", "items": [{"sku": "WIDGET-4", "quantity": 1}]},
        headers={"Idempotency-Key": "key-2"},
    ).json()
    assert first["id"] != second["id"]


def test_creating_an_order_publishes_a_message_to_the_main_queue(rabbit_channel):
    resp = client.post("/orders", json={
        "customer_id": "cust-5",
        "items": [{"sku": "WIDGET-5", "quantity": 1}],
    })
    order_id = resp.json()["id"]

    method, properties, body = rabbit_channel.basic_get(QUEUE_MAIN, auto_ack=True)
    assert method is not None, "expected a message on the main queue, found none"
    assert body.decode() == order_id
    assert properties.headers["x-retry-count"] == 0
