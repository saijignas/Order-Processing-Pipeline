"""Producer API: accepts an order, writes it to Postgres, publishes an
`order.created` event, and returns immediately -- the caller doesn't
wait for the order to actually be processed, the consumer does that
asynchronously.

Idempotency: a client retrying a POST (e.g. after a timeout where it
never saw the response) sends the same Idempotency-Key header. A repeat
key returns the existing order instead of creating a second one --
the same pattern already proven in Rate-Limited-API-Gateway, applied
here at the write path instead of a cache read path.
"""
import sys
import uuid
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.models import Order, get_session_factory
from shared.rabbitmq import get_connection, publish_order_created

app = FastAPI(title="Order Processing Pipeline — Producer")
SessionLocal = get_session_factory()


class OrderItem(BaseModel):
    sku: str
    quantity: int = Field(gt=0)


class CreateOrderRequest(BaseModel):
    customer_id: str
    items: List[OrderItem]
    simulate_failures: int = Field(0, ge=0, le=10, description="Testing-only: force this many consumer failures before success.")


class OrderResponse(BaseModel):
    id: uuid.UUID
    customer_id: str
    items: list
    status: str
    retry_count: int


@app.post("/orders", response_model=OrderResponse, status_code=201)
def create_order(payload: CreateOrderRequest, idempotency_key: Optional[str] = Header(None)):
    session = SessionLocal()
    try:
        if idempotency_key:
            existing = session.query(Order).filter_by(idempotency_key=idempotency_key).first()
            if existing:
                return OrderResponse(
                    id=existing.id, customer_id=existing.customer_id,
                    items=existing.items, status=existing.status, retry_count=existing.retry_count,
                )

        order = Order(
            idempotency_key=idempotency_key,
            customer_id=payload.customer_id,
            items=[item.model_dump() for item in payload.items],
            status="pending",
            simulate_failures=payload.simulate_failures,
        )
        session.add(order)
        session.commit()
        session.refresh(order)

        connection = get_connection()
        try:
            channel = connection.channel()
            publish_order_created(channel, order.id)
        finally:
            connection.close()

        return OrderResponse(
            id=order.id, customer_id=order.customer_id,
            items=order.items, status=order.status, retry_count=order.retry_count,
        )
    finally:
        session.close()


@app.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: uuid.UUID):
    session = SessionLocal()
    try:
        order = session.query(Order).filter_by(id=order_id).first()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        return OrderResponse(
            id=order.id, customer_id=order.customer_id,
            items=order.items, status=order.status, retry_count=order.retry_count,
        )
    finally:
        session.close()


@app.get("/health")
def health():
    return {"status": "ok"}
