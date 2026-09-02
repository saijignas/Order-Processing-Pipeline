"""Shared SQLAlchemy model, imported by both the producer and the
consumer -- they're two processes but one schema."""
import os
import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class Order(Base):
    __tablename__ = "orders"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    idempotency_key = Column(String, unique=True, nullable=True, index=True)
    customer_id = Column(String, nullable=False)
    items = Column(JSONB, nullable=False)
    # pending -> processing -> completed | failed
    status = Column(String, nullable=False, default="pending")
    retry_count = Column(Integer, nullable=False, default=0)
    # Testing-only knob, honestly disclosed: forces the consumer to fail
    # this many times before succeeding, so retry/DLQ behavior is
    # observable on demand rather than waiting for real flakiness.
    simulate_failures = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


def get_engine():
    url = os.environ.get("DATABASE_URL", "postgresql://orderuser:orderpass@localhost:5432/orders")
    return create_engine(url, pool_pre_ping=True)


def get_session_factory(engine=None):
    return sessionmaker(bind=engine or get_engine(), autocommit=False, autoflush=False)
