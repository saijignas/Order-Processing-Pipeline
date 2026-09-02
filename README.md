# Order Processing Pipeline — Event-Driven, RabbitMQ

![CI](https://github.com/saijignas/Order-Processing-Pipeline/actions/workflows/ci.yml/badge.svg)

A producer and consumer, decoupled by a real message queue, with the two
things that actually matter in an at-least-once delivery system: retries
don't spin forever, and redelivery doesn't double-process. Not a toy
"hello world" queue demo — the idempotency here is the same pattern
already proven in [Rate-Limited-API-Gateway](https://github.com/saijignas/Rate-Limited-API-Gateway),
applied on the write path instead of a cache read path.

**Stack:** Python, FastAPI, RabbitMQ, PostgreSQL, Docker, Kubernetes.

**[Live demo](http://order-pipeline-saijignas.centralindia.cloudapp.azure.com:30080/health)** — running on a single free-tier Azure VM (k3s). Try it:
```bash
curl -X POST http://order-pipeline-saijignas.centralindia.cloudapp.azure.com:30080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "demo", "items": [{"sku": "WIDGET-1", "quantity": 1}]}'
# GET the returned id a moment later -- status will have moved pending -> completed
```

## Architecture

```mermaid
flowchart LR
    Client -->|POST /orders| Producer
    Producer -->|writes order,\nstatus=pending| Postgres[(Postgres)]
    Producer -->|publish order.created| Exchange{{orders exchange}}
    Exchange --> Main[orders.process]
    Main --> Consumer

    Consumer -->|success| Postgres
    Consumer -->|failure, retries left| Retry[orders.retry\nTTL=5s]
    Retry -->|TTL expires, auto dead-letter| Exchange
    Consumer -->|failure, retries exhausted| DLQ[orders.dlq]
```

- **Idempotent producer**: a client retrying a `POST /orders` (e.g. after
  a timeout where it never saw the response) sends the same
  `Idempotency-Key` header. A repeat key returns the *existing* order
  instead of creating a duplicate.
- **Idempotent consumer**: before doing any work, the consumer checks the
  order's current status in Postgres. If it's already `completed`, the
  handler returns immediately with no side effects. This is what
  actually matters under at-least-once delivery: if the consumer commits
  the DB update but crashes before acking the message, RabbitMQ
  redelivers it. Without this check, that redelivery would reprocess
  (in a real system: double-charge, double-ship) an already-completed
  order.
- **Retry with delay, not a spin loop**: a failed order isn't nacked
  back onto the main queue (which would retry with no delay and burn
  CPU). It's published to `orders.retry`, which holds it for 5 seconds
  via a per-queue TTL, then RabbitMQ's own dead-letter mechanism
  automatically routes it back to the main queue. After `MAX_RETRIES`
  (3) failed attempts, it goes to `orders.dlq` instead and the order is
  marked `failed` — a human has to look at it; nothing auto-retries out
  of the DLQ.

## Testing discipline

Every test runs against a real Postgres and a real RabbitMQ (via
GitHub Actions service containers), not mocks — including a full
round-trip test that publishes to the actual queue, pulls the message
back off it, and runs it through the real message handler, which is
what actually proves the exchange/routing-key/header wiring works
rather than just the Python logic in isolation.

| What's tested | How |
|---|---|
| Idempotent order creation | Same `Idempotency-Key` twice → same order ID |
| Idempotent consumer | An order already `completed`, redelivered → no-op, no reprocessing |
| Retry-then-recover | `simulate_failures=2` fails twice, succeeds on the 3rd attempt |
| DLQ after exhausting retries | `simulate_failures` set high enough → ends at `orders.dlq`, order marked `failed` |
| Real queue round-trip | Publish for real, `basic_get` it back, run it through the real handler |

`simulate_failures` is a testing-only field on order creation (disclosed
here, not hidden) that forces the consumer to fail a set number of times
before succeeding — without it, retry/DLQ behavior would only be
observable by waiting for real, unpredictable flakiness.

## Deployment

**Live**, on a single free-tier Azure VM (Azure for Students — AWS/GCP
free tiers require a card, Azure for Students doesn't) running **k3s**
(a lightweight, still CNCF-certified Kubernetes distribution) instead of
a managed control plane like EKS/AKS, whose price (~$73/month for EKS
alone) isn't worth paying for a portfolio project. Postgres and RabbitMQ
run inside the cluster too, not as managed services, so the entire stack
fits on one node.

**The honest constraint:** the free-tier VM size available to this
subscription has 1GB RAM. That's tight enough that it shaped real
decisions, disclosed here rather than smoothed over:
- **1 replica each** for producer/consumer, not the 2 originally
  designed for (visible in git history) — running 2 of each was not
  going to fit.
- **Image built directly on the node** (`docker build`), not pulled
  from a registry via CI/CD — the registry+CD path is a real
  next step, just not one this specific box's RAM was spent on.
- **2GB swap** added on top of the 1GB RAM so k3s's own control-plane
  overhead doesn't OOM-kill the actual application pods.
- Traefik (k3s's bundled ingress controller) and metrics-server were
  removed — unused here (the producer is exposed via a plain NodePort)
  and not worth the RAM on a box this small.

None of this changes the pipeline's actual guarantees (idempotency,
retry, DLQ all work identically regardless of replica count) — it's
exactly the kind of resource-constrained tradeoff a real small-scale
deployment runs into, made explicit instead of hidden.

## Limitations

- "Inventory check" and "payment" in the consumer are deterministic
  stand-ins, not real integrations — the point of this project is the
  delivery/retry/idempotency guarantees around processing, not a
  payment gateway.
- Single RabbitMQ node, no clustering/mirrored queues — a real
  production deployment handling money would want queue mirroring so a
  broker restart doesn't risk in-flight messages; out of scope here.
- The retry queue's TTL (5s) is fixed, not exponential backoff — simpler
  to reason about and to make observable in tests quickly; a real system
  processing high-value orders might want backoff that grows with
  `x-retry-count`.

## How to run locally

```bash
docker compose up --build
# Producer: http://localhost:8000
# RabbitMQ management UI: http://localhost:15672 (guest/guest)

curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-1" \
  -d '{"customer_id": "cust-1", "items": [{"sku": "WIDGET-1", "quantity": 2}]}'
```

## Files

```
producer/app.py       # FastAPI: POST /orders, GET /orders/{id}
consumer/worker.py     # RabbitMQ consumer: processes orders, retry/DLQ routing
shared/
  models.py            # SQLAlchemy Order model (shared by both processes)
  rabbitmq.py           # exchange/queue topology, publish helpers
  init_db.py            # creates tables
tests/
  conftest.py           # real Postgres + RabbitMQ fixtures
  test_producer.py       # API + idempotent creation + real-queue publish
  test_consumer.py       # process_order() logic + real-queue round-trip
k8s/                    # Kubernetes manifests (k3s target, see Deployment above)
docker-compose.yml      # local dev: Postgres + RabbitMQ + both services
```
