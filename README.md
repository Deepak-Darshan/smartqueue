# SmartQueue

![CI](https://github.com/Deepak-Darshan/smartqueue/actions/workflows/ci.yml/badge.svg)

A production-grade distributed task queue built on AWS with an AI-powered ops assistant.

---

## Architecture

```
                        ┌─────────────────────────────────────┐
                        │           Producer API               │
         REST           │         (FastAPI / port 8000)        │
  Client ──────────────►│  POST /tasks   GET /queues/depth     │
                        │  GET  /tasks/{id}  POST /assistant/ask│
                        └──────────┬──────────────┬────────────┘
                                   │ enqueue       │ read state
                    ┌──────────────┼──────────────┐│
                    ▼              ▼              ▼ │
             ┌─────────┐   ┌──────────┐   ┌─────────┐
             │SQS High │   │SQS Normal│   │SQS Low  │
             └────┬────┘   └────┬─────┘   └────┬────┘
                  │             │               │
                  └─────────────┼───────────────┘
                                │ poll (high → normal → low)
                        ┌───────▼────────┐
                        │    Workers     │◄──────────────────┐
                        │  (N instances) │                   │
                        └───────┬────────┘           ┌───────┴──────┐
                                │ write status        │  Auto-Scaler │
                        ┌───────▼────────┐            │(circuit breaker│
                        │   DynamoDB     │            │ 1→2→4→8 workers)
                        │smartqueue-tasks│            └───────┬──────┘
                        └───────┬────────┘                   │
                                │                            │ monitor depth
                        ┌───────▼────────┐           ┌───────┴──────┐
                        │  AI Assistant  │◄──────────►│  SQS queues  │
                        │  (Claude API)  │            └──────────────┘
                        └────────────────┘
```

---

## Tech Stack

| Component        | Technology                     | Purpose                                          |
|------------------|--------------------------------|--------------------------------------------------|
| API              | FastAPI + Uvicorn              | REST endpoints, task ingestion, status queries   |
| Queue            | AWS SQS (3 priority + DLQ)     | Durable, decoupled message delivery              |
| Workers          | Python threads + boto3         | Priority polling, retries, DLQ routing           |
| Database         | AWS DynamoDB (on-demand)       | Task state tracking with TTL auto-expiry         |
| Auto-scaling     | Custom scaler + circuit breaker| Adjusts worker count to queue depth              |
| AI Assistant     | Anthropic Claude API           | Natural-language ops queries over live data      |
| CI/CD            | GitHub Actions                 | Lint → test → Docker build on every push         |
| Containerisation | Docker + docker-compose        | Single-command local deployment                  |

---

## Quick Start

```bash
git clone https://github.com/yourusername/smartqueue.git
cd smartqueue

python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Configure AWS credentials and queue URLs
cp .env.example .env   # edit with your values

# Provision DynamoDB table
python scripts/create_table.py

# Start all services (producer + worker + scaler)
python scripts/run_all.py
```

The producer API is now live at `http://localhost:8000`.

---

## API Reference

| Method | Endpoint              | Description                                | Example Response                                                    |
|--------|-----------------------|--------------------------------------------|---------------------------------------------------------------------|
| POST   | `/tasks`              | Enqueue a task                             | `{"task_id": "abc-123", "status": "queued"}`                        |
| GET    | `/tasks/{task_id}`    | Fetch task status and result               | `{"task_id": "...", "status": "completed"}`                         |
| GET    | `/tasks`              | List tasks, optional `?status=` filter     | `{"tasks": [...]}`                                                  |
| GET    | `/queues/depth`       | Approximate message count per queue        | `{"queue_depths": {"high": 0, "normal": 4, "low": 1, "dlq": 0}}`   |
| GET    | `/scaler/status`      | Current worker count and circuit state     | `{"current_workers": 2, "circuit_breaker_status": "closed"}`        |
| POST   | `/assistant/ask`      | Ask the AI ops assistant a question        | `{"answer": "Your high-priority queue has 12 messages..."}`         |
| GET    | `/assistant/health`   | Verify Claude integration is configured    | `{"status": "ok", "model": "claude-sonnet-4-20250514"}`             |
| GET    | `/health`             | Service liveness check                     | `{"status": "ok", "service": "producer"}`                           |

**Enqueue a task:**
```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "send_email", "payload": {"to": "user@example.com"}, "priority": "high"}'
```

**Ask the AI assistant:**
```bash
curl -X POST http://localhost:8000/assistant/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "Why is my queue backing up and what should I do?"}'
```

---

## Key Engineering Decisions

**SQS over Kafka or RabbitMQ.** SQS requires zero infrastructure to operate — no brokers to provision, patch, or scale. For an AWS-native system where reliability and operational simplicity matter more than sub-millisecond latency, SQS's managed durability, dead-letter queue support, and per-message delay semantics are a better fit than running a self-managed cluster.

**DynamoDB over RDS/PostgreSQL.** Task state is written once and read rarely; there are no joins. DynamoDB's single-digit millisecond writes, pay-per-request billing, and native TTL-based expiry are a precise match for this access pattern. A relational database would add connection pooling, schema migrations, and operational overhead without adding value.

**Three priority lanes.** A single queue forces every task to wait behind every other task. Three separate SQS queues let the worker always drain urgent work first without complex in-queue priority logic. The poll loop checks high → normal → low on every cycle, giving O(1) priority enforcement with no extra bookkeeping.

**Circuit breaker in the auto-scaler.** Without it, a bug that crashes every worker would trigger an infinite scale-up loop — consuming resources while masking the root cause. The circuit breaker opens after three failures within 60 seconds, halts all scaling decisions for a 120-second cooldown, and makes failure modes observable and bounded rather than silent and compounding.

**Claude for ops over rule-based alerting.** Rules tell you *what* happened; Claude tells you *why* and *what to do*. By giving the assistant live tool access to queue depths and task records, an operator can ask "why is the DLQ spiking?" and get a diagnosis grounded in real data — not a static threshold alert that requires manual cross-referencing to interpret.

---

## Load Test Results

```bash
python scripts/load_test.py --url http://localhost:8000 --tasks 100 --threads 20
```

| Metric       | Result        |
|--------------|---------------|
| Total tasks  | 100           |
| Successful   | 100           |
| Failed       | 0             |
| Success rate | 100.0%        |
| Total time   | 10.43s        |
| Throughput   | 9.6 tasks/sec |

---

## What I Built

- A priority-aware task queue that scales workers from 1 to 8 in response to live SQS depth, with a circuit breaker that halts runaway scaling during failure cascades.
- Full task lifecycle tracking in DynamoDB — every state transition (queued → processing → completed / failed / dead-lettered) is recorded, queryable by status, and automatically expired after 7 days via TTL.
- An AI ops assistant that grounds Claude's answers in real-time queue and task data through tool calls, replacing brittle threshold alerts with natural-language diagnosis.

---

## Running Tests

```bash
make test
```

```
tests/test_producer.py::test_health_check PASSED
tests/test_producer.py::test_enqueue_valid_task PASSED
tests/test_producer.py::test_enqueue_invalid_priority PASSED
tests/test_producer.py::test_enqueue_missing_fields PASSED
tests/test_producer.py::test_queue_depth_endpoint PASSED

5 passed in 0.35s
```

All tests mock AWS — no credentials required to run locally.
