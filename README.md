# OpsSim

A containerized event-driven microservice pipeline built to simulate and observe real-world operational workflows. Designed as a portfolio project demonstrating support engineering skills: fault tolerance, distributed tracing, retry logic, dead letter queues, infrastructure health checks, and CI pipeline design.

---

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │              Docker Network              │
                        │                                          │
  HTTP Request          │   ┌────────┐                            │
──────────────────────► │   │ nginx  │  reverse proxy             │
  localhost:8080        │   └───┬────┘                            │
                        │       │                                  │
                        │   ┌───▼──────┐   publishes to RMQ       │
                        │   │service-a │──────────────────────►   │
                        │   │ FastAPI  │   workflow_queue_b        │
                        │   └───┬──────┘                          │
                        │       │ reads/writes                     │
                        │   ┌───▼──────────────────────────────┐  │
                        │   │            Redis                  │  │
                        │   │  workflow state + readiness keys  │  │
                        │   └───────────────────────────────────┘  │
                        │                                          │
                        │   ┌──────────┐   publishes to RMQ       │
                        │   │service-b │──────────────────────►   │
                        │   │ consumer │   workflow_queue_c        │
                        │   └──────────┘                          │
                        │                                          │
                        │   ┌──────────┐   calls external API     │
                        │   │service-c │──────────────────────►   │
                        │   │ consumer │   external-api:8003       │
                        │   └──────────┘                          │
                        │                                          │
                        │   ┌──────────┐  ┌──────────┐           │
                        │   │RabbitMQ  │  │ Postgres │           │
                        │   │ :5672    │  │  :5432   │           │
                        │   └──────────┘  └──────────┘           │
                        └─────────────────────────────────────────┘
```

### Workflow state machine

```
/work called
    └─► PROCESSING_A
            └─► PUBLISHED_TO_B
                    └─► PROCESSING_B
                            ├─► FAILED_B ──► RETRY_B (up to 3x) ──► DLQ_B
                            └─► COMPLETED_B
                                    └─► PROCESSING_C
                                            ├─► FAILED_C ──► RETRY_C (up to 3x) ──► DLQ_C
                                            ├─► ERRORED_C
                                            └─► COMPLETED_C
```

---

## Services

| Service | Role | Port |
|---|---|---|
| nginx | Reverse proxy — single entry point for all HTTP traffic | 8080 |
| service-a | FastAPI — workflow entry point, status API, system health aggregator | 8001 |
| service-b | RabbitMQ consumer — processes workflow, publishes to service-c | — |
| service-c | RabbitMQ consumer — calls external API, writes final state | — |
| external-api | Simulated third-party API called by service-c | 8003 |
| RabbitMQ | Message broker between services | 5672 / 15672 |
| Redis | Workflow state store + service readiness keys | 6379 |
| Postgres | Persistent audit log of all workflow events | 5432 |

---

## Key Engineering Features

**Fault tolerance**
- Service-b and service-c implement retry logic with a configurable `MAX_RETRIES` limit
- Messages exceeding retry limits are routed to a Dead Letter Queue (`workflow_dlq`)
- Failure simulation built into service-b and service-c to exercise retry and DLQ paths

**Distributed tracing**
- Every workflow carries a `trace_id` and `request_id` through all services
- Structured JSON logs emitted at every state transition across all services
- Full workflow history queryable via `/workflow/{request_id}` from Postgres

**Readiness and health**
- Service-b and service-c publish their dependency readiness (`redis`, `postgres`, `rabbitmq`) to Redis every 10 seconds via a background heartbeat
- Service-a aggregates system-wide readiness via `/system/status`
- Docker Compose healthchecks gate service startup order: RabbitMQ (`check_port_connectivity`), Postgres (`pg_isready`), service-a (`curl /health`)

---

## API Endpoints

All endpoints are accessible via nginx on port 8080.

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Service-a liveness check |
| GET | `/ready` | Service-a dependency readiness |
| GET | `/system/status` | Aggregated readiness of all services |
| GET | `/work` | Trigger a new workflow |
| GET | `/status/{request_id}` | Live workflow state from Redis |
| GET | `/workflow/{request_id}` | Full workflow history from Postgres |
| GET | `/result/{request_id}` | Final result and completion status |

---

## CI Pipeline

The GitHub Actions CI pipeline validates the full stack end-to-end on every push and pull request:

1. Build all service images
2. Start the full stack with `docker compose up`
3. Wait for RabbitMQ to pass `check_port_connectivity`
4. Wait for service-a to pass its healthcheck via nginx
5. Poll `/system/status` until all three services report `ready: true`
6. Trigger a workflow via `/work`
7. Poll `/status/{request_id}` until a terminal state is reached
8. Validate the terminal state is a known completion or failure state
9. Print logs and tear down

---

## Running Locally

**Prerequisites:** Docker and Docker Compose

```bash
git clone https://github.com/ramya-rmkrsh/OpsSim.git
cd OpsSim
docker compose up --build
```

Trigger a workflow:
```bash
curl http://localhost:8080/work
```

Check workflow status:
```bash
curl http://localhost:8080/status/<request_id>
```

Check full system readiness:
```bash
curl http://localhost:8080/system/status
```

RabbitMQ management UI: http://localhost:15672 (guest/guest)

---

## Project Structure

```
OpsSim/
├── service-a/          # FastAPI API layer
├── service-b/          # RabbitMQ consumer — workflow processing
├── service-c/          # RabbitMQ consumer — external API integration
├── external-api/       # Simulated third-party API
├── nginx/              # Reverse proxy config
├── postgres/           # DB init SQL
├── docker-compose.yml
└── .github/workflows/  # CI pipeline
```

---

## Roadmap

- [ ] Observability layer — Grafana dashboards from Postgres `workflow_events`
- [ ] Prometheus metrics counters on service-b and service-c
- [ ] Fault injection script to simulate and stream failures to Grafana
- [ ] CD pipeline to deploy to a cloud environment on merge to main
