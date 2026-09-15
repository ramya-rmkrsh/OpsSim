# OpsSim
Version: 2.0.0

A containerized event-driven microservice pipeline built to simulate and observe real-world operational workflows. Designed as a portfolio project demonstrating support engineering skills: fault tolerance, distributed tracing, retry logic, dead letter queues, infrastructure health checks, full-stack observability, and CI pipeline design.

---

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │              Docker Network             │
                        │                                         │
  HTTP Request          │   ┌────────┐                            │
──────────────────────► │   │ nginx  │  reverse proxy             │
  localhost:8080        │   └───┬────┘                            │
                        │       │                                 │
                        │   ┌───▼──────┐   publishes to RMQ       │
                        │   │service-a │──────────────────────►   │
                        │   │ FastAPI  │   workflow_queue_b       │
                        │   └───┬──────┘                          │
                        │       │ reads/writes                    │
                        │   ┌───▼──────────────────────────────┐  │
                        │   │            Redis                 │  │
                        │   │  workflow state + readiness keys │  │
                        │   └──────────────────────────────────┘  │
                        │                                         │
                        │   ┌──────────┐   publishes to RMQ       │
                        │   │service-b │──────────────────────►   │
                        │   │ consumer │   workflow_queue_c       │
                        │   └──────────┘                          │
                        │                                         │
                        │   ┌──────────┐   calls external API     │
                        │   │service-c │──────────────────────►   │
                        │   │ consumer │   external-api:8003      │
                        │   └──────────┘                          │
                        │                                         │
                        │   ┌──────────┐  ┌──────────┐            │
                        │   │RabbitMQ  │  │ Postgres │            │
                        │   │ :5672    │  │  :5432   │            │
                        │   └──────────┘  └──────────┘            │
                        └─────────────────────────────────────────┘
```

### Observability stack

```
service-a / service-b / service-c
        │  (spans + structured JSON logs)
        ▼
  OTel Collector ──────► Tempo (traces) ──┐
        │                                 │
        └──► Promtail ──► Loki (logs) ────┼──► Grafana
                                          │
  redis-exporter ─────┐                   │
  postgres-exporter ──┼──► Prometheus (metrics)
  rabbitmq (built-in) ┘
```

Every workflow carries a `trace_id` and `request_id` through all three services. Traces (Tempo), logs (Loki), and metrics (Prometheus) are correlated by `trace_id`, and viewable together in Grafana at `localhost:3000`.

---

### Workflow state machine

Each service moves a request through its own states in Redis (live status) and, at select points, Postgres (persistent audit log). Retries share the same state name as the failure they're retrying — the *reason* is captured in the log/DB `message` field, not a separate state.

```
/work called (service-a)
    └─► PROCESSING_A         (redis + log + trace)
            └─► PUBLISHED_TO_B   (log + trace — hands off to service-b)
                    └─► PROCESSING_B (service-b)     (redis + log + trace)
                            │
                            ├─► FAILED_B  (retry, up to 5x — redis + log + trace only)
                            │        └─► retries exhausted
                            │                 └─► FAILED_B  [DB write · terminal]
                            │                       message: "Message sent to DLQ"
                            │
                            └─► COMPLETED_B  [DB write · not terminal — workflow continues]
                                    └─► PROCESSING_C (service-c)   (redis + log + trace)
                                            │
                                            ├─► FAILED_C  (retry, up to 5x — redis + log + trace only)
                                            │        └─► retries exhausted
                                            │                 └─► FAILED_C  [DB write · terminal]
                                            │                       message: "Message sent to DLQ"
                                            │
                                            ├─► FAILED_C  [DB write · terminal]
                                            │     message: "external API failed" or the caught exception
                                            │
                                            └─► COMPLETED_C  [DB write · terminal]
```

**Terminal states:** `FAILED_B`, `FAILED_C`, `COMPLETED_C` — these are the only states the `/result/{request_id}` endpoint recognizes as workflow-complete (see `FINAL_STATES` in service-a). `COMPLETED_B` is **not** terminal — it marks a successful hand-off from service-b to service-c, and the workflow keeps going.

**What gets persisted to Postgres:** `COMPLETED_C` and the three terminal outcomes. Redis holds live/in-flight state (including retry attempts); Postgres records the hand-off to service-c and how each workflow finally ended. 

Failures are captured as state FAILED_B / FAILED_C with a message describing the specific cause (DLQ, external API failure, exception) - the state name tells you which service and the reason why it failed

---

## Services

| Service      | Role                                                                 | Port         |
| ------------ | -------------------------------------------------------------------- | ------------ |
| nginx        | Reverse proxy — single entry point for all HTTP traffic              | 8080         |
| service-a    | FastAPI — workflow entry point, status API, system health aggregator | 8001         |
| service-b    | RabbitMQ consumer — processes workflow, publishes to service-c       | —            |
| service-c    | RabbitMQ consumer — calls external API, writes final state           | —            |
| external-api | Simulated third-party API called by service-c                        | 8003         |
| RabbitMQ     | Message broker between services                                      | 5672 / 15672 |
| Redis        | Workflow state store + service readiness keys                        | 6379         |
| Postgres     | Persistent audit log of terminal workflow outcomes                   | 5432         |
| Grafana      | Unified visualization — logs, traces, metrics                        | 3000         |
| Prometheus   | Metrics scraping and storage                                         | 9090         |
| Loki         | Log aggregation                                                      | 3100         |
| Tempo        | Distributed trace storage                                            | 3200         |

---

## Key Engineering Features

**Fault tolerance**
- service-b and service-c implement retry logic with a configurable `MAX_RETRIES` limit (5 for service-b, 3 for service-c), tracked via per-service Redis keys
- Messages exceeding retry limits are routed to a Dead Letter Queue (`workflow_dlq`) and recorded as a `FAILED_B`/`FAILED_C` terminal state in Postgres, with the reason in the `message` field
- Failure simulation built into service-b and service-c to reliably exercise retry and DLQ paths on normal runs

**Distributed tracing**
- Every workflow carries a `trace_id` and `request_id` through all services, propagated over RabbitMQ message headers
- Explicit spans around every Redis, Postgres, and RabbitMQ operation, plus the external API call
- Terminal workflow outcomes queryable via `/workflow/{request_id}` from Postgres; full in-flight detail (including retries) visible via correlated logs and traces in Grafana

**Structured logging**
- Every log line includes `service`, `component`, `operation`, `state`, `trace_id`, and `request_id`
- Promtail parses these fields into Loki labels, enabling direct log-to-trace correlation in Grafana

**Observability stack**
- OpenTelemetry Collector receives spans from all three services and exports to Tempo
- Prometheus scrapes application metrics plus Redis, RabbitMQ, and Postgres exporters
- Grafana ties logs, traces, and metrics together in one place, queryable by `trace_id`

**Readiness and health**
- service-b and service-c publish their dependency readiness (`redis`, `postgres`, `rabbitmq`, and for service-c, `external_api`) to Redis every 10 seconds via a background heartbeat
- service-a aggregates system-wide readiness via `/system/status`
- Docker Compose healthchecks gate service startup order: RabbitMQ (`check_port_connectivity`), Postgres (`pg_isready`), service-a (`curl /health`)

---

## API Endpoints

All endpoints are accessible via nginx on port 8080.

| Method | Endpoint                 | Description                                          |
| ------ | ------------------------ | ----------------------------------------------------- |
| GET    | `/health`                | service-a liveness check                             |
| GET    | `/ready`                 | service-a dependency readiness                        |
| GET    | `/system/status`         | Aggregated readiness of all services                  |
| GET    | `/work`                  | Trigger a new workflow                                 |
| GET    | `/status/{request_id}`   | Live workflow state from Redis                         |
| GET    | `/workflow/{request_id}` | Terminal workflow outcome history from Postgres        |
| GET    | `/result/{request_id}`   | Current state + whether it's one of the terminal states (`FAILED_B`, `FAILED_C`, `COMPLETED_C`) |

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

Check terminal outcome history:
```bash
curl http://localhost:8080/workflow/<request_id>
```

Check full system readiness:
```bash
curl http://localhost:8080/system/status
```

**Observability UIs**
- Grafana: <http://localhost:3000> (admin/admin) — logs, traces, and metrics dashboards
- Prometheus: <http://localhost:9090> — raw metrics and target health
- RabbitMQ management: <http://localhost:15672> (guest/guest)

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
├── prometheus/         # Prometheus scrape config
├── loki/                # Loki config
├── promtail/            # Log parsing + shipping config
├── tempo/                # Tempo config
├── otel/                 # OTel Collector config
├── docker-compose.yml
└── .github/workflows/  # CI pipeline
```

---

## Roadmap

- [x] Observability layer — Grafana, Prometheus, Loki, Tempo wired end-to-end
- [x] Metrics for Redis, RabbitMQ, and Postgres via exporters
- [x] Distributed tracing with correct span coverage across all services
- [x] Terminal state persistence for DLQ'd and failed workflows
- [ ] Prometheus metrics counters directly on service-b and service-c (currently inferred via logs/traces only)
- [ ] Fault injection script to simulate and stream failures to Grafana
- [ ] Datadog migration — stream logs/traces/metrics to Datadog as a comparison to the OSS stack
- [x] CD pipeline — deploy to a cloud environment on merge to main (separate from the CI validation pipeline above, which is already in place)
- [ ] CD pipeline to deploy to a cloud environment on merge to main
