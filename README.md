# IRONCLAD — Distributed LLM Orchestration Runtime

Production-grade distributed execution platform for LLM agent workloads.
Redis-backed, event-driven, multi-tenant.

## Architecture

```
hfa-core        event schema/codec, runtime state store, observability primitives
hfa-control     admission, scheduler, registry, shard ownership, leader election,
                recovery, REST API
hfa-worker      shard consumers, execution, idempotency, heartbeat, drain lifecycle
hfa-tools       sandbox, middleware, inspector
```

## Current sprint state

| Sprint | Focus | Status |
|--------|-------|--------|
| 9  | Distributed runtime foundation | ✅ green |
| 10 | Control plane (scheduler, registry, recovery, leader) | ✅ green |
| 11 | Worker execution engine, failure taxonomy, drain, idempotency | ✅ green |
| 12 | Observability, drain metrics, backward compatibility | ✅ green |
| 13 | Control-plane operability, runtime introspection APIs | ✅ green |

## Running tests

```bash
# All core tests (requires fakeredis)
pytest tests/core -q

# Sprint 13 only
pytest tests/core/test_sprint13_* -q

# Lint
ruff check hfa-core/src hfa-control/src hfa-worker/src hfa-tools/src tests/core
```

## Control Plane API — Sprint 13 surface

Base prefix: `/control/v1`

### Security model

| Category | Auth required |
|----------|---------------|
| Liveness / readiness | None (public) |
| Operator endpoints | `X-CP-Auth` header |
| Tenant endpoints | `X-Tenant-ID` header |

### Health

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health/live` | — | Liveness probe |
| GET | `/health/ready` | — | Readiness probe (Redis + streams) |
| GET | `/health` | tenant | Legacy health summary |

### Workers

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/workers/healthy` | operator | All alive workers (healthy + draining) |
| GET | `/workers/schedulable` | operator | Workers eligible for new runs |
| GET | `/workers` | tenant | Worker list (Sprint 10) |
| GET | `/workers/{worker_id}` | tenant | Worker detail |
| POST | `/workers/{worker_id}/drain` | operator | Initiate drain |

**Schedulable definition:** `status=HEALTHY AND NOT draining AND inflight < capacity`

### Runs

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/runs/running` | operator | Runs in the running ZSET |
| GET | `/runs/{run_id}/state` | tenant | Run state + metadata |
| GET | `/runs/{run_id}/claim` | operator | Claim owner + TTL |
| GET | `/runs/{run_id}/result` | tenant | Stored result |
| GET | `/runs/{run_id}/placement` | tenant | Placement details (Sprint 10) |

### Recovery

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/recovery/stale` | operator | Stale run candidates |
| GET | `/recovery/summary` | operator | stale + dlq + worker counts |
| GET | `/recovery/dlq` | operator | DLQ entries (all tenants) |

## Key contracts (never break)

- `CONSUMER_GROUP = "worker_consumers"` — Redis stream consumer group name
- Redis key patterns: `hfa:run:state:{id}`, `hfa:run:meta:{id}`, `hfa:run:result:{id}`, `hfa:cp:running`, `hfa:run:claim:{id}`
- Failure taxonomy: `success → done+ACK`, `terminal → failed+ACK`, `infra → no ACK, claim released`
- Event class names and field names are immutable; schema evolution is additive-only
