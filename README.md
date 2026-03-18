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
| 14A | Tenant registry, load factor, capability model | ✅ green |
| 14B | Tenant-aware admission enforcement (inflight, rate limit, rejection) | ✅ green |
| 14C | Tenant fairness accounting hook (instrumentation only) | ✅ green |
| 15 | Hardening & Resilience (keys, atomic rate limit, integration tests) | ✅ green |

## Running tests

```bash
# All core tests (fast, uses fakeredis)
pytest tests/core -q

# Sprint 15 regression
pytest tests/core/test_sprint15_keys.py -q
pytest tests/core/test_sprint14c_fairness.py -q
pytest tests/core/test_sprint14c_scheduler_fairness_hook.py -q

# Sprint 14B regression (must stay green after rate limiter refactor)
pytest tests/core/test_sprint14b_rate_limit.py -q
pytest tests/core/test_sprint14b_tenant_admission.py -q
pytest tests/core/test_sprint14b_inflight_lifecycle.py -q

# Full core regression
pytest tests/core -q

# Integration tests (requires Docker + testcontainers)
pytest tests/integration -q -m integration

# Lint
ruff check hfa-core/src hfa-control/src hfa-worker/src hfa-tools/src tests/core
```

## System guarantees and known limits

### Fairness — current state

IRONCLAD implements **tenant fairness instrumentation and accounting**.

- Admission control enforces per-tenant `max_inflight_runs` and `max_runs_per_second`.
- After each successful scheduling dispatch, the tenant's virtual runtime (vruntime)
  is updated via `TenantFairnessTracker`.
- **Scheduler placement remains load-factor and policy driven** (LEAST_LOADED,
  REGION_AFFINITY, ROUND_ROBIN, CAPABILITY_MATCH).
- Full active fair-queue scheduling (vruntime-based dispatch ordering) is
  planned for a future sprint.

Accurate description: "The system has fairness instrumentation and a foundation
for tenant-aware dispatch evolution, but scheduler placement is not yet
fairness-driven."

### Rate limiting — Sprint 15 atomicity

The tenant rate limiter uses a single Redis Lua script (`ATOMIC_RATE_LIMIT_LUA`
in `hfa_control/rate_limit.py`) to atomically trim-check-and-record each
admission in a sliding 1-second window. There is no TOCTOU window under
concurrent admission.

- Unit tests (`tests/core/`) use fakeredis with a non-atomic fallback path.
- Integration tests (`tests/integration/`) use real Redis via Testcontainers
  and exercise the Lua eval path. These are the authoritative atomicity tests.

### Redis as hot-state store

Redis is currently the single system of record for all runtime state:
run lifecycle, tenant inflight, rate limits, worker registry, leader
election, streams, and DLQ.

- Redis HA / Sentinel configuration is the operator's responsibility.
- Persistent run archival to a cold store (e.g. PostgreSQL) is planned
  for a future sprint.
- All Redis keys and TTLs are centralized in `hfa-core/src/hfa/config/keys.py`.

### Key and TTL management — Sprint 15

All Redis key patterns and TTL values are defined in one place:

```python
from hfa.config.keys import RedisKey, RedisTTL, TTL

key = RedisKey.run_state("run-abc-123")  # → "hfa:run:state:run-abc-123"
await redis.set(key, "admitted", ex=TTL.RUN_STATE)  # TTL.RUN_STATE = 86400
```

No raw key strings or magic integers should appear in source files.

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
- Redis key patterns: see `hfa-core/src/hfa/config/keys.py` (canonical source)
- Failure taxonomy: `success → done+ACK`, `terminal → failed+ACK`, `infra → no ACK, claim released`
- Event class names and field names are immutable; schema evolution is additive-only
- Fairness hook: `TenantFairnessTracker.update_on_dispatch()` is called ONLY after confirmed dispatch


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
