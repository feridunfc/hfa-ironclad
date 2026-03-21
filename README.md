# HFA / IRONCLAD

**Distributed LLM Orchestration Runtime** — Sprint 20

A production-grade distributed orchestration platform for LLM workloads, built on Redis Streams with multi-tenant fair scheduling and atomic dispatch guarantees.

## Architecture

```
hfa-core/        # Event schema, codec, Redis key management, Lua scripts, governance
hfa-control/     # Control plane: admission, scheduler, registry, leader election, API
hfa-worker/      # Worker plane: consumer, executor (OpenAI), heartbeat, drain lifecycle
hfa-tools/       # Middleware, sandbox, tooling helpers
```

## Status: Sprint 9 → 20 (Cumulative)

| Sprint | Feature |
|--------|---------|
| 9  | Distributed runtime foundation, event schema, shard/state primitives |
| 10 | Control plane orchestration, leader election, scheduler (4 policies) |
| 11 | Worker execution engine, idempotency, drain lifecycle, failure taxonomy |
| 12 | Observability hardening, drain metrics, recovery invariants |
| 13 | Control plane API (25 endpoints), runtime introspection |
| 14A/B/C | Tenant fairness (CFS vruntime), rate limiting, load-factor scheduling |
| 15 | Redis key centralization, atomic rate limiter (EVALSHA/Lua) |
| 16 | Per-tenant fair queue (CFS), feature-flag fair scheduling mode |
| 17 | Auth hardening (HMAC), audit logger, async CircuitBreaker, Redis resilience |
| 18 | Atomic enqueue Lua, atomic dispatch commit Lua, autoclaim fairness fix |
| 19 | SchedulerLoop (scoring + pacing), WorkerScorer, DispatchController (token bucket) |
| 20 | Real OpenAI executor, executor factory, ExecutionRequest/Result typed contracts |

## Quick Start

```bash
pip install -e hfa-core -e hfa-control -e hfa-worker

# Run tests
python -m pytest tests/core -q

# With real OpenAI executor
OPENAI_API_KEY=sk-... executor_mode=openai python -m hfa_worker.main
```

## Key Design Properties

- **Atomic scheduling**: `enqueue_admitted.lua` and `dispatch_commit.lua` guarantee no ghost runs or double-dispatch
- **CFS fairness**: Virtual runtime (vruntime) tracks per-tenant service debt; `pick_next()` always selects the most under-served tenant
- **Priority ordering**: `priority=1` → lowest score → dispatched first (ZPOPMIN semantics)
- **No silent drops**: Missing meta → explicit quarantine with `state=failed` and ERROR log
- **Fail-closed auth**: `CP_AUTH_SECRET` unset in production → HTTP 503 on all operator endpoints

## Configuration

```python
# fair_scheduling=True activates CFS fair queue + SchedulerLoop
config = ControlPlaneConfig(
    fair_scheduling=True,
    dispatch_tokens_capacity=128,
    dispatch_tokens_refill_per_sec=32.0,
    scheduler_loop_max_dispatches=32,
    scheduler_loop_max_duration_ms=100,
)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CP_AUTH_SECRET` | — | Operator auth secret (required in production) |
| `APP_ENV` | `development` | Set to `production` for fail-closed auth |
| `OPENAI_API_KEY` | — | Required when `executor_mode=openai` |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection |
| `CP_HOST` | `0.0.0.0` | Control plane bind address |
| `CP_PORT` | `8100` | Control plane port |
