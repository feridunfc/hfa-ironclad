"""
tests/core/test_sprint16_fair_scheduler.py
IRONCLAD Sprint 16 — Fair scheduler tests

Verifies:
  - fair_scheduling=False preserves Sprint 10 direct mode (backward compat)
  - fair_scheduling=True enqueues runs into per-tenant queues
  - _dispatch_fair_batch() picks tenants in vruntime order
  - Heavy tenant is deprioritized after burst
  - Starvation never occurs (every tenant gets scheduled)
  - PlacementError does not update vruntime
"""

from __future__ import annotations

import pytest
import fakeredis.aioredis as faredis
from unittest.mock import AsyncMock

from hfa.events.schema import RunAdmittedEvent
from hfa_control.scheduler import Scheduler
from hfa_control.models import ControlPlaneConfig, WorkerProfile, WorkerStatus
from hfa_control.tenant_queue import TenantQueue


# ---------------------------------------------------------------------------
# Test helpers (reuse sprint10 style)
# ---------------------------------------------------------------------------


def _cfg(fair_scheduling: bool = False, **kw) -> ControlPlaneConfig:
    data = {
        "instance_id": "test-sched",
        "stale_run_timeout": 0.1,
        "recovery_sweep_interval": 999,
        "max_reschedule_attempts": 3,
        "fair_scheduling": fair_scheduling,
    }
    data.update(kw)
    return ControlPlaneConfig(**data)


def _worker(wid="w0", group="grp-a", capacity=10, inflight=0) -> WorkerProfile:
    return WorkerProfile(
        worker_id=wid,
        worker_group=group,
        region="us-east-1",
        capacity=capacity,
        inflight=inflight,
        status=WorkerStatus.HEALTHY,
        capabilities=[],
    )


def _event(
    run_id="run-001",
    tenant_id="acme",
    agent_type="coder",
    priority=5,
    policy="LEAST_LOADED",
) -> RunAdmittedEvent:
    return RunAdmittedEvent(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type=agent_type,
        priority=priority,
        preferred_placement=policy,
    )


async def _make_sched(redis, workers, fair: bool = False, shard=4) -> Scheduler:
    registry = AsyncMock()
    registry.list_healthy_workers = AsyncMock(return_value=workers)
    registry.list_schedulable_workers = AsyncMock(return_value=workers)
    shards = AsyncMock()
    shards.shard_for_group = AsyncMock(return_value=shard)
    return Scheduler(redis, registry, shards, _cfg(fair_scheduling=fair))


# ---------------------------------------------------------------------------
# Backward compatibility — fair_scheduling=False
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_direct_mode_schedule_still_works():
    """fair_scheduling=False must preserve Sprint 10 scheduling behavior."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()])

    evt = _event(run_id="run-direct-001")
    await sched._schedule(evt)

    state = await redis.get(f"hfa:run:state:run-direct-001")
    s = state.decode() if isinstance(state, bytes) else state
    assert s == "scheduled"


@pytest.mark.asyncio
async def test_direct_mode_fairness_hook_still_fires():
    """In direct mode, vruntime accounting still fires after dispatch."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()])

    evt = _event(run_id="run-direct-002", tenant_id="acme")
    assert sched._tenant_fairness.get("acme") == 0.0

    await sched._schedule(evt)
    assert sched._tenant_fairness.get("acme") > 0.0


# ---------------------------------------------------------------------------
# Fair mode — enqueue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fair_mode_enqueue_adds_to_tenant_queue():
    """In fair mode, admitted runs go into the tenant queue."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()], fair=True)

    evt = _event(run_id="run-fair-001", tenant_id="acme")
    await sched._enqueue_admitted(evt)

    depth = await sched._tenant_queue.depth("acme")
    assert depth == 1


@pytest.mark.asyncio
async def test_fair_mode_enqueue_stores_meta():
    """Enqueued run must have its meta stored for later reconstruction."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()], fair=True)

    evt = _event(run_id="run-fair-002", tenant_id="acme", agent_type="coder")
    await sched._enqueue_admitted(evt)

    rebuilt = await sched._rebuild_event_from_meta("run-fair-002")
    assert rebuilt is not None
    assert rebuilt.tenant_id == "acme"
    assert rebuilt.agent_type == "coder"


@pytest.mark.asyncio
async def test_rebuild_event_returns_none_for_missing_meta():
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()], fair=True)
    result = await sched._rebuild_event_from_meta("ghost-run")
    assert result is None


# ---------------------------------------------------------------------------
# Fair mode — dispatch batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_fair_batch_schedules_queued_run():
    """_dispatch_fair_batch() should place a queued run."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()], fair=True)

    evt = _event(run_id="run-dispatch-001", tenant_id="acme")
    await sched._enqueue_admitted(evt)
    await sched._dispatch_fair_batch()

    state = await redis.get("hfa:run:state:run-dispatch-001")
    s = state.decode() if isinstance(state, bytes) else state
    assert s == "scheduled"


@pytest.mark.asyncio
async def test_dispatch_fair_batch_empty_queue_is_noop():
    """_dispatch_fair_batch() with no queued runs should not crash."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()], fair=True)
    await sched._dispatch_fair_batch()  # no exception


@pytest.mark.asyncio
async def test_dispatch_fair_batch_updates_vruntime():
    """After fair dispatch, tenant's vruntime must increase."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()], fair=True)

    evt = _event(run_id="run-vrt-001", tenant_id="acme")
    await sched._enqueue_admitted(evt)
    assert sched._tenant_fairness.get("acme") == 0.0

    await sched._dispatch_fair_batch()
    assert sched._tenant_fairness.get("acme") > 0.0


# ---------------------------------------------------------------------------
# Starvation prevention
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_heavy_tenant_does_not_starve_light_tenant():
    """
    Heavy tenant (10 runs) vs light tenant (1 run).
    Light tenant must be scheduled before heavy tenant finishes its backlog.
    """
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()], fair=True)

    # Heavy: 10 runs
    for i in range(10):
        await sched._enqueue_admitted(_event(f"run-heavy-{i:03d}", tenant_id="heavy"))

    # Light: 1 run
    await sched._enqueue_admitted(_event("run-light-001", tenant_id="light"))

    # Dispatch first 5
    await sched._dispatch_fair_batch(max_dispatches=5)

    light_state = await redis.get("hfa:run:state:run-light-001")
    light_s = (light_state.decode() if isinstance(light_state, bytes) else light_state) or ""
    assert light_s == "scheduled", "Light tenant was starved by heavy tenant"


@pytest.mark.asyncio
async def test_two_tenants_get_equal_share():
    """Two tenants with equal backlog should each get ~50% of dispatches."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()], fair=True)

    for i in range(6):
        await sched._enqueue_admitted(_event(f"run-a-{i}", tenant_id="alpha"))
        await sched._enqueue_admitted(_event(f"run-b-{i}", tenant_id="beta"))

    await sched._dispatch_fair_batch(max_dispatches=6)

    scheduled_a = sum([1 for i in range(6) if (await redis.get(f"hfa:run:state:run-a-{i}") or b"").decode() == "scheduled"])
    scheduled_b = sum([1 for i in range(6) if (await redis.get(f"hfa:run:state:run-b-{i}") or b"").decode() == "scheduled"])

    assert abs(scheduled_a - scheduled_b) <= 1, (
        f"Unfair dispatch: alpha={scheduled_a} beta={scheduled_b}"
    )


# ---------------------------------------------------------------------------
# PlacementError does NOT update vruntime
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fair_dispatch_placement_failure_no_vruntime_update():
    """PlacementError during fair dispatch must not increase vruntime."""
    redis = faredis.FakeRedis()
    # No workers → PlacementError
    sched = await _make_sched(redis, [], fair=True)

    evt = _event(run_id="run-fail-001", tenant_id="fail-tenant")
    await sched._enqueue_admitted(evt)

    before = sched._tenant_fairness.get("fail-tenant")
    await sched._dispatch_fair_batch()
    after = sched._tenant_fairness.get("fail-tenant")

    assert after == before, "vruntime updated despite placement failure"


# ---------------------------------------------------------------------------
# Priority within fair mode
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_high_priority_run_dispatched_first_within_tenant():
    """Within a single tenant, priority-1 run should be dispatched before priority-9."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()], fair=True)

    now = 5000.0
    await sched._tenant_queue.enqueue("acme", "run-low-pri", priority=9, now=now)
    await sched._tenant_queue.enqueue("acme", "run-high-pri", priority=1, now=now + 0.01)

    # Store meta for both
    for run_id, pri in [("run-low-pri", 9), ("run-high-pri", 1)]:
        await redis.hset(
            f"hfa:run:meta:{run_id}",
            mapping={
                "run_id": run_id, "tenant_id": "acme",
                "agent_type": "coder", "priority": str(pri),
                "preferred_region": "", "preferred_placement": "LEAST_LOADED",
                "admitted_at": str(now), "queue_state": "queued",
            },
        )
        await redis.expire(f"hfa:run:meta:{run_id}", 86400)

    # Dispatch only 1
    await sched._dispatch_fair_batch(max_dispatches=1)

    # high-pri should be scheduled, low-pri should not
    high_state = await redis.get("hfa:run:state:run-high-pri")
    high_s = (high_state.decode() if isinstance(high_state, bytes) else high_state) or ""
    assert high_s == "scheduled"

    low_state = await redis.get("hfa:run:state:run-low-pri")
    assert not low_state or low_state.decode() != "scheduled"
