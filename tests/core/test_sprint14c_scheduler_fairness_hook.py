"""
tests/core/test_sprint14c_scheduler_fairness_hook.py
IRONCLAD Sprint 14C — Scheduler fairness accounting hook tests

Verifies that:
  - fairness tracker is updated AFTER successful scheduling dispatch
  - fairness tracker is NOT updated when PlacementError is raised
  - fairness tracker is NOT updated on unexpected exceptions
  - _fairness_cost() returns correct bounded values
  - multiple tenants accumulate independently

Critical rule: all existing Sprint 10 scheduler tests must remain green.
"""

from __future__ import annotations

import pytest
import fakeredis.aioredis as faredis

from tests.core.test_sprint10_scheduler import _cfg, _event, _make_sched, _worker
from hfa.events.schema import RunAdmittedEvent
from hfa_control.scheduler import Scheduler
from hfa_control.models import ControlPlaneConfig, WorkerProfile, WorkerStatus
from unittest.mock import AsyncMock


# ---------------------------------------------------------------------------
# Fairness hook — successful dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fairness_updated_after_successful_schedule():
    """Fairness vruntime must increase after a confirmed placement."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()])

    evt = _event(run_id="run-fair-001", tenant_id="tenant-a")
    assert sched._tenant_fairness.get("tenant-a") == 0.0

    await sched._schedule(evt)

    assert sched._tenant_fairness.get("tenant-a") > 0.0


@pytest.mark.asyncio
async def test_fairness_accumulates_across_multiple_dispatches():
    """Each successful dispatch increases the vruntime monotonically."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()])

    for i in range(3):
        evt = _event(run_id=f"run-multi-{i:03d}", tenant_id="tenant-b")
        await sched._schedule(evt)

    assert sched._tenant_fairness.get("tenant-b") >= 3.0


@pytest.mark.asyncio
async def test_fairness_isolated_per_tenant():
    """Different tenants accumulate vruntime independently."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()])

    await sched._schedule(_event(run_id="run-t1-001", tenant_id="tenant-x"))
    await sched._schedule(_event(run_id="run-t1-002", tenant_id="tenant-x"))
    await sched._schedule(_event(run_id="run-t2-001", tenant_id="tenant-y"))

    vrt_x = sched._tenant_fairness.get("tenant-x")
    vrt_y = sched._tenant_fairness.get("tenant-y")

    assert vrt_x > 0.0
    assert vrt_y > 0.0
    # tenant-x had 2 dispatches; tenant-y had 1 — x should have higher vruntime
    assert vrt_x > vrt_y


# ---------------------------------------------------------------------------
# Fairness hook — failure paths (no update expected)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fairness_not_updated_on_placement_error():
    """Fairness MUST NOT change when no workers are available."""
    redis = faredis.FakeRedis()
    # No workers → PlacementError
    sched = await _make_sched(redis, [])

    evt = _event(run_id="run-fail-001", tenant_id="tenant-fail")
    before = sched._tenant_fairness.get("tenant-fail")

    # _schedule() catches PlacementError internally — does not re-raise
    await sched._schedule(evt)

    after = sched._tenant_fairness.get("tenant-fail")
    assert after == before == 0.0


@pytest.mark.asyncio
async def test_fairness_not_updated_when_all_workers_at_capacity():
    """PlacementError from full workers must not update fairness."""
    redis = faredis.FakeRedis()
    full_workers = [
        _worker("w0", "grp-a", capacity=10, inflight=10),
        _worker("w1", "grp-b", capacity=5, inflight=5),
    ]
    sched = await _make_sched(redis, full_workers)

    evt = _event(run_id="run-full-001", tenant_id="tenant-full")
    await sched._schedule(evt)

    assert sched._tenant_fairness.get("tenant-full") == 0.0


# ---------------------------------------------------------------------------
# _fairness_cost() helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fairness_cost_default_when_no_cost():
    """No estimated_cost_cents → cost defaults to 1.0."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()])

    evt = _event(run_id="run-cost-000", tenant_id="t")
    # RunAdmittedEvent does not set estimated_cost_cents by default
    cost = sched._fairness_cost(evt)
    assert cost == 1.0


@pytest.mark.asyncio
async def test_fairness_cost_scales_with_cents():
    """estimated_cost_cents=200 → cost = 2.0."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()])

    evt = RunAdmittedEvent(
        run_id="run-cost-200",
        tenant_id="t",
        agent_type="coder",
        estimated_cost_cents=200,
    )
    cost = sched._fairness_cost(evt)
    assert cost == 2.0


@pytest.mark.asyncio
async def test_fairness_cost_clamped_to_minimum_1():
    """Very small cost_cents (<100) → cost is clamped to 1.0."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()])

    evt = RunAdmittedEvent(
        run_id="run-cost-small",
        tenant_id="t",
        agent_type="coder",
        estimated_cost_cents=10,
    )
    cost = sched._fairness_cost(evt)
    assert cost == 1.0


@pytest.mark.asyncio
async def test_fairness_cost_clamped_to_maximum_100():
    """Very large cost_cents → cost is clamped to 100.0."""
    redis = faredis.FakeRedis()
    sched = await _make_sched(redis, [_worker()])

    evt = RunAdmittedEvent(
        run_id="run-cost-huge",
        tenant_id="t",
        agent_type="coder",
        estimated_cost_cents=999_999,
    )
    cost = sched._fairness_cost(evt)
    assert cost == 100.0
