"""
hfa-control/tests/test_sprint10_scheduler.py
IRONCLAD Sprint 10 & 22 — Scheduler tests (Contract Updated for PR-2 Determinism)
"""

from __future__ import annotations

import pytest
import fakeredis.aioredis as faredis
from unittest.mock import AsyncMock

from hfa.events.schema import RunAdmittedEvent
from hfa_control.scheduler import Scheduler
from hfa_control.models import WorkerStatus, ControlPlaneConfig


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _cfg(**kw) -> ControlPlaneConfig:
    data = {
        "instance_id": "test",
        "stale_run_timeout": 0.1,
        "recovery_sweep_interval": 999,
        "max_reschedule_attempts": 3,
    }
    data.update(kw)
    return ControlPlaneConfig(**data)


class MockWorker:
    """
    Duck-typed mock. Prevents Pydantic mutation errors while satisfying
    PR-2's scheduler property constraints.
    """

    def __init__(self, wid, group, region, capacity, inflight, caps=None):
        self.worker_id = wid
        self.worker_group = group
        self.region = region
        self.capacity = capacity
        self.inflight = inflight
        self.status = WorkerStatus.HEALTHY
        self.capabilities = caps or []
        self.agent_types = []
        self.available_slots = max(0, capacity - inflight)
        self.load_factor = (inflight / capacity) if capacity > 0 else 1.0
        self.schedulable = self.available_slots > 0


def _worker(
        wid="w0",
        group="grp-a",
        region="us-east-1",
        capacity=10,
        inflight=0,
        caps=None,
) -> MockWorker:
    return MockWorker(wid, group, region, capacity, inflight, caps)


def _event(
        run_id="run-001",
        tenant_id="acme",
        agent_type="coder",
        policy="LEAST_LOADED",
        region="",
) -> RunAdmittedEvent:
    return RunAdmittedEvent(
        run_id=run_id,
        tenant_id=tenant_id,
        agent_type=agent_type,
        preferred_placement=policy,
        preferred_region=region,
    )


async def _make_sched(redis, workers, shard=4, schedulable_workers=None) -> Scheduler:
    registry = AsyncMock()
    registry.list_healthy_workers = AsyncMock(return_value=workers)
    registry.list_schedulable_workers = AsyncMock(
        return_value=workers if schedulable_workers is None else schedulable_workers
    )
    shards = AsyncMock()
    shards.shard_for_group = AsyncMock(return_value=shard)
    cfg = _cfg()
    return Scheduler(redis, registry, shards, cfg)


# ── Test class ────────────────────────────────────────────────────────────────

class TestSchedulerPlacement:
    @pytest.mark.asyncio
    async def test_least_loaded_selects_min_load(self):
        redis = faredis.FakeRedis()
        workers = [
            _worker("w0", "grp-a", inflight=8, capacity=10),
            _worker("w1", "grp-b", inflight=2, capacity=10),  # least loaded
            _worker("w2", "grp-c", inflight=5, capacity=10),
        ]
        sched = await _make_sched(redis, workers, shard=3)
        evt = _event(policy="LEAST_LOADED")

        await sched._schedule(evt)
        sched._shards.shard_for_group.assert_called_once_with("grp-b", evt.run_id)

    @pytest.mark.asyncio
    async def test_least_loaded_raises_when_all_at_capacity(self):
        redis = faredis.FakeRedis()
        workers = [
            _worker("w0", "grp-a", inflight=10, capacity=10),
            _worker("w1", "grp-b", inflight=10, capacity=10),
        ]
        sched = await _make_sched(redis, workers, schedulable_workers=[])
        evt = _event()

        await sched._schedule(evt)
        state = await redis.get(f"hfa:run:state:{evt.run_id}")
        assert state.decode() == "failed"

    @pytest.mark.asyncio
    async def test_region_affinity_prefers_matching_region(self):
        eu_worker = _worker("eu0", "eu-grp", region="eu-west-1", inflight=1)
        us_worker = _worker("us0", "us-grp", region="us-east-1", inflight=0)

        sched = await _make_sched(None, [eu_worker, us_worker])
        evt = _event(policy="REGION_AFFINITY", region="eu-west-1")

        group = await sched._select_worker_group(evt, "REGION_AFFINITY")
        assert group == "eu-grp"

    @pytest.mark.asyncio
    async def test_region_affinity_fallback_when_no_regional(self):
        us_worker = _worker("us0", "us-grp", region="us-east-1", inflight=0)
        sched = await _make_sched(None, [us_worker])
        evt = _event(policy="REGION_AFFINITY", region="eu-west-1")

        group = await sched._select_worker_group(evt, "REGION_AFFINITY")
        assert group == "us-grp"

    @pytest.mark.asyncio
    async def test_round_robin_is_now_deterministic_fallback(self):
        workers = [
            _worker("w0", "grp-b", inflight=0),
            _worker("w1", "grp-a", inflight=0),
        ]
        sched = await _make_sched(None, workers)
        evt = _event(policy="ROUND_ROBIN")

        # PR-2 Contract: Must rely on deterministic lexical fallback, no rotation
        g1 = await sched._select_worker_group(evt, "ROUND_ROBIN")
        g2 = await sched._select_worker_group(evt, "ROUND_ROBIN")

        assert g1 == "grp-a"
        assert g2 == "grp-a"

    @pytest.mark.asyncio
    async def test_capability_match_prefers_capable_worker(self):
        capable = _worker("w0", "gpu-grp", caps=["python", "gpu"], inflight=0)
        incapable = _worker("w1", "cpu-grp", caps=["python"], inflight=0)

        sched = await _make_sched(None, [capable, incapable])
        evt = _event(agent_type="gpu_coder", policy="CAPABILITY_MATCH")

        group = await sched._select_worker_group(evt, "CAPABILITY_MATCH")
        # No exact match -> falls back to least loaded/lexical
        assert group in ("gpu-grp", "cpu-grp")

        # V22 FIX: Immutable testing pattern. Instead of mutating 'capable.capabilities',
        # we return a brand new mock registry response that simulates the correct state.
        capable_match = _worker("w0", "gpu-grp", caps=["python", "gpu", "gpu_coder"], inflight=0)
        sched._registry.list_schedulable_workers.return_value = [capable_match, incapable]

        group2 = await sched._select_worker_group(evt, "CAPABILITY_MATCH")
        assert group2 == "gpu-grp"

    @pytest.mark.asyncio
    async def test_run_state_set_to_scheduled(self):
        redis = faredis.FakeRedis()
        sched = await _make_sched(redis, [_worker()])
        evt = _event(run_id="run-sched-001")
        await sched._schedule(evt)
        state = await redis.get("hfa:run:state:run-sched-001")
        assert state.decode() == "scheduled"

    @pytest.mark.asyncio
    async def test_run_meta_written(self):
        redis = faredis.FakeRedis()
        sched = await _make_sched(redis, [_worker("w0", "grp-a")])
        evt = _event(run_id="run-meta-001", tenant_id="acme")
        await sched._schedule(evt)
        meta = await redis.hgetall("hfa:run:meta:run-meta-001")
        assert meta[b"tenant_id"] == b"acme"
        assert meta[b"worker_group"] == b"grp-a"

    @pytest.mark.asyncio
    async def test_run_scheduled_event_emitted(self):
        redis = faredis.FakeRedis()
        sched = await _make_sched(redis, [_worker()])
        await sched._schedule(_event(run_id="run-sev-001"))
        msgs = await redis.xrange("hfa:stream:control")
        event_types = [m[1].get(b"event_type", b"").decode() for m in msgs]
        assert "RunScheduled" in event_types

    @pytest.mark.asyncio
    async def test_run_requested_event_forwarded_to_shard(self):
        redis = faredis.FakeRedis()
        sched = await _make_sched(redis, [_worker()], shard=5)
        await sched._schedule(_event(run_id="run-shard-001"))
        msgs = await redis.xrange("hfa:stream:runs:5")
        assert len(msgs) == 1
        assert msgs[0][1][b"event_type"] == b"RunRequested"

    @pytest.mark.asyncio
    async def test_placement_error_when_no_healthy_workers(self):
        redis = faredis.FakeRedis()
        sched = await _make_sched(redis, [], schedulable_workers=[])
        await sched._schedule(_event(run_id="run-fail-001"))
        state = await redis.get("hfa:run:state:run-fail-001")
        assert state.decode() == "failed"

    @pytest.mark.asyncio
    async def test_run_added_to_running_zset(self):
        redis = faredis.FakeRedis()
        sched = await _make_sched(redis, [_worker()])
        await sched._schedule(_event(run_id="run-zset-001"))
        score = await redis.zscore("hfa:cp:running", "run-zset-001")
        assert score is not None
        assert score > 0