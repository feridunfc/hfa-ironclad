"""
hfa-control/src/hfa_control/scheduler.py
IRONCLAD Sprint 10/14C/16 — Fair Scheduler

Sprint 16 changes
-----------------
The scheduler now operates in two modes, controlled by ControlPlaneConfig.fair_scheduling:

  False (default, backward-compatible):
    Direct mode — RunAdmittedEvents are scheduled immediately in arrival order.
    Existing behavior preserved. All Sprint 10-15 tests continue to pass.

  True (Sprint 16 fair mode):
    Queue-based fair mode:
      1. RunAdmittedEvent → enqueue to per-tenant queue (TenantQueue)
      2. Each scheduling tick: pick the most under-served tenant (TenantFairnessTracker)
      3. Dequeue the highest-priority run from that tenant
      4. Place it (unchanged placement policies)
      5. Update vruntime after successful dispatch

    This ensures:
      * No tenant starves — vruntime-based selection prevents monopolization.
      * Priority ordering within each tenant (score = priority + admission time).
      * Burst control — a heavy tenant's queue fills but doesn't skip the fairness gate.

Placement policies (unchanged)
-------------------------------
  LEAST_LOADED     — worker with lowest inflight/capacity ratio
  REGION_AFFINITY  — prefer workers in run's preferred_region, fall back global
  ROUND_ROBIN      — rotate across worker groups
  CAPABILITY_MATCH — require workers with matching agent_type capability

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop().
* close() always safe.
* cost_cents: int — never float USD.
* Fair mode is opt-in — default False preserves all existing behavior.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import List, Optional

from hfa.config.keys import RedisKey, RedisTTL
from hfa.events.codec import serialize_event
from hfa.events.schema import RunAdmittedEvent, RunRequestedEvent, RunScheduledEvent
from hfa_control.exceptions import PlacementError
from hfa_control.fairness import FairnessSelector
from hfa_control.models import ControlPlaneConfig, WorkerProfile
from hfa_control.registry import WorkerRegistry
from hfa_control.shard import ShardOwnershipManager
from hfa_control.tenant_fairness import TenantFairnessTracker
from hfa_control.tenant_queue import TenantQueue
from hfa_control.scheduler_lua import SchedulerLua

try:
    from hfa.obs.runtime_metrics import IRONCLADMetrics as _M
except Exception:
    _M = None  # type: ignore[assignment]

try:
    from hfa.obs.tracing import get_tracer  # type: ignore

    _tracer = get_tracer("hfa.scheduler")
except Exception:
    _tracer = None

logger = logging.getLogger(__name__)

_GROUP = "hfa-cp-scheduler"


class Scheduler:
    def __init__(
        self,
        redis,
        registry: WorkerRegistry,
        shards: ShardOwnershipManager,
        config: ControlPlaneConfig,
    ) -> None:
        self._redis = redis
        self._registry = registry
        self._shards = shards
        self._config = config
        self._task: Optional[asyncio.Task] = None
        self._rr_counter = 0  # round-robin state

        # Sprint 14A: legacy fairness selector (kept for compat)
        self._fairness = FairnessSelector()

        # Sprint 16: CFS-style vruntime tracker — now active in fair mode
        self._tenant_fairness = TenantFairnessTracker()

        # Sprint 16: per-tenant queue (only used when fair_scheduling=True)
        self._tenant_queue = TenantQueue(redis)

        # Sprint 18: Lua atomic operations for enqueue + dispatch commit
        self._lua = SchedulerLua(redis)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await self._ensure_group()
        # Sprint 18: pre-load Lua scripts (EVALSHA ready before first dispatch)
        await self._lua.initialise()
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(
            self._consume_admitted(), name="scheduler.consume"
        )
        logger.info("Scheduler started: instance=%s", self._config.instance_id)

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Scheduler closed")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._config.control_stream, _GROUP, id="0", mkstream=True
            )
        except Exception:
            # BUSYGROUP and equivalent "already exists" paths are safe to ignore
            pass

    async def _consume_admitted(self) -> None:
        consumer = f"scheduler-{self._config.instance_id}"
        while True:
            try:
                await self._autoclaim(consumer)

                msgs = await self._redis.xreadgroup(
                    groupname=_GROUP,
                    consumername=consumer,
                    streams={self._config.control_stream: ">"},
                    count=20,
                    block=2000,
                )

                for _stream, entries in msgs or []:
                    for msg_id, data in entries:
                        event_type = data.get(b"event_type") or data.get("event_type")
                        if isinstance(event_type, bytes):
                            event_type = event_type.decode()

                        if event_type == "RunAdmitted":
                            evt = RunAdmittedEvent.from_redis(data)
                            if getattr(self._config, "fair_scheduling", False):
                                # Sprint 16 fair mode: enqueue and let dispatch loop pick
                                await self._enqueue_admitted(evt)
                            else:
                                # Direct mode: schedule immediately (Sprint 10 behavior)
                                await self._schedule(evt)

                        await self._redis.xack(self._config.control_stream, _GROUP, msg_id)

                # Sprint 16: drain the fair queue after processing stream batch
                if getattr(self._config, "fair_scheduling", False):
                    await self._dispatch_fair_batch()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Scheduler._consume error: %s", exc, exc_info=True)
                await asyncio.sleep(1.0)

    async def _autoclaim(self, consumer: str) -> None:
        """
        Reclaim idle PEL messages from crashed/slow scheduler instances.

        Sprint 18.2 fix: In fair mode, recovered messages are RE-ENQUEUED
        through the tenant fair queue instead of being directly scheduled.
        This ensures EVERY admitted run goes through the fairness gate —
        no bypass path exists.

        In direct mode: previous behavior preserved (schedule immediately).
        """
        try:
            result = await self._redis.xautoclaim(
                self._config.control_stream,
                _GROUP,
                consumer,
                min_idle_time=self._config.autoclaim_idle_ms,
                start_id="0-0",
                count=self._config.autoclaim_count,
            )
            if result and result[1]:
                for msg_id, data in result[1]:
                    event_type = data.get(b"event_type") or data.get("event_type")
                    if isinstance(event_type, bytes):
                        event_type = event_type.decode()

                    if event_type == "RunAdmitted":
                        evt = RunAdmittedEvent.from_redis(data)
                        if getattr(self._config, "fair_scheduling", False):
                            # Sprint 18.2: re-enqueue through fair queue
                            # (idempotent — NX guard prevents duplicate)
                            await self._enqueue_admitted(evt)
                            logger.debug(
                                "Autoclaim: re-enqueued run=%s tenant=%s via fair queue",
                                evt.run_id, evt.tenant_id,
                            )
                        else:
                            await self._schedule(evt)

                    await self._redis.xack(self._config.control_stream, _GROUP, msg_id)
        except Exception as exc:
            logger.debug("Scheduler._autoclaim skipped: %s", exc)

    # ------------------------------------------------------------------
    # Sprint 16: Fair-mode queue helpers
    # ------------------------------------------------------------------

    async def _enqueue_admitted(self, event: RunAdmittedEvent) -> None:
        """
        Sprint 18.1: Atomically enqueue a run into the per-tenant fair queue.

        All writes (ZADD queue, HSET meta, SET state, SADD active-index) happen
        in a single Lua transaction. No ghost runs from partial writes.
        Idempotent — re-enqueuing the same run_id is a no-op (NX guard).
        """
        from hfa_control.tenant_queue import MAX_PRIORITY
        priority = max(1, min(int(event.priority), MAX_PRIORITY))
        admitted_at = getattr(event, "admitted_at", None) or time.time()
        ts_micros = int(admitted_at * 1_000_000) % int(1e12)
        score = float((MAX_PRIORITY - priority) * int(1e12) + ts_micros)

        newly_added = await self._lua.enqueue_admitted(
            run_id=event.run_id,
            tenant_id=event.tenant_id,
            agent_type=event.agent_type,
            priority=priority,
            preferred_region=event.preferred_region or "",
            preferred_placement=event.preferred_placement or "LEAST_LOADED",
            admitted_at=admitted_at,
            score=score,
        )

        if newly_added:
            logger.debug(
                "Fair enqueue [atomic]: run=%s tenant=%s priority=%d score=%.0f",
                event.run_id, event.tenant_id, priority, score,
            )
        else:
            logger.debug(
                "Fair enqueue [idempotent skip]: run=%s already queued",
                event.run_id,
            )

    async def _dispatch_fair_batch(self, max_dispatches: int = 20) -> None:
        """
        Sprint 16/18 fair dispatch loop.

        Sprint 18.3 fix: missing/corrupt meta is quarantined, never silently dropped.
        Sprint 18.5: dispatch commit is atomic via SchedulerLua.dispatch_commit().
        """
        dispatched = 0
        while dispatched < max_dispatches:
            active_tenants = await self._tenant_queue.active_tenants()
            if not active_tenants:
                break

            chosen_tenant = self._tenant_fairness.pick_next(active_tenants)
            run_id = await self._tenant_queue.dequeue(chosen_tenant)
            if run_id is None:
                continue

            event = await self._rebuild_event_from_meta(run_id)
            if event is None:
                # Sprint 18.3: quarantine — never silently drop a dequeued run
                logger.error(
                    "Fair dispatch: meta MISSING for run=%s (tenant=%s) — "
                    "quarantining as failed to prevent ghost run",
                    run_id, chosen_tenant,
                )
                if _M:
                    try:
                        _M.scheduling_failures_total.inc()
                    except Exception:
                        pass
                try:
                    await self._redis.set(
                        RedisKey.run_state(run_id), "failed", ex=RedisTTL.RUN_STATE
                    )
                except Exception:
                    pass
                continue

            await self._schedule(event)
            dispatched += 1

        if dispatched:
            logger.debug("Fair dispatch: placed %d runs this cycle", dispatched)

    async def _rebuild_event_from_meta(self, run_id: str) -> Optional[RunAdmittedEvent]:
        """Reconstruct a RunAdmittedEvent from stored run meta (fair-mode only)."""
        raw = await self._redis.hgetall(RedisKey.run_meta(run_id))
        if not raw:
            return None

        def _s(k: str) -> str:
            v = raw.get(k.encode()) or raw.get(k)
            return (v.decode() if isinstance(v, bytes) else v) or ""

        return RunAdmittedEvent(
            run_id=run_id,
            tenant_id=_s("tenant_id"),
            agent_type=_s("agent_type"),
            priority=int(_s("priority") or "5"),
            preferred_region=_s("preferred_region"),
            preferred_placement=_s("preferred_placement") or "LEAST_LOADED",
        )

    # ------------------------------------------------------------------
    # Placement decision
    # ------------------------------------------------------------------

    async def _schedule(self, event: RunAdmittedEvent) -> None:
        span = (
            _tracer.start_as_current_span("hfa.scheduler.place")
            if _tracer
            else _noop_span()
        )
        with span as sp:
            _set_attr(sp, "hfa.run_id", event.run_id)
            _set_attr(sp, "hfa.tenant_id", event.tenant_id)

            try:
                policy = event.preferred_placement or "LEAST_LOADED"
                worker_group = await self._select_worker_group(event, policy)
                shard = await self._shards.shard_for_group(worker_group, event.run_id)
                stream = RedisKey.stream_shard(shard)

                # Sprint 18.5: atomic dispatch commit
                # Precondition: run_state must be "admitted" or "queued"
                # Transitions state → "scheduled", updates meta, adds to running ZSET
                committed = await self._lua.dispatch_commit(
                    run_id=event.run_id,
                    tenant_id=event.tenant_id,
                    agent_type=event.agent_type,
                    worker_group=worker_group,
                    shard=shard,
                    reschedule_count=0,
                    admitted_at=getattr(event, "admitted_at", time.time()) or time.time(),
                    running_zset=self._config.running_zset,
                )
                if not committed:
                    logger.warning(
                        "Dispatch commit rejected for run=%s — already scheduled "
                        "or in terminal state (double-dispatch prevented)",
                        event.run_id,
                    )
                    return

                sched_evt = RunScheduledEvent(
                    run_id=event.run_id,
                    tenant_id=event.tenant_id,
                    agent_type=event.agent_type,
                    worker_group=worker_group,
                    shard=shard,
                    policy=policy,
                )
                await self._redis.xadd(
                    self._config.control_stream,
                    serialize_event(sched_evt),
                    maxlen=100_000,
                    approximate=True,
                )

                run_evt = RunRequestedEvent(
                    run_id=event.run_id,
                    tenant_id=event.tenant_id,
                    agent_type=event.agent_type,
                    priority=event.priority,
                    payload=event.payload,
                    idempotency_key=event.run_id,
                    trace_parent=event.trace_parent,
                    trace_state=event.trace_state,
                )
                await self._redis.xadd(
                    stream,
                    serialize_event(run_evt),
                    maxlen=50_000,
                    approximate=True,
                )

                _set_attr(sp, "hfa.worker_group", worker_group)
                _set_attr(sp, "hfa.shard", str(shard))
                _set_attr(sp, "hfa.policy", policy)

                # Sprint 14C: fairness accounting hook
                # Only updates AFTER all Redis writes succeed.
                # Never updates on PlacementError or unexpected exception.
                try:
                    self._tenant_fairness.update_on_dispatch(
                        event.tenant_id,
                        cost=self._fairness_cost(event),
                    )
                except Exception:
                    pass  # fairness is instrumentation — never block scheduling

                logger.info(
                    "Scheduled: run=%s group=%s shard=%d policy=%s",
                    event.run_id,
                    worker_group,
                    shard,
                    policy,
                )

            except PlacementError as exc:
                logger.error("PlacementError: run=%s %s", event.run_id, exc)
                if _M:
                    _M.scheduling_failures_total.inc()
                await self._redis.set(
                    RedisKey.run_state(event.run_id),
                    "failed",
                    ex=RedisTTL.RUN_STATE,
                )
            except Exception as exc:
                logger.error(
                    "Scheduler._schedule error: run=%s %s",
                    event.run_id,
                    exc,
                    exc_info=True,
                )

    # ------------------------------------------------------------------
    # Fairness helpers
    # ------------------------------------------------------------------

    def _fairness_cost(self, event: RunAdmittedEvent) -> float:
        """
        Compute fairness accounting cost for a dispatched run.

        Rules
        -----
        * If estimated_cost_cents is missing or <= 0: default to 1.0.
        * Otherwise: cost_cents / 100, clamped to [1.0, 100.0].
        * Ensures vruntime increments are bounded and comparable across tenants.
        """
        cost_cents = getattr(event, "estimated_cost_cents", 0) or 0
        if cost_cents <= 0:
            return 1.0
        return max(1.0, min(float(cost_cents) / 100.0, 100.0))

    # ------------------------------------------------------------------
    # Policy implementations
    # ------------------------------------------------------------------

    async def _select_worker_group(self, event: RunAdmittedEvent, policy: str) -> str:
        # Sprint 14C: fairness observation (no behavior change)
        try:
            self._tenant_fairness.observe(event.tenant_id)
        except Exception:
            pass

        if _M:
            _M.scheduling_attempts_total.inc()

        region = event.preferred_region or self._config.region

        workers = await self._registry.list_schedulable_workers(region=region)
        if not workers:
            workers = await self._registry.list_schedulable_workers(region=None)

        all_alive = await self._registry.list_healthy_workers(region=None)
        draining_count = sum(1 for w in all_alive if w.is_draining)
        if _M and draining_count:
            _M.workers_excluded_draining_total.inc(draining_count)

        if not workers:
            if _M:
                _M.scheduling_failures_total.inc()
            raise PlacementError(
                f"No schedulable workers available for run={event.run_id!r} "
                f"region={region!r} (draining_excluded={draining_count})"
            )

        if policy == "REGION_AFFINITY":
            return self._policy_region_affinity(workers, event)
        if policy == "ROUND_ROBIN":
            return self._policy_round_robin(workers)
        if policy == "CAPABILITY_MATCH":
            return self._policy_capability_match(workers, event)
        return self._policy_least_loaded(workers)

    def _policy_least_loaded(self, workers: List[WorkerProfile]) -> str:
        """Select worker group with lowest inflight / capacity ratio."""
        workers = [w for w in workers if w.available_slots > 0]
        if not workers:
            raise PlacementError("All healthy workers are at capacity")
        best = min(workers, key=lambda w: w.load_factor)
        return best.worker_group

    def _policy_region_affinity(
        self, workers: List[WorkerProfile], event: RunAdmittedEvent
    ) -> str:
        """Prefer workers in preferred_region, fall back to LEAST_LOADED."""
        preferred = [
            w
            for w in workers
            if w.region == (event.preferred_region or self._config.region)
            and w.available_slots > 0
        ]
        pool = preferred if preferred else [w for w in workers if w.available_slots > 0]
        if not pool:
            raise PlacementError("No workers with available slots")
        return min(pool, key=lambda w: w.load_factor).worker_group

    def _policy_round_robin(self, workers: List[WorkerProfile]) -> str:
        """Round-robin across available worker groups."""
        available = [w for w in workers if w.available_slots > 0]
        if not available:
            raise PlacementError("No workers with available slots (round-robin)")

        seen: list[str] = []
        groups: list[WorkerProfile] = []
        for w in available:
            if w.worker_group not in seen:
                seen.append(w.worker_group)
                groups.append(w)

        target = groups[self._rr_counter % len(groups)]
        self._rr_counter += 1
        return target.worker_group

    def _policy_capability_match(
        self, workers: List[WorkerProfile], event: RunAdmittedEvent
    ) -> str:
        """
        Prefer workers that advertise event.agent_type in their capabilities.
        Fall back to LEAST_LOADED if no capable workers found.
        """
        capable = [
            w
            for w in workers
            if event.agent_type in w.capabilities and w.available_slots > 0
        ]
        pool = capable if capable else [w for w in workers if w.available_slots > 0]
        if not pool:
            raise PlacementError("No capable workers with available slots")
        return min(pool, key=lambda w: w.load_factor).worker_group


def _noop_span():
    class _S:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    return _S()


def _set_attr(span, key: str, value: str) -> None:
    try:
        span.set_attribute(key, value)
    except Exception:
        pass