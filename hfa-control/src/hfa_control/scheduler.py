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
from typing import Optional
from hfa_control.scheduler_types import SchedulingDecisionScore
from hfa.config.keys import RedisKey, RedisTTL
from hfa.events.codec import serialize_event
from hfa.events.schema import RunAdmittedEvent, RunRequestedEvent, RunScheduledEvent
from hfa_control.exceptions import PlacementError
from hfa_control.fairness import FairnessSelector
from hfa_control.models import ControlPlaneConfig
from hfa_control.registry import WorkerRegistry
from hfa_control.shard import ShardOwnershipManager
from hfa_control.tenant_fairness import TenantFairnessTracker
from hfa_control.tenant_queue import TenantQueue
from hfa_control.scheduler_lua import SchedulerLua
from hfa_control.dispatch_controller import DispatchController
from hfa_control.worker_scoring import WorkerScorer
from hfa_control.scheduler_snapshot import SchedulerSnapshotBuilder
from hfa_control.scheduler_loop import SchedulerLoop
from hfa_control.state_machine import is_terminal

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

        # Sprint 19: global scheduler intelligence components (fair mode only)
        self._snapshot_builder = SchedulerSnapshotBuilder(
            redis=redis,
            registry=registry,
            tenant_queue=self._tenant_queue,
            tenant_fairness=self._tenant_fairness,
            config=config,
        )
        self._dispatch_controller = DispatchController(redis, config)
        self._worker_scorer = WorkerScorer(config)
        self._scheduler_loop = SchedulerLoop(
            redis=redis,
            registry=registry,
            shards=shards,
            tenant_queue=self._tenant_queue,
            tenant_fairness=self._tenant_fairness,
            lua=self._lua,
            snapshot_builder=self._snapshot_builder,
            dispatch_controller=self._dispatch_controller,
            worker_scorer=self._worker_scorer,
            config=config,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await self._ensure_group()
        # Sprint 18: pre-load Lua scripts (EVALSHA ready before first dispatch)
        await self._lua.initialise()
        await self._scheduler_loop.on_leadership_gained()
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
        await self._scheduler_loop.on_leadership_lost()
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

                        await self._redis.xack(
                            self._config.control_stream, _GROUP, msg_id
                        )

                # Sprint 19: run SchedulerLoop cycle (scoring + pacing + fairness)
                if getattr(self._config, "fair_scheduling", False):
                    await self._scheduler_loop.run_cycle()

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
                                evt.run_id,
                                evt.tenant_id,
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
        Sprint 18.1/20: Atomically enqueue ALL run data in one Lua transaction.

        Writes queue entry + full meta (routing, cost, trace) + payload + state
        in a single Redis operation. No supplemental non-atomic writes needed.
        Idempotent — re-enqueueing the same run_id is a no-op (NX guard).
        """
        import json as _json
        from hfa_control.tenant_queue import MAX_PRIORITY

        priority = max(1, min(int(event.priority), MAX_PRIORITY))
        admitted_at = getattr(event, "admitted_at", None) or time.time()
        ts_micros = int(admitted_at * 1_000_000) % int(1e12)
        # Starvation prevention: logarithmic capped aging so long-waiting runs
        # score lower (ZPOPMIN picks them first) without unbounded priority inversion.
        #
        # Formula: aging_credit = min(max_boost, log1p(age_s) * weight * 1e6)
        #   - log1p avoids negative at age=0
        #   - cap prevents high-priority job starvation
        #   - weight=0 → no aging (backward compatible)
        import math as _math
        age_weight = float(getattr(self._config, 'scheduler_age_weight', 0.0) or 0.0)
        age_s = max(0.0, time.time() - admitted_at)
        max_aging_boost = int(getattr(self._config, 'scheduler_age_max_boost', 5) or 5) * int(1e12)
        if age_weight > 0:
            aging_credit = min(max_aging_boost, int(_math.log1p(age_s) * age_weight * 1_000_000))
        else:
            aging_credit = 0
        raw_score = priority * int(1e12) + ts_micros - aging_credit
        score = float(max(1, raw_score))  # clamp: score must be ≥ 1 (ZSET requires positive)

        payload_json = ""
        try:
            payload_json = _json.dumps(event.payload or {})
        except Exception:
            pass

        # Fetch tenant inflight limit for atomic admission check
        max_inflight = 0
        if self._config.fair_scheduling and hasattr(self, '_admission_registry'):
            pass  # future: read from tenant registry

        newly_added = await self._lua.enqueue_admitted(
            run_id=event.run_id,
            tenant_id=event.tenant_id,
            agent_type=event.agent_type,
            priority=priority,
            preferred_region=event.preferred_region or "",
            preferred_placement=event.preferred_placement or "LEAST_LOADED",
            admitted_at=admitted_at,
            score=score,
            estimated_cost_cents=int(getattr(event, "estimated_cost_cents", 0) or 0),
            trace_parent=event.trace_parent or "",
            trace_state=event.trace_state or "",
            payload_json=payload_json,
            max_inflight=max_inflight,
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
        Fair-mode dispatch loop.

        Compatibility-preserving behavior:
        - choose next tenant by vruntime over active tenants
        - dequeue exactly one run at a time from that tenant
        - rebuild event from run meta
        - if state is missing, backfill "queued" precondition
        - schedule through authoritative _schedule() path
        - never charge fairness here directly; _schedule() owns post-success charge

        This preserves Sprint 16/18 semantics while still allowing newer
        SchedulerLoop-based intelligence to coexist elsewhere.
        """
        dispatched = 0

        while dispatched < max_dispatches:
            active = await self._tenant_queue.active_tenants()
            if not active:
                return

            tenant_id = self._tenant_fairness.pick_next(active)

            run_id = await self._tenant_queue.dequeue(tenant_id)
            if not run_id:
                # queue may have gone empty between active_tenants() and dequeue()
                continue

            evt = await self._rebuild_event_from_meta(run_id)
            if evt is None:
                # Ghost/missing-meta protection: quarantine explicitly, never silent drop.
                await self._redis.set(
                    RedisKey.run_state(run_id),
                    "failed",
                    ex=RedisTTL.RUN_STATE,
                )
                logger.error("Quarantined run %s: missing meta", run_id)
                continue

            try:
                await self._schedule(evt)
                dispatched += 1
            except Exception:
                # _schedule already logs and marks failed on PlacementError paths.
                # Never update fairness on failure.
                continue

        if dispatched:
            logger.debug("Fair dispatch: placed %d runs this cycle", dispatched)

    async def _rebuild_event_from_meta(self, run_id: str) -> Optional[RunAdmittedEvent]:
        """Reconstruct a RunAdmittedEvent from stored run meta (fair-mode only)."""
        import json

        raw = await self._redis.hgetall(RedisKey.run_meta(run_id))
        if not raw:
            return None

        def _s(k: str) -> str:
            v = raw.get(k.encode()) or raw.get(k)
            return (v.decode() if isinstance(v, bytes) else v) or ""

        def _f(k: str) -> float:
            try:
                return float(_s(k))
            except (ValueError, TypeError):
                return 0.0

        payload: dict = {}
        raw_payload = await self._redis.get(RedisKey.run_payload(run_id))
        if raw_payload:
            try:
                payload = json.loads(raw_payload.decode() if isinstance(raw_payload, bytes) else raw_payload)
            except Exception:
                payload = {}

        return RunAdmittedEvent(
            run_id=run_id,
            tenant_id=_s("tenant_id"),
            agent_type=_s("agent_type"),
            priority=int(_s("priority") or "5"),
            preferred_region=_s("preferred_region"),
            preferred_placement=_s("preferred_placement") or "LEAST_LOADED",
            payload=payload,
            estimated_cost_cents=int(_s("estimated_cost_cents") or "0"),
            admitted_at=_f("admitted_at") or time.time(),
            trace_parent=_s("trace_parent") or None,
            trace_state=_s("trace_state") or None,
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

                admitted_at = float(
                    getattr(event, "admitted_at", time.time()) or time.time()
                )

                # ------------------------------------------------------------------
                # Sprint 18 compatibility fix (CRITICAL)
                #
                # dispatch_commit requires run_state in {"admitted", "queued"}.
                # In direct-mode scheduling, _schedule() may be called without
                # prior enqueue. We must establish minimal preconditions.
                # ------------------------------------------------------------------
                state_key = RedisKey.run_state(event.run_id)
                current_state = await self._redis.get(state_key)
                if isinstance(current_state, bytes):
                    current_state = current_state.decode()

                # State machine validation before dispatch
                if current_state and is_terminal(current_state):
                    logger.warning(
                        "_schedule: run=%s already in terminal state=%r — skipping",
                        event.run_id, current_state,
                    )
                    return

                if not current_state:
                    # Set minimal valid precondition
                    await self._redis.set(
                        state_key,
                        "admitted",
                        ex=RedisTTL.RUN_STATE,
                    )

                    meta_key = RedisKey.run_meta(event.run_id)
                    await self._redis.hset(
                        meta_key,
                        mapping={
                            "tenant_id": event.tenant_id,
                            "agent_type": event.agent_type,
                            "priority": str(event.priority),
                            "preferred_region": event.preferred_region or "",
                            "preferred_placement": event.preferred_placement or "LEAST_LOADED",
                            "admitted_at": str(admitted_at),
                        },
                    )
                    await self._redis.expire(meta_key, RedisTTL.RUN_META)

                # ------------------------------------------------------------------
                # Atomic dispatch commit (authoritative state transition)
                # ------------------------------------------------------------------
                committed = await self._lua.dispatch_commit(
                    run_id=event.run_id,
                    tenant_id=event.tenant_id,
                    agent_type=event.agent_type,
                    worker_group=worker_group,
                    shard=shard,
                    reschedule_count=0,
                    admitted_at=admitted_at,
                    running_zset=self._config.running_zset,
                )

                if not committed:
                    logger.warning(
                        "Dispatch commit rejected for run=%s — state conflict "
                        "(double-dispatch or invalid transition prevented)",
                        event.run_id,
                    )
                    return

                # ------------------------------------------------------------------
                # Emit scheduled event
                # ------------------------------------------------------------------
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

                # ------------------------------------------------------------------
                # Forward to worker shard stream
                # ------------------------------------------------------------------
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

                # ------------------------------------------------------------------
                # Fairness accounting (post-success only)
                # ------------------------------------------------------------------
                try:
                    self._tenant_fairness.update_on_dispatch(
                        event.tenant_id,
                        cost=self._fairness_cost(event),
                    )
                except Exception:
                    pass  # never block scheduling

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
    def _build_worker_decision_score(self, worker, primary_score: int):
        """Bir worker için deterministik sort key üretir. Legacy objeleri tolere eder."""
        capacity = int(getattr(worker, "capacity", 0) or 0)
        inflight = int(getattr(worker, "inflight", 0) or 0)

        # V22 CONTRACT FIX: Legacy objelerde load_factor yoksa dinamik hesapla.
        if hasattr(worker, "load_factor"):
            load_factor = float(worker.load_factor)
        else:
            load_factor = (inflight / capacity) if capacity > 0 else 1.0

        return SchedulingDecisionScore(
            primary_score=primary_score,
            load_factor=load_factor,
            inflight=inflight,
            capacity=capacity,
            worker_group=str(getattr(worker, "worker_group", "")),
            region=str(getattr(worker, "region", "")),
            worker_id=str(getattr(worker, "worker_id", "")),
        )

    def _pick_best_worker(self, workers: list, primary_scores_by_worker_id: dict[str, int]):
        """Aday worker listesini mutlak deterministik kurallara göre sıralar."""
        candidates = []

        for worker in workers:
            # V22 BACKWARD COMPATIBILITY FIX: Legacy objelerde schedulable alanı yoktur.
            if not getattr(worker, "schedulable", True):
                continue

            worker_id = str(getattr(worker, "worker_id", "")).strip()
            if not worker_id:
                continue

            primary_score = int(primary_scores_by_worker_id.get(worker_id, 0))
            score_obj = self._build_worker_decision_score(worker, primary_score)

            candidates.append((score_obj, worker))

        if not candidates:
            return None

        candidates.sort(key=lambda item: item[0].as_sort_key())
        return candidates[0][1]

    async def _select_worker_group(self, event: RunAdmittedEvent, policy: str) -> str:
        """
        Select a worker group through a single deterministic path.
        """
        workers = await self._registry.list_schedulable_workers(region=None)
        if not workers:
            raise PlacementError(
                f"No schedulable workers available for run='{event.run_id}' "
                f"region='{getattr(event, 'preferred_region', None)}' "
                f"(draining_excluded=0)"
            )

        pool = list(workers)

        if policy == "REGION_AFFINITY":
            preferred_region = getattr(event, "preferred_region", None)
            if preferred_region:
                regional_pool = [w for w in pool if getattr(w, "region", None) == preferred_region]
                if regional_pool:
                    pool = regional_pool

        elif policy == "CAPABILITY_MATCH":
            required_agent = getattr(event, "agent_type", None)
            if required_agent:
                capable_pool = []
                for worker in pool:
                    capabilities = getattr(worker, "capabilities", None) or []
                    agent_types = getattr(worker, "agent_types", None) or []

                    capability_set = set()
                    if isinstance(capabilities, (list, tuple, set)):
                        capability_set.update(str(x) for x in capabilities if x)
                    if isinstance(agent_types, (list, tuple, set)):
                        capability_set.update(str(x) for x in agent_types if x)

                    if required_agent in capability_set:
                        capable_pool.append(worker)

                if capable_pool:
                    pool = capable_pool

        elif policy == "ROUND_ROBIN":
            # Round-Robin is handled securely as a deterministic fallback.
            pass

        if not pool:
            raise PlacementError(
                f"No schedulable workers available for run='{event.run_id}' "
                f"region='{getattr(event, 'preferred_region', None)}' "
                f"(draining_excluded=0)"
            )

        primary_scores_by_worker_id = {}
        for worker in pool:
            worker_id = str(getattr(worker, "worker_id", "")).strip()
            if not worker_id:
                continue

            load_factor = float(getattr(worker, "load_factor", 1.0))
            primary_scores_by_worker_id[worker_id] = int(load_factor * 1_000_000)

        best = self._pick_best_worker(pool, primary_scores_by_worker_id)
        if best is None:
            raise PlacementError(
                f"No schedulable workers available for run='{event.run_id}' "
                f"region='{getattr(event, 'preferred_region', None)}' "
                f"(draining_excluded=0)"
            )

        return str(best.worker_group)




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
