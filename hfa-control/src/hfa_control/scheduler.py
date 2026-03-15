# """
# hfa-control/src/hfa_control/scheduler.py
# IRONCLAD Sprint 10 — Scheduler
#
# Consumes RunAdmittedEvent from hfa:stream:control via XREADGROUP.
# Selects a worker_group using one of four placement policies.
# Emits RunScheduledEvent and XADD RunRequestedEvent to the target shard stream.
#
# Placement policies
# ------------------
#   LEAST_LOADED     — worker with lowest inflight/capacity ratio
#   REGION_AFFINITY  — prefer workers in run's preferred_region, fall back global
#   ROUND_ROBIN      — rotate across worker groups (deterministic via run sequence)
#   CAPABILITY_MATCH — require workers with matching agent_type capability
#
# XAUTOCLAIM
# ----------
#   Before each XREADGROUP call, XAUTOCLAIM reclaims messages idle > autoclaim_idle_ms.
#   This ensures Scheduler processes admissions even if a previous CP instance crashed
#   mid-processing and left entries in the PEL.
#
# IRONCLAD rules
# --------------
# * No print() — logging only.
# * No asyncio.get_event_loop() — get_running_loop().
# * close() always safe.
# * cost_cents: int — never float USD.
# """
# from __future__ import annotations
#
# import asyncio
# import logging
# import time
# from typing import List, Optional
#
# from hfa.events.schema import RunAdmittedEvent, RunScheduledEvent, RunRequestedEvent
# from hfa.events.codec  import serialize_event
# from hfa_control.models     import WorkerProfile, ControlPlaneConfig
# from hfa_control.registry   import WorkerRegistry
# from hfa_control.shard      import ShardOwnershipManager
# from hfa_control.exceptions import PlacementError
#
# try:
#     from hfa.obs.runtime_metrics import IRONCLADMetrics as _M
# except Exception:
#     _M = None  # type: ignore[assignment]
#
# try:
#     from hfa.obs.tracing import get_tracer  # type: ignore
#     _tracer = get_tracer("hfa.scheduler")
# except Exception:
#     _tracer = None
#
# logger = logging.getLogger(__name__)
#
# _GROUP = "hfa-cp-scheduler"
#
#
# class Scheduler:
#
#     def __init__(
#         self,
#         redis,
#         registry: WorkerRegistry,
#         shards:   ShardOwnershipManager,
#         config:   ControlPlaneConfig,
#     ) -> None:
#         self._redis    = redis
#         self._registry = registry
#         self._shards   = shards
#         self._config   = config
#         self._task:    Optional[asyncio.Task] = None
#         self._rr_counter = 0   # round-robin state
#
#     # ------------------------------------------------------------------
#     # Lifecycle
#     # ------------------------------------------------------------------
#
#     async def start(self) -> None:
#         await self._ensure_group()
#         loop = asyncio.get_running_loop()
#         self._task = loop.create_task(
#             self._consume_admitted(), name="scheduler.consume"
#         )
#         logger.info("Scheduler started: instance=%s", self._config.instance_id)
#
#     async def close(self) -> None:
#         if self._task:
#             self._task.cancel()
#             try:
#                 await self._task
#             except asyncio.CancelledError:
#                 pass
#         logger.info("Scheduler closed")
#
#     # ------------------------------------------------------------------
#     # Main loop
#     # ------------------------------------------------------------------
#
#     async def _ensure_group(self) -> None:
#         try:
#             await self._redis.xgroup_create(
#                 self._config.control_stream, _GROUP, id="0", mkstream=True
#             )
#         except Exception:
#             pass
#
#     async def _consume_admitted(self) -> None:
#         consumer = f"scheduler-{self._config.instance_id}"
#         while True:
#             try:
#                 # XAUTOCLAIM — recover PEL entries from dead CP instances
#                 await self._autoclaim(consumer)
#
#                 msgs = await self._redis.xreadgroup(
#                     groupname=_GROUP,
#                     consumername=consumer,
#                     streams={self._config.control_stream: ">"},
#                     count=20,
#                     block=2000,
#                 )
#                 for _stream, entries in (msgs or []):
#                     for msg_id, data in entries:
#                         et = (data.get(b"event_type") or b"").decode()
#                         if et == "RunAdmitted":
#                             evt = RunAdmittedEvent.from_redis(data)
#                             await self._schedule(evt)
#                         await self._redis.xack(
#                             self._config.control_stream, _GROUP, msg_id
#                         )
#
#             except asyncio.CancelledError:
#                 break
#             except Exception as exc:
#                 logger.error("Scheduler._consume error: %s", exc, exc_info=True)
#                 await asyncio.sleep(1.0)
#
#     async def _autoclaim(self, consumer: str) -> None:
#         try:
#             result = await self._redis.xautoclaim(
#                 self._config.control_stream,
#                 _GROUP,
#                 consumer,
#                 min_idle_time=self._config.autoclaim_idle_ms,
#                 start_id="0-0",
#                 count=self._config.autoclaim_count,
#             )
#             if result and result[1]:
#                 for msg_id, data in result[1]:
#                     et = (data.get(b"event_type") or b"").decode()
#                     if et == "RunAdmitted":
#                         evt = RunAdmittedEvent.from_redis(data)
#                         await self._schedule(evt)
#                     await self._redis.xack(
#                         self._config.control_stream, _GROUP, msg_id
#                     )
#         except Exception as exc:
#             logger.debug("Scheduler._autoclaim skipped: %s", exc)
#
#     # ------------------------------------------------------------------
#     # Placement decision
#     # ------------------------------------------------------------------
#
#     async def _schedule(self, event: RunAdmittedEvent) -> None:
#         span = (
#             _tracer.start_as_current_span("hfa.scheduler.place")
#             if _tracer else _noop_span()
#         )
#         with span as sp:
#             _set_attr(sp, "hfa.run_id",    event.run_id)
#             _set_attr(sp, "hfa.tenant_id", event.tenant_id)
#
#             try:
#                 policy       = event.preferred_placement or "LEAST_LOADED"
#                 worker_group = await self._select_worker_group(event, policy)
#                 shard        = await self._shards.shard_for_group(
#                     worker_group, event.run_id
#                 )
#                 stream = f"hfa:stream:runs:{shard}"
#
#                 # Write run metadata
#                 await self._redis.hset(
#                     f"hfa:run:meta:{event.run_id}",
#                     mapping={
#                         "run_id":           event.run_id,
#                         "tenant_id":        event.tenant_id,
#                         "agent_type":       event.agent_type,
#                         "worker_group":     worker_group,
#                         "shard":            str(shard),
#                         "reschedule_count": "0",
#                         "admitted_at":      str(event.admitted_at),
#                     },
#                 )
#                 await self._redis.expire(f"hfa:run:meta:{event.run_id}", 86400)
#                 await self._redis.set(
#                     f"hfa:run:state:{event.run_id}", "scheduled", ex=86400
#                 )
#
#                 # Track in running ZSET for recovery sweep
#                 await self._redis.zadd(
#                     self._config.running_zset,
#                     {event.run_id: time.time()},
#                 )
#
#                 # Emit RunScheduledEvent to control stream (observability)
#                 sched_evt = RunScheduledEvent(
#                     run_id=event.run_id,
#                     tenant_id=event.tenant_id,
#                     agent_type=event.agent_type,
#                     worker_group=worker_group,
#                     shard=shard,
#                     policy=policy,
#                 )
#                 await self._redis.xadd(
#                     self._config.control_stream,
#                     serialize_event(sched_evt),
#                     maxlen=100_000,
#                     approximate=True,
#                 )
#
#                 # XADD RunRequestedEvent to target shard stream
#                 run_evt = RunRequestedEvent(
#                     run_id=event.run_id,
#                     tenant_id=event.tenant_id,
#                     agent_type=event.agent_type,
#                     priority=event.priority,
#                     payload=event.payload,
#                     idempotency_key=event.run_id,
#                     trace_parent=event.trace_parent,
#                     trace_state=event.trace_state,
#                 )
#                 await self._redis.xadd(
#                     stream,
#                     serialize_event(run_evt),
#                     maxlen=50_000,
#                     approximate=True,
#                 )
#
#                 _set_attr(sp, "hfa.worker_group", worker_group)
#                 _set_attr(sp, "hfa.shard",        str(shard))
#                 _set_attr(sp, "hfa.policy",        policy)
#                 logger.info(
#                     "Scheduled: run=%s group=%s shard=%d policy=%s",
#                     event.run_id, worker_group, shard, policy,
#                 )
#
#             except PlacementError as exc:
#                 logger.error(
#                     "PlacementError: run=%s %s", event.run_id, exc
#                 )
#                 if _M:
#                     _M.scheduling_failures_total.inc()
#                 await self._redis.set(
#                     f"hfa:run:state:{event.run_id}", "failed", ex=86400
#                 )
#             except Exception as exc:
#                 logger.error(
#                     "Scheduler._schedule error: run=%s %s",
#                     event.run_id, exc, exc_info=True,
#                 )
#
#     # ------------------------------------------------------------------
#     # Policy implementations
#     # ------------------------------------------------------------------
#
#     async def _select_worker_group(
#         self, event: RunAdmittedEvent, policy: str
#     ) -> str:
#         if _M:
#             _M.scheduling_attempts_total.inc()
#
#         region   = event.preferred_region or self._config.region
#
#         # Use schedulable (non-draining) workers only.
#         workers  = await self._registry.list_schedulable_workers(region=region)
#         if not workers:
#             workers = await self._registry.list_schedulable_workers(region=None)
#
#         # Count draining workers that were excluded for observability.
#         all_alive = await self._registry.list_healthy_workers(region=None)
#         draining_count = sum(1 for w in all_alive if w.is_draining)
#         if _M and draining_count:
#             _M.workers_excluded_draining_total.inc(draining_count)
#
#         if not workers:
#             if _M:
#                 _M.scheduling_failures_total.inc()
#             raise PlacementError(
#                 f"No schedulable workers available for run={event.run_id!r} "
#                 f"region={region!r} (draining_excluded={draining_count})"
#             )
#
#         if policy == "REGION_AFFINITY":
#             return self._policy_region_affinity(workers, event)
#         if policy == "ROUND_ROBIN":
#             return self._policy_round_robin(workers)
#         if policy == "CAPABILITY_MATCH":
#             return self._policy_capability_match(workers, event)
#         return self._policy_least_loaded(workers)
#
#     def _policy_least_loaded(self, workers: List[WorkerProfile]) -> str:
#         """Select worker group with lowest inflight / capacity ratio."""
#         workers = [w for w in workers if w.available_slots > 0]
#         if not workers:
#             raise PlacementError("All healthy workers are at capacity")
#         best = min(workers, key=lambda w: w.load_factor)
#         return best.worker_group
#
#     def _policy_region_affinity(
#         self, workers: List[WorkerProfile], event: RunAdmittedEvent
#     ) -> str:
#         """Prefer workers in preferred_region, fall back to LEAST_LOADED."""
#         preferred = [
#             w for w in workers
#             if w.region == (event.preferred_region or self._config.region)
#             and w.available_slots > 0
#         ]
#         pool = preferred if preferred else [
#             w for w in workers if w.available_slots > 0
#         ]
#         if not pool:
#             raise PlacementError("No workers with available slots")
#         return min(pool, key=lambda w: w.load_factor).worker_group
#
#     def _policy_round_robin(self, workers: List[WorkerProfile]) -> str:
#         """Round-robin across available worker groups."""
#         available = [w for w in workers if w.available_slots > 0]
#         if not available:
#             raise PlacementError("No workers with available slots (round-robin)")
#         # Deduplicate groups preserving insertion order
#         seen:   list[str] = []
#         groups: list[WorkerProfile] = []
#         for w in available:
#             if w.worker_group not in seen:
#                 seen.append(w.worker_group)
#                 groups.append(w)
#         target = groups[self._rr_counter % len(groups)]
#         self._rr_counter += 1
#         return target.worker_group
#
#     def _policy_capability_match(
#         self, workers: List[WorkerProfile], event: RunAdmittedEvent
#     ) -> str:
#         """
#         Prefer workers that advertise event.agent_type in their capabilities.
#         Fall back to LEAST_LOADED if no capable workers found.
#         """
#         capable = [
#             w for w in workers
#             if event.agent_type in w.capabilities and w.available_slots > 0
#         ]
#         pool = capable if capable else [
#             w for w in workers if w.available_slots > 0
#         ]
#         if not pool:
#             raise PlacementError("No capable workers with available slots")
#         return min(pool, key=lambda w: w.load_factor).worker_group
#
#
# def _noop_span():
#     class _S:
#         def __enter__(self): return self
#         def __exit__(self, *_): pass
#     return _S()
#
#
# def _set_attr(span, key: str, value: str) -> None:
#     try:
#         span.set_attribute(key, value)
#     except Exception:
#         pass


"""
hfa-control/src/hfa_control/scheduler.py
IRONCLAD Sprint 10/12 — Scheduler

Consumes RunAdmittedEvent from hfa:stream:control via XREADGROUP.
Selects a worker_group using one of four placement policies.
Emits RunScheduledEvent and XADD RunRequestedEvent to the target shard stream.

Placement policies
------------------
  LEAST_LOADED     — worker with lowest inflight/capacity ratio
  REGION_AFFINITY  — prefer workers in run's preferred_region, fall back global
  ROUND_ROBIN      — rotate across worker groups (deterministic via run sequence)
  CAPABILITY_MATCH — require workers with matching agent_type capability

XAUTOCLAIM
----------
  Before each XREADGROUP call, XAUTOCLAIM reclaims messages idle > autoclaim_idle_ms.
  This ensures Scheduler processes admissions even if a previous CP instance crashed
  mid-processing and left entries in the PEL.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop().
* close() always safe.
* cost_cents: int — never float USD.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import  Optional

from hfa.events.schema import RunAdmittedEvent, RunScheduledEvent, RunRequestedEvent
from hfa.events.codec import serialize_event
from hfa_control.models import ControlPlaneConfig
from hfa_control.registry import WorkerRegistry
from hfa_control.shard import ShardOwnershipManager
from hfa_control.exceptions import PlacementError

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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await self._ensure_group()
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
            pass

    async def _consume_admitted(self) -> None:
        consumer = f"scheduler-{self._config.instance_id}"
        while True:
            try:
                # XAUTOCLAIM — recover PEL entries from dead CP instances
                await self._autoclaim(consumer)

                msgs = await self._redis.xreadgroup(
                    groupname=_GROUP,
                    consumername=consumer,
                    streams={self._config.control_stream: ">"},
                    count=20,
                    block=2000,
                )
                for _stream, entries in (msgs or []):
                    for msg_id, data in entries:
                        et = (data.get(b"event_type") or b"").decode()
                        if et == "RunAdmitted":
                            evt = RunAdmittedEvent.from_redis(data)
                            await self._schedule(evt)
                        await self._redis.xack(
                            self._config.control_stream, _GROUP, msg_id
                        )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Scheduler._consume error: %s", exc, exc_info=True)
                await asyncio.sleep(1.0)

    async def _autoclaim(self, consumer: str) -> None:
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
                    et = (data.get(b"event_type") or b"").decode()
                    if et == "RunAdmitted":
                        evt = RunAdmittedEvent.from_redis(data)
                        await self._schedule(evt)
                    await self._redis.xack(
                        self._config.control_stream, _GROUP, msg_id
                    )
        except Exception as exc:
            logger.debug("Scheduler._autoclaim skipped: %s", exc)

    # ------------------------------------------------------------------
    # Placement decision & Filtering
    # ------------------------------------------------------------------

    def _filter_schedulable_workers(self, workers: list) -> list:
        """
        Backward-compatible worker filtering.
        Safely handles both dicts (from Redis) and WorkerProfile objects (from tests/older sprints).
        """

        def _worker_field(worker, name, default=None):
            if isinstance(worker, dict):
                return worker.get(name, default)
            return getattr(worker, name, default)

        schedulable = []
        for worker in workers:
            # 1. Status Check (Default to True if missing)
            status = _worker_field(worker, "status", "healthy")
            # Enum veya string olarak 'healthy' kontrolü
            # is_healthy = status == "healthy" or (hasattr(status, "value") and status.value == "healthy")
            status_value = getattr(status, "value", status)
            is_healthy = status_value == "healthy"
            if not is_healthy:
                continue

            # 2. Draining Check (Default to False)
            is_draining = bool(_worker_field(worker, "is_draining", False))
            if is_draining:
                continue

            # 3. Capacity & Inflight Check (Safe cast to int)
            capacity = int(_worker_field(worker, "capacity", 0) or 0)
            inflight = int(_worker_field(worker, "inflight", 0) or 0)

            if capacity <= 0:
                continue
            if inflight >= capacity:
                continue

            schedulable.append(worker)

        return schedulable

    async def _select_worker_group(
            self, event: RunAdmittedEvent, policy: str
    ) -> str:
        if _M:
            _M.scheduling_attempts_total.inc()

        region = event.preferred_region or self._config.region

        def _get_field(w, field, default=None):
            return w.get(field, default) if isinstance(w, dict) else getattr(w, field, default)

        # ALWAYS fetch healthy workers first (fixes test mocks and legacy data)
        all_alive = await self._registry.list_healthy_workers(region=None)

        draining_count = sum(1 for w in all_alive if bool(_get_field(w, "is_draining", False)))
        if _M and draining_count:
            _M.workers_excluded_draining_total.inc(draining_count)

        # Apply safe filtering
        schedulable_all = self._filter_schedulable_workers(all_alive)

        # Filter by region
        workers = [w for w in schedulable_all if _get_field(w, "region") == region]
        if not workers:
            workers = schedulable_all

        if not workers:
            if _M:
                _M.scheduling_failures_total.inc()
            raise PlacementError(
                f"run={event.run_id} All healthy workers are at capacity"
            )

        if policy == "REGION_AFFINITY":
            return self._policy_region_affinity(workers, event)
        if policy == "ROUND_ROBIN":
            return self._policy_round_robin(workers)
        if policy == "CAPABILITY_MATCH":
            return self._policy_capability_match(workers, event)
        return self._policy_least_loaded(workers)

    async def _schedule(self, event: RunAdmittedEvent) -> None:
        span = (
            _tracer.start_as_current_span("hfa.scheduler.place")
            if _tracer else _noop_span()
        )
        with span as sp:
            _set_attr(sp, "hfa.run_id", event.run_id)
            _set_attr(sp, "hfa.tenant_id", event.tenant_id)

            try:
                policy = event.preferred_placement or "LEAST_LOADED"
                worker_group = await self._select_worker_group(event, policy)
                shard = await self._shards.shard_for_group(
                    worker_group, event.run_id
                )
                stream = f"hfa:stream:runs:{shard}"

                # Write run metadata
                await self._redis.hset(
                    f"hfa:run:meta:{event.run_id}",
                    mapping={
                        "run_id": event.run_id,
                        "tenant_id": event.tenant_id,
                        "agent_type": event.agent_type,
                        "worker_group": worker_group,
                        "shard": str(shard),
                        "reschedule_count": "0",
                        "admitted_at": str(event.admitted_at),
                    },
                )
                await self._redis.expire(f"hfa:run:meta:{event.run_id}", 86400)
                await self._redis.set(
                    f"hfa:run:state:{event.run_id}", "scheduled", ex=86400
                )

                # Track in running ZSET for recovery sweep
                await self._redis.zadd(
                    self._config.running_zset,
                    {event.run_id: time.time()},
                )

                # Emit RunScheduledEvent to control stream (observability)
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

                # XADD RunRequestedEvent to target shard stream
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
                logger.info(
                    "Scheduled: run=%s group=%s shard=%d policy=%s",
                    event.run_id, worker_group, shard, policy,
                )

            except PlacementError as exc:
                logger.error(
                    "PlacementError: %s", exc
                )
                if _M:
                    _M.scheduling_failures_total.inc()
                await self._redis.set(
                    f"hfa:run:state:{event.run_id}", "failed", ex=86400
                )
            except Exception as exc:
                logger.error(
                    "Scheduler._schedule error: run=%s %s",
                    event.run_id, exc, exc_info=True,
                )

    # ------------------------------------------------------------------
    # Policy implementations (Safe Access)
    # ------------------------------------------------------------------

    def _policy_least_loaded(self, workers: list) -> str:
        """Select worker group with lowest inflight / capacity ratio."""

        def _get_field(w, field, default=None):
            return w.get(field, default) if isinstance(w, dict) else getattr(w, field, default)

        def safe_load_factor(w):
            cap = int(_get_field(w, "capacity", 0) or 0)
            inf = int(_get_field(w, "inflight", 0) or 0)
            return inf / cap if cap > 0 else 1.0

        best = min(workers, key=safe_load_factor)
        return _get_field(best, "worker_group", "")

    def _policy_region_affinity(
            self, workers: list, event: RunAdmittedEvent
    ) -> str:
        """Prefer workers in preferred_region, fall back to LEAST_LOADED."""

        def _get_field(w, field, default=None):
            return w.get(field, default) if isinstance(w, dict) else getattr(w, field, default)

        preferred = [
            w for w in workers
            if _get_field(w, "region") == (event.preferred_region or self._config.region)
        ]
        pool = preferred if preferred else workers
        return self._policy_least_loaded(pool)

    def _policy_round_robin(self, workers: list) -> str:
        """Round-robin across available worker groups."""

        def _get_field(w, field, default=None):
            return w.get(field, default) if isinstance(w, dict) else getattr(w, field, default)

        seen: list[str] = []
        groups: list = []
        for w in workers:
            wg = _get_field(w, "worker_group")
            if wg not in seen:
                seen.append(wg)
                groups.append(w)

        target = groups[self._rr_counter % len(groups)]
        self._rr_counter += 1
        return _get_field(target, "worker_group", "")

    def _policy_capability_match(
            self, workers: list, event: RunAdmittedEvent
    ) -> str:
        """
        Prefer workers that advertise event.agent_type in their capabilities.
        Fall back to LEAST_LOADED if no capable workers found.
        """

        def _get_field(w, field, default=None):
            return w.get(field, default) if isinstance(w, dict) else getattr(w, field, default)

        capable = [
            w for w in workers
            if event.agent_type in _get_field(w, "capabilities", [])
        ]
        pool = capable if capable else workers
        return self._policy_least_loaded(pool)


def _noop_span():
    class _S:
        def __enter__(self): return self

        def __exit__(self, *_): pass

    return _S()


def _set_attr(span, key: str, value: str) -> None:
    try:
        span.set_attribute(key, value)
    except Exception:
        pass