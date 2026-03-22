from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

from hfa.config.keys import RedisKey, RedisTTL
from hfa_control.scheduler_reasons import (
    QUARANTINE_REASONS, REQUEUE_REASONS,
    QUARANTINED_STATE_CONFLICT, QUARANTINED_ALREADY_RUNNING, QUARANTINED_ILLEGAL_TRANSITION,
    STATE_CONFLICT, ALREADY_RUNNING, ILLEGAL_TRANSITION, INTERNAL_ERROR,
)
from hfa.state import InvalidStateTransition, get_run_state, transition_state
from hfa.events.schema import RunAdmittedEvent
from hfa_control.backpressure import BackpressureGuard

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchedulerDecision:
    tenant_id: str
    run_id: str
    worker_group: str
    shard: int
    policy: str
    fairness_vruntime_before: float
    fairness_cost: float
    dispatch_reason: str


@dataclass(frozen=True)
class DispatchAttemptResult:
    committed: bool
    reason: str
    decision: Optional[SchedulerDecision] = None
    should_requeue: bool = False


class SchedulerLoop:
    def __init__(
        self,
        redis,
        registry,
        shards,
        tenant_queue,
        tenant_fairness,
        lua,
        snapshot_builder,
        dispatch_controller,
        worker_scorer,
        config,
        metrics=None,
    ) -> None:
        self._redis = redis
        self._registry = registry
        self._shards = shards
        self._tenant_queue = tenant_queue
        self._tenant_fairness = tenant_fairness
        self._lua = lua
        self._snapshot_builder = snapshot_builder
        self._dispatch_controller = dispatch_controller
        self._worker_scorer = worker_scorer
        self._config = config
        self._metrics = metrics

    async def on_leadership_gained(self) -> None:
        self._tenant_fairness.reset()
        await self._dispatch_controller.initialise()
        logger.info("SchedulerLoop leadership gained")

    async def on_leadership_lost(self) -> None:
        self._tenant_fairness.reset()
        logger.info("SchedulerLoop leadership lost")

    async def run_cycle(self, max_dispatches: Optional[int] = None) -> int:
        dispatched = 0
        started = time.monotonic()
        budget = max_dispatches or self._config.scheduler_loop_max_dispatches
        permit = await self._dispatch_controller.current_permit()
        if not permit.allowed or permit.max_dispatches <= 0:
            return 0
        budget = min(budget, permit.max_dispatches)

        # Backpressure evaluation before dispatch loop
        bp_guard = BackpressureGuard(self._config)
        try:
            cap = await self._snapshot_builder.build_capacity_snapshot()
            schedulable = [w for w in cap.workers if w.schedulable]
            load_factors = [w.load_factor for w in schedulable]
            capacities   = [w.capacity   for w in schedulable]
            queue_depths = await self._tenant_queue.all_depths()
            total_queued = sum(queue_depths.values())
            total_capacity = sum(capacities)
            global_inflight = total_capacity - cap.total_available_slots
            bp = bp_guard.evaluate(
                global_inflight=global_inflight,
                total_capacity=total_capacity,
                queue_depth=total_queued,
                worker_load_factors=load_factors,
                worker_capacities=capacities,
                max_dispatches_requested=budget,
            )
            if bp.is_hard:
                logger.info("Backpressure HARD active: %s — skipping cycle", bp.reason)
                return 0
            if bp.is_soft:
                logger.debug("Backpressure SOFT: %s — budget %d→%d",
                             bp.reason, budget, bp.allowed_dispatches)
                budget = bp.allowed_dispatches
        except Exception:
            logger.debug("Backpressure evaluation error — proceeding", exc_info=True)
        failures = 0
        while dispatched < budget:
            elapsed_ms = (time.monotonic() - started) * 1000.0
            if elapsed_ms >= self._config.scheduler_loop_max_duration_ms:
                break
            result = await self.dispatch_once()
            if result.committed:
                dispatched += 1
                failures = 0
                continue
            failures += 1
            if result.reason in {"dispatch_budget_exhausted", "no_active_tenants", "redis_degraded"}:
                break
            if failures >= getattr(self._config, "scheduler_loop_max_failures", 8):
                break
        return dispatched

    async def dispatch_once(self) -> DispatchAttemptResult:
        consumed = await self._dispatch_controller.try_consume(1)
        if not consumed:
            return DispatchAttemptResult(False, "dispatch_budget_exhausted")

        tenant_id = await self.pick_tenant()
        if tenant_id is None:
            await self._dispatch_controller.refund(1)
            return DispatchAttemptResult(False, "no_active_tenants")

        run_id = await self._tenant_queue.peek(tenant_id)
        if not run_id:
            await self._dispatch_controller.refund(1)
            return DispatchAttemptResult(False, "tenant_empty_or_invalid")

        event = await self._rebuild_full_event(run_id)
        if event is None:
            await self._quarantine_missing_meta(tenant_id, run_id)
            await self._tenant_queue.remove(tenant_id, run_id)
            await self._dispatch_controller.refund(1)
            return DispatchAttemptResult(False, "missing_meta_quarantined")

        worker_group, reason = await self.pick_worker(event)
        if worker_group is None:
            await self._dispatch_controller.on_dispatch_failure(reason)
            await self._dispatch_controller.refund(1)
            return DispatchAttemptResult(False, reason)

        return await self.commit_dispatch(event, worker_group)

    async def pick_tenant(self) -> Optional[str]:
        tenants = await self._snapshot_builder.list_candidate_tenants()
        eligible = [t.tenant_id for t in tenants if t.dispatchable and t.queued_depth > 0]
        if not eligible:
            return None
        return self._tenant_fairness.pick_next(eligible)

    async def pick_worker(self, event: RunAdmittedEvent) -> tuple[Optional[str], str]:
        snapshot = await self._snapshot_builder.build_capacity_snapshot()
        if not snapshot.dispatch_allowed:
            return None, snapshot.blocked_reason or "dispatch_disallowed"
        if snapshot.total_available_slots <= 0:
            return None, "worker_pool_empty"
        selection = self._worker_scorer.select_worker_group(
            snapshot.workers,
            agent_type=event.agent_type,
            preferred_region=event.preferred_region or self._config.region,
            policy=event.preferred_placement or "LEAST_LOADED",
        )
        return selection.chosen, selection.reason

    async def commit_dispatch(self, event: RunAdmittedEvent, worker_group: str) -> DispatchAttemptResult:
        policy = event.preferred_placement or "LEAST_LOADED"
        shard = await self._shards.shard_for_group(worker_group, event.run_id)

        dequeued = await self._tenant_queue.dequeue(event.tenant_id)
        if dequeued != event.run_id:
            await self._dispatch_controller.refund(1)
            if dequeued:
                try:
                    displaced = await self._rebuild_full_event(dequeued)
                    if displaced is not None:
                        await self._tenant_queue.enqueue(
                            displaced.tenant_id,
                            displaced.run_id,
                            priority=displaced.priority,
                            now=displaced.admitted_at,
                        )
                    else:
                        await self._quarantine_missing_meta(event.tenant_id, dequeued)
                except Exception:
                    logger.exception("Failed to restore displaced queue head run=%s", dequeued)
            return DispatchAttemptResult(False, "queue_head_changed")

        commit_result = await self._lua.dispatch_commit_detailed(
            run_id=event.run_id,
            tenant_id=event.tenant_id,
            agent_type=event.agent_type,
            worker_group=worker_group,
            shard=shard,
            reschedule_count=0,
            admitted_at=event.admitted_at,
            running_zset=self._config.running_zset,
            priority=event.priority,
            payload=event.payload,
            trace_parent=event.trace_parent,
            trace_state=event.trace_state,
            policy=policy,
            region=event.preferred_region or self._config.region,
            control_stream=self._config.control_stream,
            shard_stream=RedisKey.stream_shard(shard),
            control_stream_maxlen=RedisTTL.STREAM_MAXLEN,
            shard_stream_maxlen=RedisTTL.SHARD_MAXLEN,
        )
        if not commit_result.committed:
            await self._dispatch_controller.on_dispatch_failure(commit_result.status)
            if commit_result.should_requeue:
                await self._tenant_queue.enqueue(
                    event.tenant_id,
                    event.run_id,
                    priority=event.priority,
                    now=event.admitted_at,
                )
                await self._dispatch_controller.refund(1)
                return DispatchAttemptResult(False, f"{commit_result.status}_requeued", should_requeue=True)
            await self._dispatch_controller.refund(1)
            self._worker_scorer.observe_dispatch_failure(worker_group, commit_result.status)
            # Explicit quarantine for non-transient failures — no silent drops
            if commit_result.status in QUARANTINE_REASONS:
                q_reason = (
                    QUARANTINED_STATE_CONFLICT if commit_result.status == STATE_CONFLICT
                    else QUARANTINED_ALREADY_RUNNING if commit_result.status == ALREADY_RUNNING
                    else QUARANTINED_ILLEGAL_TRANSITION
                )
                await self._quarantine_run(
                    event.tenant_id, event.run_id,
                    reason=q_reason,
                    stage="commit_dispatch",
                )
                return DispatchAttemptResult(False, q_reason)
            return DispatchAttemptResult(False, commit_result.status)

        fairness_before = self._tenant_fairness.get(event.tenant_id)
        fairness_cost = self._fairness_cost(event)
        self._tenant_fairness.update_on_dispatch(event.tenant_id, fairness_cost)
        await self._dispatch_controller.on_dispatch_success()
        self._worker_scorer.observe_dispatch_success(worker_group)
        decision = SchedulerDecision(
            tenant_id=event.tenant_id,
            run_id=event.run_id,
            worker_group=worker_group,
            shard=shard,
            policy=policy,
            fairness_vruntime_before=fairness_before,
            fairness_cost=fairness_cost,
            dispatch_reason="committed",
        )
        return DispatchAttemptResult(True, "committed", decision)

    async def _rebuild_full_event(self, run_id: str) -> Optional[RunAdmittedEvent]:
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
            admitted_at=_f("admitted_at") or time.time(),
            estimated_cost_cents=int(_s("estimated_cost_cents") or "0"),
            trace_parent=_s("trace_parent") or None,
            trace_state=_s("trace_state") or None,
        )

    async def _quarantine_run(
            self,
            tenant_id: str,
            run_id: str,
            *,
            reason: str,
            stage: str = "unknown",
    ) -> None:
        """
        Explicit terminal quarantine for a run.
        """
        logger.error(
            "SchedulerLoop: quarantining run=%s tenant=%s reason=%s stage=%s",
            run_id, tenant_id, reason, stage,
        )

        current_state = await get_run_state(self._redis, run_id)

        try:
            _qr = await transition_state(
                self._redis, run_id, "failed",
                state_key=RedisKey.run_state(run_id),
                state_ttl=RedisTTL.RUN_STATE,
                expected_state=current_state
            )
            if not _qr:
                logger.warning("SchedulerLoop._quarantine_run: transition_state skipped for run=%s (already terminal?)",
                               run_id)
        except InvalidStateTransition as e:
            logger.warning("SchedulerLoop._quarantine_run: transition_state invalid for run=%s (%s)", run_id, e)

        try:
            await self._redis.hset(
                RedisKey.run_meta(run_id),
                mapping={
                    "dispatch_failure_reason": reason,
                    "dispatch_failure_stage": stage,
                    "queue_action": "quarantined",
                },
            )
        except Exception as meta_exc:
            logger.warning("_quarantine_run meta write failed: run=%s %s", run_id, meta_exc)

    async def _quarantine_missing_meta(self, tenant_id: str, run_id: str) -> None:
        """Backward-compat alias for missing-meta quarantine."""
        await self._quarantine_run(
            tenant_id, run_id,
            reason="missing_meta_quarantined",
            stage="meta_check",
        )

    def _fairness_cost(self, event: RunAdmittedEvent) -> float:
        """
        Deterministic fairness cost function.

        Must be:
        - monotonic
        - non-zero
        - tenant comparable
        """

        try:
            cost = float(event.estimated_cost_cents or 0)
        except Exception:
            cost = 0.0

        # 🔒 fail-safe: never zero cost
        if cost <= 0:
            cost = 1.0

        return cost
