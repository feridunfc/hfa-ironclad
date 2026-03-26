
from __future__ import annotations

import logging
from typing import Any

from hfa.state import transition_state
from hfa_control.backpressure import BackpressureGuard
from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore

logger = logging.getLogger(__name__)


class SchedulerLoop:
    """Persistent-state scheduler loop with event hooks and single commit authority."""

    def __init__(self, *args, **kwargs) -> None:
        if kwargs:
            self._dispatch_controller = kwargs.get("dispatch_controller")
            self._snapshot_builder = kwargs.get("snapshot_builder")
            self._worker_scorer = kwargs.get("worker_scorer")
            self._tenant_fairness = kwargs.get("tenant_fairness")
            self._config = kwargs.get("config")
            self._event_store = kwargs.get("event_store")
        else:
            self._dispatch_controller = args[7] if len(args) > 7 else None
            self._snapshot_builder = args[5] if len(args) > 5 else None
            self._worker_scorer = args[6] if len(args) > 6 else None
            self._tenant_fairness = args[4] if len(args) > 4 else None
            self._config = args[9] if len(args) > 9 else (args[-1] if args else None)
            self._event_store = None
        self._bp_guard = BackpressureGuard(self._config)

    async def on_leadership_gained(self) -> None:
        reset = getattr(self._tenant_fairness, "reset", None)
        if callable(reset):
            reset()
        initialise = getattr(self._dispatch_controller, "initialise", None)
        if initialise is not None:
            result = initialise()
            if hasattr(result, "__await__"):
                await result
        self._bp_guard.reset()

    async def on_leadership_lost(self) -> None:
        reset = getattr(self._tenant_fairness, "reset", None)
        if callable(reset):
            reset()
        self._bp_guard.reset()

    async def _quarantine_run(self, run_id: str, reason: str) -> None:
        logger.warning("SchedulerLoop quarantine: run_id=%s reason=%s", run_id, reason)

    async def commit_dispatch(self, run_id: str, *, expected_state: str = "queued", target_state: str = "scheduled") -> Any:
        redis = getattr(self._dispatch_controller, "redis", None)
        if redis is None:
            return None
        return await transition_state(
            redis,
            run_id=run_id,
            expected_state=expected_state,
            target_state=target_state,
            state_key=run_id,
        )

    async def _dispatch_once(self, snapshot: Any) -> bool:
        controller = self._dispatch_controller
        if controller is None:
            return False

        for name in ("dispatch_once", "try_dispatch_once", "run_once"):
            fn = getattr(controller, name, None)
            if fn is None:
                continue
            result = fn(snapshot=snapshot, worker_scorer=self._worker_scorer)
            if hasattr(result, "__await__"):
                result = await result
            if isinstance(result, dict):
                run_id = result.get("run_id")
                worker_id = result.get("worker_id")
                tenant_id = result.get("tenant_id")
                if run_id:
                    emit_event_background(
                        self._event_store,
                        run_id=run_id,
                        event_type=EventStore.EVENT_TASK_SCHEDULED,
                        worker_id=worker_id,
                        details={"tenant_id": tenant_id} if tenant_id else None,
                    )
            return bool(result)
        return False

    async def run_cycle(self, max_dispatches: int | None = None) -> int:
        snapshot = await self._snapshot_builder.build_capacity_snapshot()
        decision = self._bp_guard.evaluate(
            inflight=getattr(snapshot, "total_inflight", 0),
            capacity=max(getattr(snapshot, "total_capacity", 0), 1),
            saturation=getattr(snapshot, "saturation", None),
            max_dispatches_requested=int(max_dispatches or getattr(snapshot, "max_dispatches_this_cycle", 0) or 0),
        )
        if decision.throttled:
            logger.info("SchedulerLoop throttled: reason=%s saturation=%.4f", decision.reason, decision.saturation)
            return 0

        permit = await self._dispatch_controller.current_permit()
        if not permit.allowed:
            return 0

        budget = int(max_dispatches or getattr(permit, "max_dispatches", 0) or 0)
        dispatched = 0
        for _ in range(max(budget, 0)):
            ok = await self._dispatch_once(snapshot)
            if not ok:
                break
            dispatched += 1
            consume = getattr(self._dispatch_controller, "try_consume", None)
            if consume is not None:
                consumed = consume(1)
                if hasattr(consumed, "__await__"):
                    await consumed
        return dispatched
