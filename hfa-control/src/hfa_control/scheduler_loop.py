
from __future__ import annotations

import logging
from typing import Any

from hfa_control.backpressure import BackpressureGuard

logger = logging.getLogger(__name__)


class SchedulerLoop:
    """Persistent-state scheduler loop.

    Runtime model:
    tenant queue -> snapshot builder -> fairness/scoring -> dispatch controller
    -> authoritative reservation -> atomic state transition.
    """

    def __init__(self, *args, **kwargs) -> None:
        # Repo-current keyword style
        if kwargs:
            self._dispatch_controller = kwargs.get("dispatch_controller")
            self._snapshot_builder = kwargs.get("snapshot_builder")
            self._worker_scorer = kwargs.get("worker_scorer")
            self._tenant_fairness = kwargs.get("tenant_fairness")
            self._config = kwargs.get("config")
        else:
            # Compatibility for older audit-style positional constructor tests
            self._dispatch_controller = args[7] if len(args) > 7 else None
            self._snapshot_builder = args[5] if len(args) > 5 else None
            self._worker_scorer = args[6] if len(args) > 6 else None
            self._tenant_fairness = args[4] if len(args) > 4 else None
            self._config = args[9] if len(args) > 9 else (args[-1] if args else None)

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

    async def _dispatch_once(self, snapshot: Any) -> bool:
        """Best-effort bridge into whichever dispatch API this repo currently exposes."""
        controller = self._dispatch_controller
        if controller is None:
            return False

        # Newer possible API: dispatch_once(snapshot=..., worker_scorer=...)
        for name in ("dispatch_once", "try_dispatch_once", "run_once"):
            fn = getattr(controller, name, None)
            if fn is not None:
                result = fn(snapshot=snapshot, worker_scorer=self._worker_scorer)
                if hasattr(result, "__await__"):
                    result = await result
                return bool(result)

        # Some controllers may expose choose_and_dispatch/dispatch_task style APIs;
        # without a candidate object here, we cannot safely synthesize one.
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
            logger.info(
                "SchedulerLoop throttled: reason=%s saturation=%.4f",
                decision.reason,
                decision.saturation,
            )
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
