
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from hfa_control.worker_reservation import WorkerReservationManager


@dataclass(frozen=True)
class ReservationDispatchResult:
    ok: bool
    status: str
    worker_id: str = ""
    scheduler_epoch: str = ""


class SchedulerReservationDispatcher:
    def __init__(
        self,
        reservation_manager: WorkerReservationManager,
        dispatch_fn: Callable[..., Awaitable[bool]],
    ) -> None:
        self._reservation_manager = reservation_manager
        self._dispatch_fn = dispatch_fn

    async def reserve_and_dispatch(
        self,
        *,
        task_id: str,
        worker_id: str,
        scheduler_epoch: str,
        dispatch_payload: dict,
        reserved_at_ms: int | None = None,
    ) -> ReservationDispatchResult:
        reserved_at_ms = reserved_at_ms if reserved_at_ms is not None else int(time.time() * 1000)

        reservation = await self._reservation_manager.reserve(
            worker_id=worker_id,
            task_id=task_id,
            scheduler_epoch=scheduler_epoch,
            reserved_at_ms=reserved_at_ms,
        )
        if not reservation.ok:
            return ReservationDispatchResult(
                ok=False,
                status=reservation.status,
                worker_id=worker_id,
                scheduler_epoch=scheduler_epoch,
            )

        dispatched = False
        try:
            dispatched = await self._dispatch_fn(
                task_id=task_id,
                worker_id=worker_id,
                scheduler_epoch=scheduler_epoch,
                dispatch_payload=dispatch_payload,
            )
            if not dispatched:
                await self._reservation_manager.release(worker_id)
                return ReservationDispatchResult(
                    ok=False,
                    status="dispatch_failed",
                    worker_id=worker_id,
                    scheduler_epoch=scheduler_epoch,
                )

            return ReservationDispatchResult(
                ok=True,
                status="reserved_and_dispatched",
                worker_id=worker_id,
                scheduler_epoch=scheduler_epoch,
            )
        except Exception:
            if not dispatched:
                await self._reservation_manager.release(worker_id)
            raise
