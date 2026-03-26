
from __future__ import annotations

from dataclasses import dataclass

from hfa_control.effect_ledger import EffectLedger
from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore


@dataclass(frozen=True)
class ReservationDispatchResult:
    ok: bool
    status: str
    worker_id: str = ""
    task_id: str = ""
    run_id: str = ""
    tenant_id: str = ""
    scheduler_epoch: str = ""


SchedulerReservationDispatchResult = ReservationDispatchResult


class SchedulerReservationDispatcher:
    def __init__(
        self,
        reservation_manager,
        dispatch_fn,
        event_store: EventStore | None = None,
        effect_ledger: EffectLedger | None = None,
    ) -> None:
        self._reservation_manager = reservation_manager
        self._dispatch_fn = dispatch_fn
        self._event_store = event_store
        self._effect_ledger = effect_ledger

    @staticmethod
    def _dispatch_token(*, run_id: str, task_id: str, attempt: int, scheduler_epoch: str) -> str:
        return f"dispatch:{run_id}:{task_id}:attempt:{attempt}:epoch:{scheduler_epoch}"

    async def reserve_and_dispatch(
        self,
        *,
        task_id: str,
        worker_id: str,
        scheduler_epoch: str,
        dispatch_payload: dict,
        reserved_at_ms: int | None = None,
    ) -> ReservationDispatchResult:
        run_id = str(dispatch_payload.get("run_id", task_id))
        tenant_id = str(dispatch_payload.get("tenant_id", ""))
        attempt = int(dispatch_payload.get("attempt", 1) or 1)

        if self._effect_ledger is not None:
            token = self._dispatch_token(
                run_id=run_id,
                task_id=task_id,
                attempt=attempt,
                scheduler_epoch=scheduler_epoch,
            )
            receipt = await self._effect_ledger.acquire_effect(
                run_id=run_id,
                token=token,
                effect_type="dispatch",
                owner_id=worker_id,
            )
            if receipt.duplicate:
                return ReservationDispatchResult(
                    ok=False,
                    status="duplicate_dispatch_suppressed",
                    worker_id=receipt.owner_id or worker_id,
                    task_id=task_id,
                    run_id=run_id,
                    tenant_id=tenant_id,
                    scheduler_epoch=scheduler_epoch,
                )

        try:
            reserved = await self._reservation_manager.reserve(
                worker_id=worker_id,
                task_id=task_id,
                scheduler_epoch=scheduler_epoch,
                reserved_at_ms=reserved_at_ms,
            )
        except TypeError:
            try:
                reserved = await self._reservation_manager.reserve(
                    worker_id=worker_id,
                    run_id=task_id,
                    reserved_at_ms=reserved_at_ms,
                )
            except TypeError:
                reserved = await self._reservation_manager.reserve(worker_id, task_id)

        if not reserved.ok:
            return ReservationDispatchResult(
                ok=False,
                status=reserved.status,
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=getattr(reserved, "scheduler_epoch", scheduler_epoch),
            )

        dispatched = await self._dispatch_fn(
            task_id=task_id,
            worker_id=worker_id,
            scheduler_epoch=scheduler_epoch,
            dispatch_payload=dispatch_payload,
        )

        if not dispatched:
            release = getattr(self._reservation_manager, "release", None)
            if release is not None:
                await release(worker_id)
            return ReservationDispatchResult(
                ok=False,
                status="dispatch_failed",
                worker_id=worker_id,
                task_id=task_id,
                run_id=run_id,
                tenant_id=tenant_id,
                scheduler_epoch=scheduler_epoch,
            )

        result = ReservationDispatchResult(
            ok=True,
            status="reserved_and_dispatched",
            worker_id=worker_id,
            task_id=task_id,
            run_id=run_id,
            tenant_id=tenant_id,
            scheduler_epoch=scheduler_epoch,
        )
        emit_event_background(
            self._event_store,
            run_id=result.run_id,
            event_type=EventStore.EVENT_TASK_SCHEDULED,
            worker_id=result.worker_id,
            details={"tenant_id": result.tenant_id, "task_id": result.task_id},
        )
        return result
