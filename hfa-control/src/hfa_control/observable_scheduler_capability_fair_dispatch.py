
from __future__ import annotations

import uuid
from dataclasses import dataclass

from hfa_control.decision_observability import (
    DecisionBreakdown,
    RejectedWorkerReason,
    SchedulerDecisionTrace,
)
from hfa_control.scheduler_capability_selector import (
    SchedulerCapabilitySelector,
    WorkerCandidate,
)
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.scheduler_scoring import SchedulerScoring, ScoringCandidate


@dataclass(frozen=True)
class ObservableTaskDispatchRequest:
    tenant_id: str
    task_id: str
    required_capabilities: list[str]
    dispatch_payload: dict
    vruntime: float = 0.0
    inflight: int = 0


@dataclass(frozen=True)
class ObservableDispatchResult:
    ok: bool
    status: str
    tenant_id: str = ""
    task_id: str = ""
    worker_id: str = ""
    trace: SchedulerDecisionTrace | None = None


class ObservableSchedulerCapabilityFairDispatcher:
    def __init__(self, reservation_dispatcher: SchedulerReservationDispatcher) -> None:
        self._reservation_dispatcher = reservation_dispatcher

    async def dispatch_task(
        self,
        *,
        request: ObservableTaskDispatchRequest,
        workers: list[WorkerCandidate],
        scheduler_epoch: str,
        reserved_at_ms: int | None = None,
    ) -> ObservableDispatchResult:
        decision_id = str(uuid.uuid4())

        selection = SchedulerCapabilitySelector.filter_workers(
            required_capabilities=request.required_capabilities,
            workers=workers,
        )

        rejected = [
            RejectedWorkerReason(
                worker_id=worker_id,
                reason="capability_mismatch",
                detail={"missing_capabilities": selection.missing_by_worker.get(worker_id, [])},
            )
            for worker_id in selection.rejected_worker_ids
        ]

        if not selection.compatible:
            trace = SchedulerDecisionTrace(
                decision_id=decision_id,
                stage="capability_filter",
                selected_task_id=request.task_id,
                selected_tenant_id=request.tenant_id,
                selected_reason="no_compatible_workers",
                candidate_count=0,
                compatible_worker_ids=[],
                rejected_workers=rejected,
                scored_candidates=[],
            )
            return ObservableDispatchResult(
                ok=False,
                status="no_compatible_workers",
                tenant_id=request.tenant_id,
                task_id=request.task_id,
                worker_id="",
                trace=trace,
            )

        scored_raw = [
            ScoringCandidate(
                tenant_id=request.tenant_id,
                worker_id=w.worker_id,
                task_id=request.task_id,
                vruntime=request.vruntime,
                inflight=request.inflight,
                worker_load=w.current_load,
                capacity=w.capacity,
            )
            for w in selection.compatible
        ]

        scored_candidates = [
            DecisionBreakdown(
                tenant_id=c.tenant_id,
                task_id=c.task_id,
                worker_id=c.worker_id,
                vruntime=c.vruntime,
                inflight=c.inflight,
                worker_load=c.worker_load,
                capacity=c.capacity,
                score=SchedulerScoring.score(c),
            )
            for c in scored_raw
        ]

        best = SchedulerScoring.choose_best(scored_raw)
        assert best is not None

        dispatch_result = await self._reservation_dispatcher.reserve_and_dispatch(
            task_id=request.task_id,
            worker_id=best.worker_id,
            scheduler_epoch=scheduler_epoch,
            dispatch_payload=request.dispatch_payload,
            reserved_at_ms=reserved_at_ms,
        )

        trace = SchedulerDecisionTrace(
            decision_id=decision_id,
            stage="reserve_dispatch",
            selected_worker_id=best.worker_id,
            selected_task_id=request.task_id,
            selected_tenant_id=request.tenant_id,
            selected_score=SchedulerScoring.score(best),
            selected_reason="best_compatible_worker_by_score",
            candidate_count=len(scored_candidates),
            compatible_worker_ids=[w.worker_id for w in selection.compatible],
            rejected_workers=rejected,
            scored_candidates=scored_candidates,
        )

        return ObservableDispatchResult(
            ok=dispatch_result.ok,
            status=dispatch_result.status,
            tenant_id=request.tenant_id,
            task_id=request.task_id,
            worker_id=best.worker_id,
            trace=trace,
        )
