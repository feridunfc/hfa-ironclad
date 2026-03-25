
import pytest

from hfa_control.observable_scheduler_capability_fair_dispatch import (
    ObservableSchedulerCapabilityFairDispatcher,
    ObservableTaskDispatchRequest,
)
from hfa_control.scheduler_capability_selector import WorkerCandidate
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.worker_reservation import WorkerReservationManager
from hfa.dag.schema import DagRedisKey

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_trace_contains_rejected_workers_and_selected_score(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    await redis_client.set(DagRedisKey.task_state("task-1"), "scheduled")

    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    reservation_dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    dispatcher = ObservableSchedulerCapabilityFairDispatcher(reservation_dispatcher)

    request = ObservableTaskDispatchRequest(
        tenant_id="tenant-a",
        task_id="task-1",
        required_capabilities=["python"],
        dispatch_payload={"tenant_id": "tenant-a"},
        vruntime=10.0,
        inflight=1,
    )
    workers = [
        WorkerCandidate(worker_id="worker-good", capabilities=["python"], current_load=1, capacity=5),
        WorkerCandidate(worker_id="worker-bad", capabilities=["frontend"], current_load=0, capacity=10),
    ]

    result = await dispatcher.dispatch_task(
        request=request,
        workers=workers,
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is True
    assert result.trace is not None
    assert result.trace.selected_worker_id == "worker-good"
    assert result.trace.selected_reason == "best_compatible_worker_by_score"
    assert result.trace.rejected_workers[0].worker_id == "worker-bad"
    assert result.trace.rejected_workers[0].reason == "capability_mismatch"
    assert result.trace.selected_score is not None


@pytest.mark.integration
async def test_trace_for_no_compatible_workers_is_stable(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    reservation_dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    dispatcher = ObservableSchedulerCapabilityFairDispatcher(reservation_dispatcher)

    request = ObservableTaskDispatchRequest(
        tenant_id="tenant-b",
        task_id="task-2",
        required_capabilities=["sql"],
        dispatch_payload={"tenant_id": "tenant-b"},
        vruntime=1.0,
        inflight=0,
    )
    workers = [
        WorkerCandidate(worker_id="worker-ui", capabilities=["frontend"], current_load=0, capacity=1),
        WorkerCandidate(worker_id="worker-design", capabilities=["design"], current_load=0, capacity=1),
    ]

    result = await dispatcher.dispatch_task(
        request=request,
        workers=workers,
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is False
    assert result.status == "no_compatible_workers"
    assert result.trace is not None
    assert result.trace.selected_reason == "no_compatible_workers"
    assert result.trace.candidate_count == 0
    assert len(result.trace.rejected_workers) == 2
