
import json
import pytest

from hfa.dag.schema import DagRedisKey
from hfa_control.decision_metrics import DecisionMetrics
from hfa_control.decision_trace_store import DecisionTraceStore
from hfa_control.observable_scheduler_capability_fair_dispatch import (
    ObservableSchedulerCapabilityFairDispatcher,
    ObservableTaskDispatchRequest,
)
from hfa_control.observable_scheduler_pipeline import ObservableSchedulerPipeline
from hfa_control.scheduler_capability_selector import WorkerCandidate
from hfa_control.scheduler_reservation_dispatch import SchedulerReservationDispatcher
from hfa_control.worker_reservation import WorkerReservationManager

pytestmark = pytest.mark.asyncio


@pytest.mark.integration
async def test_successful_dispatch_persists_trace_and_metrics(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    await redis_client.set(DagRedisKey.task_state("task-1"), "scheduled")

    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    reservation_dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    observable_dispatcher = ObservableSchedulerCapabilityFairDispatcher(reservation_dispatcher)
    trace_store = DecisionTraceStore(redis_client)
    metrics = DecisionMetrics(redis_client)
    pipeline = ObservableSchedulerPipeline(observable_dispatcher, trace_store, metrics)

    result = await pipeline.dispatch_with_observability(
        request=ObservableTaskDispatchRequest(
            tenant_id="tenant-a",
            task_id="task-1",
            required_capabilities=["python"],
            dispatch_payload={"tenant_id": "tenant-a"},
            vruntime=10.0,
            inflight=1,
        ),
        workers=[
            WorkerCandidate(worker_id="worker-good", capabilities=["python"], current_load=1, capacity=5),
            WorkerCandidate(worker_id="worker-bad", capabilities=["frontend"], current_load=0, capacity=10),
        ],
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is True
    assert result.trace_stream_id != ""
    assert result.worker_id == "worker-good"

    success_total = await metrics.get("dispatch_success_total")
    assert success_total == 1

    stream_entries = await redis_client.xrange(DagRedisKey.scheduler_decision_stream(), count=1)
    assert len(stream_entries) == 1
    payload = stream_entries[0][1]["decision_json"]
    data = json.loads(payload)
    assert data["selected_worker_id"] == "worker-good"
    assert data["selected_reason"] == "best_compatible_worker_by_score"


@pytest.mark.integration
async def test_no_compatible_workers_persists_trace_and_failure_metrics(redis_client):
    async def dispatch_fn(*, task_id: str, worker_id: str, scheduler_epoch: str, dispatch_payload: dict) -> bool:
        return True

    reservation_manager = WorkerReservationManager(redis_client, reservation_ttl_seconds=30, scheduler_id="sched-A")
    reservation_dispatcher = SchedulerReservationDispatcher(reservation_manager, dispatch_fn)
    observable_dispatcher = ObservableSchedulerCapabilityFairDispatcher(reservation_dispatcher)
    trace_store = DecisionTraceStore(redis_client)
    metrics = DecisionMetrics(redis_client)
    pipeline = ObservableSchedulerPipeline(observable_dispatcher, trace_store, metrics)

    result = await pipeline.dispatch_with_observability(
        request=ObservableTaskDispatchRequest(
            tenant_id="tenant-b",
            task_id="task-2",
            required_capabilities=["sql"],
            dispatch_payload={"tenant_id": "tenant-b"},
            vruntime=1.0,
            inflight=0,
        ),
        workers=[
            WorkerCandidate(worker_id="worker-ui", capabilities=["frontend"], current_load=0, capacity=1),
            WorkerCandidate(worker_id="worker-design", capabilities=["design"], current_load=0, capacity=1),
        ],
        scheduler_epoch="epoch-1",
        reserved_at_ms=123456,
    )

    assert result.ok is False
    assert result.status == "no_compatible_workers"
    assert result.trace_stream_id != ""

    failure_total = await metrics.get("dispatch_failure_total")
    no_compatible_total = await metrics.get("dispatch_no_compatible_workers_total")
    assert failure_total == 1
    assert no_compatible_total == 1

    stream_entries = await redis_client.xrange(DagRedisKey.scheduler_decision_stream(), count=1)
    assert len(stream_entries) == 1
    payload = stream_entries[0][1]["decision_json"]
    data = json.loads(payload)
    assert data["selected_reason"] == "no_compatible_workers"
    assert len(data["rejected_workers"]) == 2
