import pytest

import fakeredis.aioredis as faredis

from hfa.events.schema import RunRequestedEvent
from hfa_worker.consumer import WorkerConsumer
from hfa_worker.execution_types import ExecutionResult, ExecutionRequest, ExecutionUsage


class CapturingExecutor:
    def __init__(self):
        self.captured_request = None

    async def execute(self, request: ExecutionRequest) -> ExecutionResult:
        self.captured_request = request
        return ExecutionResult(
            output_text="captured",
            usage=ExecutionUsage(total_tokens=0, estimated_cost_cents=0),
        )


@pytest.mark.asyncio
async def test_consumer_builds_execution_request():
    redis = faredis.FakeRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="test-worker",
        worker_group="test-group",
        shards=[0],
        executor=CapturingExecutor(),
    )

    event = RunRequestedEvent(
        run_id="run-test-123",
        tenant_id="tenant-abc",
        agent_type="test-agent",
        priority=5,
        payload={"prompt": "hello world"},
        trace_parent="trace-123",
        trace_state="state-456",
    )

    request = consumer._build_execution_request(event)

    assert request.run_id == "run-test-123"
    assert request.tenant_id == "tenant-abc"
    assert request.agent_type == "test-agent"
    assert request.payload == {"prompt": "hello world"}
    assert request.trace_parent == "trace-123"
    assert request.trace_state == "state-456"


@pytest.mark.asyncio
async def test_consumer_maps_payload_defaults():
    redis = faredis.FakeRedis()
    consumer = WorkerConsumer(
        redis=redis,
        worker_id="test",
        worker_group="test",
        shards=[0],
        executor=CapturingExecutor(),
    )

    event = RunRequestedEvent(
        run_id="run-1",
        tenant_id="t-1",
        agent_type="agent",
        payload=None,
    )

    request = consumer._build_execution_request(event)
    assert request.payload == {}
