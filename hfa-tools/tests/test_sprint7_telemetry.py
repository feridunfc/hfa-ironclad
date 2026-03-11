"""
hfa-tools/tests/test_sprint7_telemetry.py
IRONCLAD Sprint 7 Mini3 — Telemetry Hook Tests
"""

import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hfa.obs.metrics import HFAMetrics
from hfa_tools.services.orchestrator import (
    QueueFullError,
    RunOrchestrator,
    RunRequest,
    RunResult,
    RunStatus,
)
from hfa_tools.sandbox.distributed_pool import (
    DistributedSandboxPool,
    SandboxExecutionError,
    SandboxNode,
)

TENANT = "acme_corp"
NODE_ID = "node-1"


def make_run_id(tenant: str = TENANT) -> str:
    return f"run-{tenant}-{uuid.uuid4()}"


# ---------------------------------------------------------------------------
# Orchestrator telemetry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_success_increments_queue_depth():
    with patch("hfa_tools.services.orchestrator.HFAMetrics") as mock_metrics:
        orchestrator = RunOrchestrator(worker_count=0)
        orchestrator.register("test-agent", AsyncMock())
        orchestrator._running = True  # bypass start()

        request = RunRequest(
            agent_type="test-agent",
            tenant_id=TENANT,
            run_id=make_run_id(),
            payload={},
        )

        run_id = await orchestrator.enqueue(request)

        mock_metrics.inc_queue_depth.assert_called_once_with(TENANT)
        assert run_id is not None


@pytest.mark.asyncio
async def test_queue_full_does_not_increment():
    with patch("hfa_tools.services.orchestrator.HFAMetrics") as mock_metrics:
        orchestrator = RunOrchestrator(worker_count=0, max_queue_size=1)
        orchestrator.register("test-agent", AsyncMock())
        orchestrator._running = True

        await orchestrator.enqueue(
            RunRequest("test-agent", TENANT, make_run_id(), {})
        )
        mock_metrics.inc_queue_depth.reset_mock()

        with pytest.raises(QueueFullError):
            await orchestrator.enqueue(
                RunRequest("test-agent", TENANT, make_run_id(), {})
            )

        mock_metrics.inc_queue_depth.assert_not_called()


@pytest.mark.asyncio
async def test_worker_dequeues_decrements_queue_depth():
    with patch("hfa_tools.services.orchestrator.HFAMetrics") as mock_metrics:
        orchestrator = RunOrchestrator(worker_count=1)
        orchestrator.register("test-agent", AsyncMock())
        await orchestrator.start()

        run_id = make_run_id()
        await orchestrator.enqueue(
            RunRequest("test-agent", TENANT, run_id, {})
        )
        await orchestrator.wait_for(run_id, timeout=1.0)

        mock_metrics.dec_queue_depth.assert_called_once_with(TENANT)
        await orchestrator.close()


@pytest.mark.asyncio
async def test_worker_decrements_even_on_crash():
    async def crash_handler(req, graph):
        raise RuntimeError("Simulated crash")

    with patch("hfa_tools.services.orchestrator.HFAMetrics") as mock_metrics:
        orchestrator = RunOrchestrator(worker_count=1)
        orchestrator.register("test-agent", crash_handler)
        await orchestrator.start()

        run_id = make_run_id()
        await orchestrator.enqueue(
            RunRequest("test-agent", TENANT, run_id, {})
        )
        await orchestrator.wait_for(run_id, timeout=1.0)

        mock_metrics.dec_queue_depth.assert_called_once_with(TENANT)
        await orchestrator.close()


@pytest.mark.asyncio
async def test_sentinel_path_no_decrement():
    with patch("hfa_tools.services.orchestrator.HFAMetrics") as mock_metrics:
        orchestrator = RunOrchestrator(worker_count=1)
        orchestrator.register("test-agent", AsyncMock())
        await orchestrator.start()

        await orchestrator.close()

        mock_metrics.dec_queue_depth.assert_not_called()


@pytest.mark.asyncio
async def test_record_result_emits_run_total():
    with patch("hfa_tools.services.orchestrator.HFAMetrics") as mock_metrics:
        orchestrator = RunOrchestrator(worker_count=1)
        orchestrator.register("test-agent", AsyncMock())
        await orchestrator.start()

        run_id = make_run_id()
        await orchestrator.enqueue(
            RunRequest("test-agent", TENANT, run_id, {})
        )
        await orchestrator.wait_for(run_id, timeout=1.0)

        mock_metrics.inc_runs_total.assert_called_once_with(TENANT, "done")
        await orchestrator.close()


@pytest.mark.asyncio
async def test_record_result_emits_latency_when_available():
    with patch("hfa_tools.services.orchestrator.HFAMetrics") as mock_metrics:
        orchestrator = RunOrchestrator(worker_count=1)
        orchestrator.register("test-agent", AsyncMock())
        await orchestrator.start()

        run_id = make_run_id()
        await orchestrator.enqueue(
            RunRequest("test-agent", TENANT, run_id, {})
        )
        await orchestrator.wait_for(run_id, timeout=1.0)

        mock_metrics.record_run_latency.assert_called_once()
        args = mock_metrics.record_run_latency.call_args[0]
        assert args[0] == TENANT
        assert args[1] > 0
        await orchestrator.close()


@pytest.mark.asyncio
async def test_record_result_does_not_emit_latency_when_duration_none():
    with patch("hfa_tools.services.orchestrator.HFAMetrics") as mock_metrics:
        run_result = RunResult(
            run_id=make_run_id(),
            tenant_id=TENANT,
            agent_type="test-agent",
            status=RunStatus.DONE,
            payload={},
            duration_ms=None,
        )

        orchestrator = RunOrchestrator(worker_count=0)
        await orchestrator._record_result(run_result)

        mock_metrics.record_run_latency.assert_not_called()


# ---------------------------------------------------------------------------
# Distributed pool telemetry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sandbox_success_path_telemetry():
    pool = DistributedSandboxPool()
    node = SandboxNode(node_id=NODE_ID, host="localhost")
    pool._nodes[NODE_ID] = node

    with patch("hfa_tools.sandbox.distributed_pool.HFAMetrics") as mock_metrics:
        with patch.object(pool, "_run_on_node", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = MagicMock()

            await pool.execute("run-1", "python", "print('ok')")

        mock_metrics.inc_sandbox_slots.assert_called_once_with(NODE_ID)
        mock_metrics.dec_sandbox_slots.assert_called_once_with(NODE_ID)


@pytest.mark.asyncio
async def test_sandbox_failure_path_telemetry():
    pool = DistributedSandboxPool()
    node = SandboxNode(node_id=NODE_ID, host="localhost")
    pool._nodes[NODE_ID] = node

    with patch("hfa_tools.sandbox.distributed_pool.HFAMetrics") as mock_metrics:
        with patch.object(pool, "_run_on_node", side_effect=RuntimeError("Docker dead")):
            with pytest.raises(SandboxExecutionError):
                await pool.execute("run-1", "python", "print('ok')")

        mock_metrics.inc_sandbox_slots.assert_called_once_with(NODE_ID)
        mock_metrics.dec_sandbox_slots.assert_called_once_with(NODE_ID)


@pytest.mark.asyncio
async def test_sandbox_timeout_path_telemetry():
    pool = DistributedSandboxPool()
    node = SandboxNode(node_id=NODE_ID, host="localhost")
    pool._nodes[NODE_ID] = node

    async def timeout_simulator(*args, **kwargs):
        await asyncio.sleep(10)
        return MagicMock()

    with patch("hfa_tools.sandbox.distributed_pool.HFAMetrics") as mock_metrics:
        with patch.object(pool, "_run_on_node", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = timeout_simulator

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    pool.execute("run-1", "python", "print('ok')"),
                    timeout=0.1,
                )

        mock_metrics.inc_sandbox_slots.assert_called_once_with(NODE_ID)
        mock_metrics.dec_sandbox_slots.assert_called_once_with(NODE_ID)


# ---------------------------------------------------------------------------
# No-op safety
# ---------------------------------------------------------------------------

def test_metrics_no_op_safe_when_otel_missing():
    with patch("hfa.obs.metrics._OTEL_AVAILABLE", False):
        from hfa.obs.metrics import _Instruments
        _Instruments.reset()

        HFAMetrics.inc_runs_total(TENANT, "done")
        HFAMetrics.record_run_latency(TENANT, 123.4)
        HFAMetrics.inc_queue_depth(TENANT)
        HFAMetrics.dec_queue_depth(TENANT)
        HFAMetrics.inc_sandbox_slots(NODE_ID)
        HFAMetrics.dec_sandbox_slots(NODE_ID)


def test_metrics_logs_on_emit_failure():
    with patch("hfa.obs.metrics._Instruments.get") as mock_get:
        mock_inst = MagicMock()
        mock_inst._ready = True
        mock_inst.runs_total.add.side_effect = Exception("OTel emit error")
        mock_get.return_value = mock_inst

        with patch("hfa.obs.metrics.logger") as mock_logger:
            HFAMetrics.inc_runs_total(TENANT, "done")

            mock_logger.debug.assert_called_once()
            assert "HFAMetrics.inc_runs_total failed" in mock_logger.debug.call_args[0][0]