"""
hfa-tools/tests/test_sprint5.py
IRONCLAD Sprint 5 — Production Gate Extended Test Suite
"""
from __future__ import annotations

import asyncio
import uuid
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock

VALID_TENANT = "acme_corp"
def _make_valid_run_id(): return f"run-{VALID_TENANT}-{uuid.uuid4()}"

@pytest.mark.asyncio
class TestProductionOrchestrator:
    @pytest_asyncio.fixture
    async def orch(self):
        from hfa_tools.services.orchestrator import RunOrchestrator
        o = RunOrchestrator(max_queue_size=50, worker_count=2)
        await o.start()
        yield o
        await o.close()

    async def test_budget_rejection_logic(self):
        """Verify run is REJECTED when budget is exhausted."""
        from hfa_tools.services.orchestrator import RunOrchestrator, RunRequest, RunStatus

        mock_guard = AsyncMock()
        mock_guard.get_state = AsyncMock(return_value=MagicMock(status=MagicMock(value="exhausted")))

        o = RunOrchestrator(worker_count=1, budget_guard=mock_guard)
        await o.start()
        o.register("architect", AsyncMock())

        rid = _make_valid_run_id()
        await o.enqueue(RunRequest("architect", VALID_TENANT, rid, {}))
        res = await o.wait_for(rid)

        assert res.status == RunStatus.REJECTED
        await o.close()

    async def test_ledger_append_tracked(self):
        """Verify ledger append is called and background task is tracked."""
        from hfa_tools.services.orchestrator import RunOrchestrator, RunRequest

        mock_ledger = AsyncMock()
        o = RunOrchestrator(worker_count=1, ledger=mock_ledger)
        await o.start()
        o.register("architect", AsyncMock(return_value=MagicMock()))

        rid = _make_valid_run_id()
        await o.enqueue(RunRequest("architect", VALID_TENANT, rid, {}))
        await o.wait_for(rid)

        # Check background task drain logic implicitly through close()
        await o.close()
        mock_ledger.append.assert_called()

    async def test_tenant_concurrency_semaphore(self, orch):
        """Verify per-tenant semaphore enforces limits."""
        from hfa_tools.services.orchestrator import RunRequest

        # Set concurrency limit to 1
        orch._per_tenant_limit = 1
        active_count = 0
        max_observed = 0

        async def handler(req, graph):
            nonlocal active_count, max_observed
            active_count += 1
            max_observed = max(max_observed, active_count)
            await asyncio.sleep(0.1)
            active_count -= 1
            return MagicMock()

        orch.register("architect", handler)
        r1 = _make_valid_run_id()
        r2 = _make_valid_run_id()

        await orch.enqueue(RunRequest("architect", VALID_TENANT, r1, {}))
        await orch.enqueue(RunRequest("architect", VALID_TENANT, r2, {}))

        await asyncio.gather(orch.wait_for(r1), orch.wait_for(r2))
        assert max_observed == 1 # Even with 2 workers, only 1 allowed for this tenant

    async def test_memory_leak_prevention(self, orch):
        """Verify results are consumed and removed from memory."""
        from hfa_tools.services.orchestrator import RunRequest
        orch.register("architect", AsyncMock(return_value=MagicMock()))

        rid = _make_valid_run_id()
        await orch.enqueue(RunRequest("architect", VALID_TENANT, rid, {}))
        await orch.wait_for(rid, consume=True)

        assert rid not in orch._results
        assert rid not in orch._result_events