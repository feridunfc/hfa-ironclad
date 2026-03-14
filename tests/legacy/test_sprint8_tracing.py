"""
hfa-tools/tests/test_sprint8_tracing.py
IRONCLAD Sprint 8 Mini 3 - Tracing Contract Tests

Seven test blocks, matching the roadmap exactly:

  Block 1 - orchestrator success span
  Block 2 - orchestrator failure span
  Block 3 - sandbox execute span (success)
  Block 4 - sandbox execute error span
  Block 5 - inspector snapshot lookup span
  Block 6 - inspector archived stream span
  Block 7 - no-op safety (OTel SDK absent)

All tests use an InMemorySpanExporter so they run fully offline with no
real OTLP endpoint, Jaeger, or Prometheus instance.

Requires:
    opentelemetry-sdk           pip install opentelemetry-sdk
    httpx                       pip install httpx
    fastapi, pytest-asyncio     already in dev deps

IRONCLAD rules (validated by AST in test gate):
    no print() - logging only
    no get_event_loop()
"""

from __future__ import annotations

import asyncio
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# ---------------------------------------------------------------------------
# Tracer / exporter bootstrap for test isolation
# ---------------------------------------------------------------------------

def _make_test_tracer():
    """
    Create an isolated TracerProvider backed by InMemorySpanExporter.

    IMPORTANT:
    - Do NOT call trace.set_tracer_provider(provider) here.
    - Tests patch module-level _tracer directly, so an isolated provider is enough.
    """
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test-hfa")

    return tracer, exporter

def _span_by_name(exporter, name: str):
    spans = exporter.get_finished_spans()
    for span in reversed(spans):
        if span.name == name:
            return span
    return None




def _attrs(span) -> dict:
    return dict(span.attributes or {})


TENANT = "acme_corp"


def _rid() -> str:
    return f"run-{TENANT}-{uuid.uuid4()}"


# ---------------------------------------------------------------------------
# Shared graph helper
# ---------------------------------------------------------------------------

async def _done_graph(run_id: str, tenant: str = TENANT):
    from hfa.obs.run_graph import ExecutionGraph
    g   = ExecutionGraph(run_id, tenant)
    nid = g.add_node("architect")
    await g.start_node(nid)
    await g.commit_node(nid, tokens=100, cost_cents=5)
    return g


# ===========================================================================
# Block 1 â€” Orchestrator success span
# ===========================================================================

class TestOrchestratorSuccessSpan:
    """
    A successful run must produce a span named "hfa.orchestrator.run"
    with the correct attributes and OK status.
    """

    async def test_orchestrator_run_creates_span(self):
        from hfa_tools.services.orchestrator import (
            RunOrchestrator, RunRequest,
        )

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.services.orchestrator._tracer", tracer):
            orch = RunOrchestrator(worker_count=1)

            async def handler(request, graph):
                return MagicMock(total_tokens=50, cost_cents=3)

            orch.register("architect", handler)
            await orch.start()

            rid = _rid()
            req = RunRequest(agent_type="architect", tenant_id=TENANT,
                             run_id=rid, payload={})
            await orch.enqueue(req)
            await asyncio.wait_for(orch.wait_for(rid), timeout=5.0)
            await orch.close()

        span = _span_by_name(exporter, "hfa.orchestrator.run")
        assert span is not None, "hfa.orchestrator.run span must be emitted"

    async def test_orchestrator_success_span_has_required_attributes(self):
        from hfa_tools.services.orchestrator import (
            RunOrchestrator, RunRequest,
        )

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.services.orchestrator._tracer", tracer):
            orch = RunOrchestrator(worker_count=1)

            async def handler(request, graph):
                return MagicMock(total_tokens=50, cost_cents=3)

            orch.register("architect", handler)
            await orch.start()

            rid = _rid()
            req = RunRequest(agent_type="architect", tenant_id=TENANT,
                             run_id=rid, payload={})
            await orch.enqueue(req)
            await asyncio.wait_for(orch.wait_for(rid), timeout=5.0)
            await orch.close()

        span  = _span_by_name(exporter, "hfa.orchestrator.run")
        attrs = _attrs(span)

        assert attrs.get("hfa.run_id")    == rid,    f"hfa.run_id missing/wrong: {attrs}"
        assert attrs.get("hfa.tenant_id") == TENANT, f"hfa.tenant_id missing: {attrs}"
        assert attrs.get("hfa.agent_type") == "architect", f"hfa.agent_type missing: {attrs}"
        assert "hfa.worker_id" in attrs,  f"hfa.worker_id missing: {attrs}"

    async def test_orchestrator_success_span_has_ok_status(self):
        from hfa_tools.services.orchestrator import (
            RunOrchestrator, RunRequest,
        )
        from opentelemetry.trace import StatusCode

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.services.orchestrator._tracer", tracer):
            orch = RunOrchestrator(worker_count=1)

            async def handler(request, graph):
                return MagicMock(total_tokens=10, cost_cents=1)

            orch.register("architect", handler)
            await orch.start()

            rid = _rid()
            req = RunRequest(agent_type="architect", tenant_id=TENANT,
                             run_id=rid, payload={})
            await orch.enqueue(req)
            await asyncio.wait_for(orch.wait_for(rid), timeout=5.0)
            await orch.close()

        span = _span_by_name(exporter, "hfa.orchestrator.run")
        assert span.status.status_code == StatusCode.OK, (
            f"Successful run span must have OK status, got {span.status}"
        )


# ===========================================================================
# Block 2 â€” Orchestrator failure span
# ===========================================================================

class TestOrchestratorFailureSpan:
    """
    A failing run must produce a span with ERROR status and a recorded exception.
    """

    async def test_orchestrator_failure_records_exception(self):
        from hfa_tools.services.orchestrator import (
            RunOrchestrator, RunRequest,
        )
        from opentelemetry.trace import StatusCode

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.services.orchestrator._tracer", tracer):
            orch = RunOrchestrator(worker_count=1)

            async def bad_handler(request, graph):
                raise RuntimeError("handler exploded")

            orch.register("architect", bad_handler)
            await orch.start()

            rid = _rid()
            req = RunRequest(agent_type="architect", tenant_id=TENANT,
                             run_id=rid, payload={})
            await orch.enqueue(req)
            await asyncio.wait_for(orch.wait_for(rid), timeout=5.0)
            await orch.close()

        span = _span_by_name(exporter, "hfa.orchestrator.run")
        assert span is not None

        # Span must be in ERROR status
        assert span.status.status_code == StatusCode.ERROR, (
            f"Failed run span must have ERROR status, got {span.status}"
        )

    async def test_orchestrator_failure_span_has_exception_event(self):
        from hfa_tools.services.orchestrator import (
            RunOrchestrator, RunRequest,
        )

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.services.orchestrator._tracer", tracer):
            orch = RunOrchestrator(worker_count=1)

            async def bad_handler(request, graph):
                raise ValueError("intentional failure")

            orch.register("architect", bad_handler)
            await orch.start()

            rid = _rid()
            req = RunRequest(agent_type="architect", tenant_id=TENANT,
                             run_id=rid, payload={})
            await orch.enqueue(req)
            await asyncio.wait_for(orch.wait_for(rid), timeout=5.0)
            await orch.close()

        span = _span_by_name(exporter, "hfa.orchestrator.run")
        exc_events = [e for e in span.events if e.name == "exception"]
        assert exc_events, (
            "A failed run span must have at least one 'exception' event from record_exception()"
        )

    async def test_orchestrator_failure_span_status_attr(self):
        """hfa.status attribute must reflect the run status."""
        from hfa_tools.services.orchestrator import (
            RunOrchestrator, RunRequest, RunStatus,
        )

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.services.orchestrator._tracer", tracer):
            orch = RunOrchestrator(worker_count=1)

            async def bad_handler(req, graph):
                raise RuntimeError("boom")

            orch.register("architect", bad_handler)
            await orch.start()

            rid = _rid()
            req = RunRequest(agent_type="architect", tenant_id=TENANT,
                             run_id=rid, payload={})
            await orch.enqueue(req)
            await asyncio.wait_for(orch.wait_for(rid), timeout=5.0)
            await orch.close()

        span  = _span_by_name(exporter, "hfa.orchestrator.run")
        attrs = _attrs(span)
        assert attrs.get("hfa.status") == RunStatus.FAILED.value, (
            f"hfa.status must be 'failed' on failure, got {attrs}"
        )


# ===========================================================================
# Block 3 â€” Sandbox execute span (success)
# ===========================================================================

class TestSandboxExecuteSpan:
    """
    Every sandbox execution must produce an "hfa.sandbox.execute" span
    with run_id, language, and node_id attributes.
    """

    async def test_sandbox_execute_creates_span(self):
        from hfa_tools.sandbox.distributed_pool import (
            DistributedSandboxPool, SandboxNode, NodeHealth,
        )

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.sandbox.distributed_pool._tracer", tracer):
            node = SandboxNode(node_id="node-01", host="localhost",
                               health=NodeHealth.HEALTHY, capacity=10)
            pool = DistributedSandboxPool(local_node=node)
            await pool.start()

            rid = _rid()
            result = await pool.execute(rid, "python", "x=1", timeout=5)
            await pool.close()

        span = _span_by_name(exporter, "hfa.sandbox.execute")
        assert span is not None, "hfa.sandbox.execute span must be emitted"

    async def test_sandbox_execute_span_attributes(self):
        from hfa_tools.sandbox.distributed_pool import (
            DistributedSandboxPool, SandboxNode, NodeHealth,
        )

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.sandbox.distributed_pool._tracer", tracer):
            node = SandboxNode(node_id="node-02", host="localhost",
                               health=NodeHealth.HEALTHY, capacity=10)
            pool = DistributedSandboxPool(local_node=node)
            await pool.start()

            rid = _rid()
            await pool.execute(rid, "python", "y=2", timeout=5)
            await pool.close()

        span  = _span_by_name(exporter, "hfa.sandbox.execute")
        attrs = _attrs(span)

        assert attrs.get("hfa.run_id")   == rid,       f"hfa.run_id wrong: {attrs}"
        assert attrs.get("hfa.language") == "python",  f"hfa.language wrong: {attrs}"
        assert attrs.get("hfa.node_id")  == "node-02", f"hfa.node_id wrong: {attrs}"

    async def test_sandbox_execute_success_span_has_ok_status(self):
        from hfa_tools.sandbox.distributed_pool import (
            DistributedSandboxPool, SandboxNode, NodeHealth,
        )
        from opentelemetry.trace import StatusCode

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.sandbox.distributed_pool._tracer", tracer):
            node = SandboxNode(node_id="node-03", host="localhost",
                               health=NodeHealth.HEALTHY, capacity=10)
            pool = DistributedSandboxPool(local_node=node)
            await pool.start()
            rid = _rid()
            await pool.execute(rid, "python", "z=3", timeout=5)
            await pool.close()

        span = _span_by_name(exporter, "hfa.sandbox.execute")
        assert span.status.status_code == StatusCode.OK


# ===========================================================================
# Block 4 â€” Sandbox execute error trace
# ===========================================================================

class TestSandboxExecuteErrorSpan:
    """
    A failing sandbox execution must produce a span with ERROR status
    and a recorded exception event.
    """

    async def test_sandbox_execute_error_records_exception(self):
        from hfa_tools.sandbox.distributed_pool import (
            DistributedSandboxPool, SandboxNode, NodeHealth,
            SandboxExecutionError,
        )
        from opentelemetry.trace import StatusCode

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.sandbox.distributed_pool._tracer", tracer):
            node = SandboxNode(node_id="node-fail", host="localhost",
                               health=NodeHealth.HEALTHY, capacity=10)
            pool = DistributedSandboxPool(local_node=node)
            await pool.start()

            # Patch _run_on_node to raise
            with patch.object(pool, "_run_on_node",
                               side_effect=RuntimeError("container OOM")):
                with pytest.raises(SandboxExecutionError):
                    await pool.execute(_rid(), "python", "boom()", timeout=5)

            await pool.close()

        span = _span_by_name(exporter, "hfa.sandbox.execute")
        assert span is not None

        assert span.status.status_code == StatusCode.ERROR, (
            f"Sandbox error must set span to ERROR, got {span.status}"
        )

    async def test_sandbox_execute_error_has_exception_event(self):
        from hfa_tools.sandbox.distributed_pool import (
            DistributedSandboxPool, SandboxNode, NodeHealth,
            SandboxExecutionError,
        )

        tracer, exporter = _make_test_tracer()

        with patch("hfa_tools.sandbox.distributed_pool._tracer", tracer):
            node = SandboxNode(node_id="node-fail2", host="localhost",
                               health=NodeHealth.HEALTHY, capacity=10)
            pool = DistributedSandboxPool(local_node=node)
            await pool.start()

            with patch.object(pool, "_run_on_node",
                               side_effect=RuntimeError("disk full")):
                with pytest.raises(SandboxExecutionError):
                    await pool.execute(_rid(), "node", "crash()", timeout=5)

            await pool.close()

        span       = _span_by_name(exporter, "hfa.sandbox.execute")
        exc_events = [e for e in span.events if e.name == "exception"]
        assert exc_events, "Sandbox failure must record an exception event"

    async def test_sandbox_execute_error_slot_still_released(self):
        """
        Even on error, slot metrics must be balanced (not tested via OTel here â€”
        this test verifies node.active returns to 0 so no slot leak).
        """
        from hfa_tools.sandbox.distributed_pool import (
            DistributedSandboxPool, SandboxNode, NodeHealth,
            SandboxExecutionError,
        )

        tracer, _ = _make_test_tracer()

        with patch("hfa_tools.sandbox.distributed_pool._tracer", tracer):
            node = SandboxNode(node_id="node-slot", host="localhost",
                               health=NodeHealth.HEALTHY, capacity=10)
            pool = DistributedSandboxPool(local_node=node)
            await pool.start()

            with patch.object(pool, "_run_on_node",
                               side_effect=RuntimeError("oom")):
                with pytest.raises(SandboxExecutionError):
                    await pool.execute(_rid(), "python", "x", timeout=5)

            await pool.close()

        assert node.active == 0, "Slot must be released (node.active == 0) even on error"


# ===========================================================================
# Block 5 â€” Inspector snapshot lookup span
# ===========================================================================

class TestInspectorSnapshotLookupSpan:
    """
    REST endpoints (get_run_graph, get_run_summary, list_tenant_runs)
    must emit an "hfa.inspector.snapshot_lookup" span.
    """

    def _make_app(self, registry):
        from fastapi import FastAPI
        from hfa_tools.api.inspector import router
        app = FastAPI()
        app.include_router(router)
        app.state.run_registry = registry
        return app

    async def _live_registry(self) -> tuple:
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        registry = RunRegistry()
        rid      = _rid()
        await registry.register(ExecutionGraph(rid, TENANT))
        return registry, rid

    async def test_get_run_graph_creates_snapshot_span(self):
        from httpx import AsyncClient, ASGITransport

        tracer, exporter = _make_test_tracer()
        registry, rid    = await self._live_registry()

        with patch("hfa_tools.api.inspector._tracer", tracer):
            app = self._make_app(registry)
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as c:
                resp = await c.get(
                    f"/v1/inspector/runs/{rid}",
                    headers={"X-Tenant-ID": TENANT},
                )
            assert resp.status_code == 200

        span = _span_by_name(exporter, "hfa.inspector.snapshot_lookup")
        assert span is not None, "get_run_graph must emit snapshot_lookup span"

    async def test_get_run_summary_creates_snapshot_span(self):
        from httpx import AsyncClient, ASGITransport

        tracer, exporter = _make_test_tracer()
        registry, rid    = await self._live_registry()

        with patch("hfa_tools.api.inspector._tracer", tracer):
            app = self._make_app(registry)
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as c:
                resp = await c.get(
                    f"/v1/inspector/runs/{rid}/summary",
                    headers={"X-Tenant-ID": TENANT},
                )
            assert resp.status_code == 200

        span = _span_by_name(exporter, "hfa.inspector.snapshot_lookup")
        assert span is not None

    async def test_snapshot_span_has_run_id_and_tenant_attrs(self):
        from httpx import AsyncClient, ASGITransport

        tracer, exporter = _make_test_tracer()
        registry, rid    = await self._live_registry()

        with patch("hfa_tools.api.inspector._tracer", tracer):
            app = self._make_app(registry)
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as c:
                await c.get(
                    f"/v1/inspector/runs/{rid}",
                    headers={"X-Tenant-ID": TENANT},
                )

        span  = _span_by_name(exporter, "hfa.inspector.snapshot_lookup")
        attrs = _attrs(span)
        assert attrs.get("hfa.run_id")    == rid,    f"hfa.run_id wrong: {attrs}"
        assert attrs.get("hfa.tenant_id") == TENANT, f"hfa.tenant_id wrong: {attrs}"

    async def test_list_tenant_runs_creates_snapshot_span(self):
        from httpx import AsyncClient, ASGITransport

        tracer, exporter = _make_test_tracer()
        registry, _      = await self._live_registry()

        with patch("hfa_tools.api.inspector._tracer", tracer):
            app = self._make_app(registry)
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as c:
                resp = await c.get(
                    f"/v1/inspector/tenants/{TENANT}/runs",
                    headers={"X-Tenant-ID": TENANT},
                )
            assert resp.status_code == 200

        span = _span_by_name(exporter, "hfa.inspector.snapshot_lookup")
        assert span is not None

    async def test_snapshot_span_ok_status_on_success(self):
        from httpx import AsyncClient, ASGITransport
        from opentelemetry.trace import StatusCode

        tracer, exporter = _make_test_tracer()
        registry, rid    = await self._live_registry()

        with patch("hfa_tools.api.inspector._tracer", tracer):
            app = self._make_app(registry)
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as c:
                await c.get(
                    f"/v1/inspector/runs/{rid}",
                    headers={"X-Tenant-ID": TENANT},
                )

        span = _span_by_name(exporter, "hfa.inspector.snapshot_lookup")
        assert span.status.status_code == StatusCode.OK


# ===========================================================================
# Block 6 â€” Inspector archived stream span
# ===========================================================================

class TestInspectorArchivedStreamSpan:
    """
    Calling the SSE endpoint must emit an "hfa.inspector.stream_open" span.
    Archived fallback and tenant mismatch must also be handled.
    """

    def _make_app(self, registry):
        from fastapi import FastAPI
        from hfa_tools.api.inspector import router
        app = FastAPI()
        app.include_router(router)
        app.state.run_registry = registry
        return app

    async def test_stream_open_span_emitted_for_live_run(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph
        from httpx import AsyncClient, ASGITransport

        tracer, exporter = _make_test_tracer()
        registry = RunRegistry()
        rid      = _rid()
        await registry.register(ExecutionGraph(rid, TENANT))

        with patch("hfa_tools.api.inspector._tracer", tracer):
            app = self._make_app(registry)
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as c:
                # GET events endpoint â€” stream will return quickly since run not running
                resp = await c.get(
                    f"/v1/inspector/runs/{rid}/events",
                    headers={"X-Tenant-ID": TENANT},
                    params={"interval_ms": 100},
                )
            assert resp.status_code == 200

        span = _span_by_name(exporter, "hfa.inspector.stream_open")
        assert span is not None, "stream_open span must be emitted for live run"

    async def test_stream_open_span_archived_attr_false_for_live_run(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph
        from httpx import AsyncClient, ASGITransport

        tracer, exporter = _make_test_tracer()
        registry = RunRegistry()
        rid      = _rid()
        await registry.register(ExecutionGraph(rid, TENANT))

        with patch("hfa_tools.api.inspector._tracer", tracer):
            app = self._make_app(registry)
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as c:
                await c.get(
                    f"/v1/inspector/runs/{rid}/events",
                    headers={"X-Tenant-ID": TENANT},
                    params={"interval_ms": 100},
                )

        span  = _span_by_name(exporter, "hfa.inspector.stream_open")
        attrs = _attrs(span)
        assert attrs.get("hfa.archived") == "false", (
            f"hfa.archived must be 'false' for live run, got: {attrs}"
        )

    async def test_stream_open_span_archived_attr_true_for_archived_run(self):
        """Archived run: live_graph=None â†’ hfa.archived=true in span."""
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport
        import json

        tracer, exporter = _make_test_tracer()
        redis_mock       = AsyncMock()
        rid              = _rid()
        snap_payload     = json.dumps({
            "run_id": rid, "tenant_id": TENANT, "nodes": [], "edges": {},
            "summary": {
                "run_id": rid, "tenant_id": TENANT, "created_at": 1.0,
                "total_nodes": 0, "done": 0, "failed": 0,
                "running": 0, "pending": 0,
                "total_tokens": 0, "total_cost_cents": 0,
                "is_complete": True, "has_failures": False,
            },
        })
        redis_mock.get = AsyncMock(return_value=snap_payload.encode())

        registry = RunRegistry(redis_client=redis_mock)
        # run_id not in RAM â†’ archived path

        with patch("hfa_tools.api.inspector._tracer", tracer):
            app = self._make_app(registry)
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as c:
                resp = await c.get(
                    f"/v1/inspector/runs/{rid}/events",
                    headers={"X-Tenant-ID": TENANT},
                    params={"interval_ms": 100},
                )
            assert resp.status_code == 200

        span  = _span_by_name(exporter, "hfa.inspector.stream_open")
        attrs = _attrs(span)
        assert attrs.get("hfa.archived") == "true", (
            f"hfa.archived must be 'true' for archived run, got: {attrs}"
        )

    async def test_stream_open_tenant_mismatch_records_error(self):
        """
        Tenant mismatch on SSE must:
          * Raise 403
          * Record exception event on span
          * Span status ERROR
        """
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph
        from httpx import AsyncClient, ASGITransport
        from opentelemetry.trace import StatusCode

        tracer, exporter = _make_test_tracer()
        registry         = RunRegistry()
        rid              = _rid()
        await registry.register(ExecutionGraph(rid, TENANT))

        with patch("hfa_tools.api.inspector._tracer", tracer):
            app = self._make_app(registry)
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as c:
                resp = await c.get(
                    f"/v1/inspector/runs/{rid}/events",
                    headers={"X-Tenant-ID": "other_tenant"},   # mismatch
                    params={"interval_ms": 100},
                )
            assert resp.status_code == 403

        span = _span_by_name(exporter, "hfa.inspector.stream_open")
        assert span is not None
        assert span.status.status_code == StatusCode.ERROR, (
            "Tenant mismatch must result in ERROR span status"
        )

    async def test_stream_open_span_has_run_id_and_tenant(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph
        from httpx import AsyncClient, ASGITransport

        tracer, exporter = _make_test_tracer()
        registry         = RunRegistry()
        rid              = _rid()
        await registry.register(ExecutionGraph(rid, TENANT))

        with patch("hfa_tools.api.inspector._tracer", tracer):
            app = self._make_app(registry)
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as c:
                await c.get(
                    f"/v1/inspector/runs/{rid}/events",
                    headers={"X-Tenant-ID": TENANT},
                    params={"interval_ms": 100},
                )

        span  = _span_by_name(exporter, "hfa.inspector.stream_open")
        attrs = _attrs(span)
        assert attrs.get("hfa.run_id")    == rid,    f"hfa.run_id missing: {attrs}"
        assert attrs.get("hfa.tenant_id") == TENANT, f"hfa.tenant_id missing: {attrs}"


# ===========================================================================
# Block 7 â€” No-op safety (OTel SDK absent)
# ===========================================================================

class TestTracingNoOpSafety:
    """
    When the OTel SDK is not installed, or when no TracerProvider is
    configured, tracing helpers must be completely silent.
    Business logic must not be affected at all.
    """

    def test_get_tracer_returns_noop_when_sdk_absent(self):
        """Simulates an environment where opentelemetry is not installed."""
        import sys

        # Temporarily hide opentelemetry
        saved = {k: v for k, v in sys.modules.items() if k.startswith("opentelemetry")}
        for k in saved:
            sys.modules[k] = None  # type: ignore

        try:
            # Force re-import with broken SDK
            import importlib
            import hfa.obs.tracing as tracing_mod
            importlib.reload(tracing_mod)
            tracer = tracing_mod.get_tracer("test")
            # Must not raise, must be some no-op tracer
            with tracer.start_as_current_span("test.span") as span:
                assert span is not None
        finally:
            # Restore modules
            for k in saved:
                sys.modules[k] = saved[k]

    def test_hfatracing_set_attrs_none_span_does_not_crash(self):
        from hfa.obs.tracing import HFATracing
        # None span â€” must be completely silent
        HFATracing.set_attrs(None, {"hfa.run_id": "x", "hfa.tenant_id": "y"})

    def test_hfatracing_record_exc_none_span_does_not_crash(self):
        from hfa.obs.tracing import HFATracing
        HFATracing.record_exc(None, RuntimeError("test error"))

    def test_hfatracing_span_ok_none_span_does_not_crash(self):
        from hfa.obs.tracing import HFATracing
        HFATracing.span_ok(None)

    def test_noop_span_absorbs_set_attribute(self):
        from hfa.obs.tracing import _NoOpSpan
        span = _NoOpSpan()
        span.set_attribute("hfa.run_id", "test-run")   # must not raise

    def test_noop_span_absorbs_record_exception(self):
        from hfa.obs.tracing import _NoOpSpan
        span = _NoOpSpan()
        span.record_exception(RuntimeError("crash"))   # must not raise

    def test_noop_span_does_not_suppress_exceptions(self):
        """_NoOpSpan.__exit__ must return False â€” original exceptions propagate."""
        from hfa.obs.tracing import _NoOpSpan
        span = _NoOpSpan()
        assert span.__exit__(RuntimeError, RuntimeError("x"), None) is False

    def test_noop_tracer_context_manager_propagates_exceptions(self):
        """Exceptions raised inside a no-op span context must not be swallowed."""
        from hfa.obs.tracing import _NoOpTracer

        tracer = _NoOpTracer()
        with pytest.raises(ValueError):
            with tracer.start_as_current_span("test") as span:
                raise ValueError("must propagate")

    async def test_orchestrator_works_without_real_tracer(self):
        """
        Orchestrator must complete a run successfully even when _tracer is
        replaced with a no-op tracer (simulates SDK-absent environment).
        """
        from hfa.obs.tracing import _NoOpTracer
        from hfa_tools.services.orchestrator import (
            RunOrchestrator, RunRequest, RunStatus,
        )

        with patch("hfa_tools.services.orchestrator._tracer", _NoOpTracer()):
            orch = RunOrchestrator(worker_count=1)

            async def handler(req, graph):
                return MagicMock(total_tokens=10, cost_cents=1)

            orch.register("architect", handler)
            await orch.start()

            rid = _rid()
            req = RunRequest(agent_type="architect", tenant_id=TENANT,
                             run_id=rid, payload={})
            await orch.enqueue(req)
            result = await asyncio.wait_for(orch.wait_for(rid), timeout=5.0)
            await orch.close()

        assert result.status == RunStatus.DONE, (
            "Run must succeed even with no-op tracer"
        )

    async def test_sandbox_works_without_real_tracer(self):
        from hfa.obs.tracing import _NoOpTracer
        from hfa_tools.sandbox.distributed_pool import (
            DistributedSandboxPool, SandboxNode, NodeHealth,
        )

        with patch("hfa_tools.sandbox.distributed_pool._tracer", _NoOpTracer()):
            node = SandboxNode(node_id="noop-node", host="localhost",
                               health=NodeHealth.HEALTHY, capacity=10)
            pool = DistributedSandboxPool(local_node=node)
            await pool.start()
            result = await pool.execute(_rid(), "python", "pass", timeout=5)
            await pool.close()

        assert result is not None


