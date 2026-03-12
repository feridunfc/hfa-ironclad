"""
hfa-tools/tests/test_sprint6.py
IRONCLAD Sprint 6 — Test suite

Coverage:
  * HFAEvent         — serialization, from_json, EventType constants
  * InMemoryEventBus — publish, subscribe, unsubscribe, fan-out, tenant filter,
                       delivery error isolation, wildcard, close
  * DistributedSandboxPool — node register/deregister, selection, health monitor,
                              execute routing, cluster error, slot management
  * NodeSelector     — weighted round-robin, health filter, deterministic tie-break
  * NodeHealthMonitor— healthy/degraded/unhealthy transitions
  * RunRegistry      — register, get, list_by_tenant, cleanup
  * Inspector API    — graph endpoint, summary, nodes filter, tenant isolation,
                       403/404 guards, SSE stream
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ===========================================================================
# HFAEvent
# ===========================================================================

class TestHFAEvent:
    def test_event_auto_fields(self):
        from hfa.events.bus import HFAEvent
        e = HFAEvent("hfa.run.started", "acme", "run-01", {"plan_id": "p1"})
        assert e.event_id is not None
        assert len(e.event_id) == 36   # UUID4
        assert e.timestamp > 1_000_000
        assert e.schema_ver == 1

    def test_to_dict_round_trip(self):
        from hfa.events.bus import HFAEvent
        e = HFAEvent("hfa.run.done", "acme", "run-02", {"tokens": 500})
        d = e.to_dict()
        assert d["event_type"] == "hfa.run.done"
        assert d["payload"]["tokens"] == 500

    def test_to_json_from_json_round_trip(self):
        from hfa.events.bus import HFAEvent
        e = HFAEvent("hfa.node.done", "acme", "run-03", {"cost_cents": 7})
        raw = e.to_json()
        e2  = HFAEvent.from_json(raw)
        assert e2.event_type  == e.event_type
        assert e2.tenant_id   == e.tenant_id
        assert e2.run_id      == e.run_id
        assert e2.payload     == e.payload
        assert e2.event_id    == e.event_id

    def test_from_json_bytes(self):
        from hfa.events.bus import HFAEvent
        e   = HFAEvent("hfa.run.failed", "acme", "run-04", {})
        e2  = HFAEvent.from_json(e.to_json().encode())
        assert e2.event_type == "hfa.run.failed"

    def test_event_type_constants(self):
        from hfa.events.bus import EventType
        assert EventType.RUN_STARTED     == "hfa.run.started"
        assert EventType.NODE_DONE       == "hfa.node.done"
        assert EventType.COMPLIANCE_DENY == "hfa.compliance.denied"
        assert EventType.HEALING_RETRY   == "hfa.healing.retry"
        assert EventType.BUDGET_DEPLETED == "hfa.budget.depleted"

    def test_cost_cents_in_payload_is_int(self):
        from hfa.events.bus import HFAEvent
        e = HFAEvent("hfa.run.done", "acme", "run-05", {"cost_cents": 42})
        assert isinstance(e.payload["cost_cents"], int)


# ===========================================================================
# InMemoryEventBus
# ===========================================================================

class TestInMemoryEventBus:
    def _make_event(self, tenant="acme", event_type="hfa.run.started") -> "HFAEvent":
        from hfa.events.bus import HFAEvent
        return HFAEvent(event_type, tenant, f"run-{uuid.uuid4().hex[:8]}", {})

    async def test_publish_delivers_to_subscriber(self):
        from hfa.events.bus import InMemoryEventBus, EventType
        bus      = InMemoryEventBus()
        received = []
        async def handler(e): received.append(e)
        await bus.subscribe(EventType.RUN_STARTED, handler)
        evt = self._make_event(event_type=EventType.RUN_STARTED)
        await bus.publish(evt)
        assert len(received) == 1
        assert received[0].event_id == evt.event_id
        await bus.close()

    async def test_wildcard_subscriber_receives_all(self):
        from hfa.events.bus import InMemoryEventBus
        bus = InMemoryEventBus()
        received = []
        async def handler(e): received.append(e)
        await bus.subscribe("*", handler)
        await bus.publish(self._make_event(event_type="hfa.run.started"))
        await bus.publish(self._make_event(event_type="hfa.node.done"))
        assert len(received) == 2
        await bus.close()

    async def test_tenant_filter_isolates_events(self):
        from hfa.events.bus import InMemoryEventBus, HFAEvent
        bus = InMemoryEventBus()
        acme_received = []
        beta_received = []
        async def acme_handler(e): acme_received.append(e)
        async def beta_handler(e): beta_received.append(e)
        await bus.subscribe("*", acme_handler, tenant_id="acme")
        await bus.subscribe("*", beta_handler, tenant_id="beta_corp")

        await bus.publish(HFAEvent("hfa.run.started", "acme", "r1", {}))
        await bus.publish(HFAEvent("hfa.run.started", "beta_corp", "r2", {}))

        assert len(acme_received) == 1
        assert len(beta_received) == 1
        assert acme_received[0].tenant_id == "acme"
        await bus.close()

    async def test_unsubscribe_stops_delivery(self):
        from hfa.events.bus import InMemoryEventBus
        bus = InMemoryEventBus()
        received = []
        async def handler(e): received.append(e)
        sub_id = await bus.subscribe("*", handler)
        await bus.publish(self._make_event())
        await bus.unsubscribe(sub_id)
        await bus.publish(self._make_event())
        assert len(received) == 1
        await bus.close()

    async def test_fan_out_to_multiple_subscribers(self):
        from hfa.events.bus import InMemoryEventBus
        bus = InMemoryEventBus()
        counts = [0, 0, 0]
        async def h1(e): counts[0] += 1
        async def h2(e): counts[1] += 1
        async def h3(e): counts[2] += 1
        await bus.subscribe("*", h1)
        await bus.subscribe("*", h2)
        await bus.subscribe("*", h3)
        await bus.publish(self._make_event())
        assert counts == [1, 1, 1]
        await bus.close()

    async def test_delivery_error_does_not_propagate(self):
        """Subscriber crash must NOT crash the bus."""
        from hfa.events.bus import InMemoryEventBus
        bus = InMemoryEventBus()
        async def crashing_handler(e):
            raise RuntimeError("handler crashed")
        await bus.subscribe("*", crashing_handler)
        # Must not raise
        await bus.publish(self._make_event())
        await bus.close()

    async def test_delivery_timeout_does_not_propagate(self):
        """Slow subscriber must NOT block bus or propagate TimeoutError."""
        from hfa.events.bus import InMemoryEventBus
        bus = InMemoryEventBus(delivery_timeout=0.05)
        async def slow_handler(e):
            await asyncio.sleep(999)
        await bus.subscribe("*", slow_handler)
        await bus.publish(self._make_event())  # must return quickly
        await bus.close()

    async def test_published_events_inspection(self):
        from hfa.events.bus import InMemoryEventBus, EventType, HFAEvent
        bus = InMemoryEventBus()
        await bus.publish(HFAEvent(EventType.RUN_STARTED, "acme", "r1", {}))
        await bus.publish(HFAEvent(EventType.NODE_DONE,   "acme", "r1", {}))
        all_events = bus.published_events()
        assert len(all_events) == 2
        run_events = bus.published_events(EventType.RUN_STARTED)
        assert len(run_events) == 1
        await bus.close()

    async def test_clear_events(self):
        from hfa.events.bus import InMemoryEventBus
        bus = InMemoryEventBus()
        await bus.publish(self._make_event())
        bus.clear_events()
        assert bus.published_events() == []
        await bus.close()

    async def test_no_delivery_to_different_event_type(self):
        from hfa.events.bus import InMemoryEventBus, HFAEvent
        bus = InMemoryEventBus()
        received = []
        async def handler(e): received.append(e)
        await bus.subscribe("hfa.run.started", handler)
        await bus.publish(HFAEvent("hfa.node.done", "acme", "r1", {}))
        assert len(received) == 0
        await bus.close()


# ===========================================================================
# NodeSelector
# ===========================================================================

class TestNodeSelector:
    def _make_node(self, node_id, health="healthy", active=0, capacity=10, langs=None, weight=10):
        from hfa_tools.sandbox.distributed_pool import SandboxNode, NodeHealth
        return SandboxNode(
            node_id   = node_id,
            host      = "localhost",
            capacity  = capacity,
            languages = langs or ["python", "node"],
            health    = NodeHealth(health),
            active    = active,
            weight    = weight,
        )

    def test_selects_healthy_node(self):
        from hfa_tools.sandbox.distributed_pool import NodeSelector
        sel   = NodeSelector()
        node  = self._make_node("n1")
        result= sel.select([node], "python")
        assert result.node_id == "n1"

    def test_excludes_unhealthy_nodes(self):
        from hfa_tools.sandbox.distributed_pool import NodeSelector, SandboxClusterError
        sel   = NodeSelector()
        node  = self._make_node("n1", health="unhealthy")
        with pytest.raises(SandboxClusterError):
            sel.select([node], "python")

    def test_excludes_full_nodes(self):
        from hfa_tools.sandbox.distributed_pool import NodeSelector, SandboxClusterError
        sel  = NodeSelector()
        node = self._make_node("n1", active=10, capacity=10)  # full
        with pytest.raises(SandboxClusterError):
            sel.select([node], "python")

    def test_excludes_wrong_language(self):
        from hfa_tools.sandbox.distributed_pool import NodeSelector, SandboxClusterError
        sel  = NodeSelector()
        node = self._make_node("n1", langs=["python"])
        with pytest.raises(SandboxClusterError):
            sel.select([node], "node")

    def test_prefers_lower_load(self):
        from hfa_tools.sandbox.distributed_pool import NodeSelector
        sel  = NodeSelector()
        low  = self._make_node("low",  active=1, capacity=10)   # 10% load
        high = self._make_node("high", active=8, capacity=10)   # 80% load
        result = sel.select([high, low], "python")
        assert result.node_id == "low"

    def test_empty_nodes_raises(self):
        from hfa_tools.sandbox.distributed_pool import NodeSelector, SandboxClusterError
        sel = NodeSelector()
        with pytest.raises(SandboxClusterError):
            sel.select([], "python")

    def test_deterministic_tiebreak_by_node_id(self):
        from hfa_tools.sandbox.distributed_pool import NodeSelector
        sel = NodeSelector()
        a   = self._make_node("node-a", active=0)
        b   = self._make_node("node-b", active=0)
        result = sel.select([b, a], "python")
        assert result.node_id == "node-a"


# ===========================================================================
# NodeHealthMonitor
# ===========================================================================

class TestNodeHealthMonitor:
    def _make_node(self, node_id, last_seen_offset=0):
        from hfa_tools.sandbox.distributed_pool import SandboxNode
        node = SandboxNode(node_id=node_id, host="localhost")
        node.last_seen = time.time() + last_seen_offset
        return node

    async def test_healthy_node_stays_healthy(self):
        from hfa_tools.sandbox.distributed_pool import NodeHealthMonitor, NodeHealth
        monitor = NodeHealthMonitor(stale_threshold=60, check_interval=0.05)
        node = self._make_node("n1", last_seen_offset=0)  # just seen
        nodes = {"n1": node}
        monitor.start(nodes)
        await asyncio.sleep(0.1)
        assert node.health == NodeHealth.HEALTHY
        await monitor.stop()

    async def test_stale_node_marked_unhealthy(self):
        from hfa_tools.sandbox.distributed_pool import NodeHealthMonitor, NodeHealth
        monitor = NodeHealthMonitor(stale_threshold=5, check_interval=0.05)
        node = self._make_node("n1", last_seen_offset=-10)  # 10s ago
        nodes = {"n1": node}
        monitor.start(nodes)
        await asyncio.sleep(0.15)
        assert node.health == NodeHealth.UNHEALTHY
        await monitor.stop()

    async def test_degraded_threshold(self):
        from hfa_tools.sandbox.distributed_pool import NodeHealthMonitor, NodeHealth
        monitor = NodeHealthMonitor(stale_threshold=10, check_interval=0.05)
        node = self._make_node("n1", last_seen_offset=-6)  # > threshold/2
        nodes = {"n1": node}
        monitor.start(nodes)
        await asyncio.sleep(0.15)
        assert node.health in (NodeHealth.DEGRADED, NodeHealth.UNHEALTHY)
        await monitor.stop()


# ===========================================================================
# DistributedSandboxPool
# ===========================================================================

class TestDistributedSandboxPool:
    def _make_pool(self, nodes=None):
        from hfa_tools.sandbox.distributed_pool import DistributedSandboxPool
        pool = DistributedSandboxPool(redis_client=None)
        if nodes:
            for n in nodes:
                pool._nodes[n.node_id] = n
        return pool

    def _make_node(self, node_id="n1", capacity=5, active=0):
        from hfa_tools.sandbox.distributed_pool import SandboxNode
        return SandboxNode(node_id=node_id, host="localhost", capacity=capacity, active=active)

    async def test_register_node(self):
        pool = self._make_pool()
        node = self._make_node()
        await pool.register_node(node)
        assert "n1" in pool._nodes

    async def test_deregister_node(self):
        from hfa_tools.sandbox.distributed_pool import DistributedSandboxPool
        pool = self._make_pool([self._make_node()])
        await pool.deregister_node("n1")
        assert "n1" not in pool._nodes

    def test_list_nodes(self):
        node = self._make_node()
        pool = self._make_pool([node])
        assert len(pool.list_nodes()) == 1

    def test_healthy_node_count(self):
        from hfa_tools.sandbox.distributed_pool import SandboxNode, NodeHealth
        n1 = self._make_node("n1")
        n2 = self._make_node("n2")
        n2.health = NodeHealth.UNHEALTHY
        pool = self._make_pool([n1, n2])
        assert pool.healthy_node_count() == 1

    async def test_execute_increments_and_decrements_active(self):
        node = self._make_node(capacity=5)
        pool = self._make_pool([node])

        async def mock_run(*args, **kwargs):
            from hfa_tools.sandbox.distributed_pool import SandboxResult
            return SandboxResult(
                run_id="r1", node_id="n1", language="python",
                stdout="ok", stderr="", exit_code=0, duration_ms=50
            )

        with patch.object(pool, "_run_on_node", side_effect=mock_run):
            result = await pool.execute("r1", "python", "print('hi')", timeout=5)

        assert result.success
        assert result.node_id == "n1"
        assert node.active == 0  # released after execution

    async def test_execute_releases_slot_on_failure(self):
        node = self._make_node(capacity=5)
        pool = self._make_pool([node])

        with patch.object(pool, "_run_on_node", side_effect=RuntimeError("docker down")):
            from hfa_tools.sandbox.distributed_pool import SandboxExecutionError
            with pytest.raises(SandboxExecutionError):
                await pool.execute("r2", "python", "print('hi')", timeout=5)

        assert node.active == 0  # always released

    async def test_cluster_error_when_no_nodes(self):
        from hfa_tools.sandbox.distributed_pool import SandboxClusterError
        pool = self._make_pool()
        with pytest.raises(SandboxClusterError):
            await pool.execute("r3", "python", "pass", timeout=5)

    async def test_close_stops_monitor(self):
        pool = self._make_pool()
        await pool.start()
        await pool.close()
        assert pool._monitor._task is None or pool._monitor._task.done()

    async def test_sandbox_result_success_property(self):
        from hfa_tools.sandbox.distributed_pool import SandboxResult
        ok  = SandboxResult("r", "n", "python", "out", "", 0, 100)
        bad = SandboxResult("r", "n", "python", "",    "err", 1, 100)
        tmo = SandboxResult("r", "n", "python", "",    "timeout", 124, 100, timed_out=True)
        assert ok.success  is True
        assert bad.success is False
        assert tmo.success is False


# ===========================================================================
# RunRegistry
# ===========================================================================

class TestRunRegistry:
    async def _make_graph(self, run_id=None, tenant="acme"):
        from hfa.obs.run_graph import ExecutionGraph
        g = ExecutionGraph(run_id or f"run-{uuid.uuid4().hex[:8]}", tenant)
        return g

    async def test_register_and_get(self):
        from hfa_tools.api.inspector import RunRegistry
        reg = RunRegistry()
        g   = await self._make_graph("run-01")
        await reg.register(g)
        assert reg.get("run-01") is g

    async def test_get_missing_returns_none(self):
        from hfa_tools.api.inspector import RunRegistry
        reg = RunRegistry()
        assert reg.get("nonexistent") is None

    async def test_list_by_tenant_filters(self):
        from hfa_tools.api.inspector import RunRegistry
        reg  = RunRegistry()
        acme = await self._make_graph("run-a", "acme")
        beta = await self._make_graph("run-b", "beta_corp")
        await reg.register(acme)
        await reg.register(beta)
        acme_runs = reg.list_by_tenant("acme")
        assert len(acme_runs) == 1
        assert acme_runs[0].run_id == "run-a"

    async def test_list_by_tenant_limit(self):
        from hfa_tools.api.inspector import RunRegistry
        reg = RunRegistry()
        for i in range(10):
            g = await self._make_graph(f"run-{i:02d}", "acme")
            await reg.register(g)
        runs = reg.list_by_tenant("acme", limit=3)
        assert len(runs) == 3

    async def test_cleanup_removes_old_completed_graphs(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph
        import uuid
        import time

        # ✅ Guardian Fix: Test için kısa interval (0.1s) ve geçerli bir ID kullan
        reg = RunRegistry(max_age=0.1, cleanup_interval=0.1)

        # VALID_TENANT değişkenini doğrudan string olarak veriyoruz (NameError çözümü)
        target_tenant = "acme_corp"
        rid = f"run-{target_tenant}-{uuid.uuid4()}"

        g = ExecutionGraph(rid, target_tenant)
        nid = g.add_node("architect")
        await g.start_node(nid)
        await g.commit_node(nid)  # Graph artık "complete" durumda

        await reg.register(g)
        reg._added_at[rid] = time.time() - 1.0  # Manuel olarak "eski" statüsüne zorla

        await reg.start()
        await asyncio.sleep(0.3)  # 0.1s olan interval'in tetiklenmesi için yeterli süre
        await reg.close()

        assert reg.get(rid) is None  # ✅ Başarıyla temizlenmiş olmalı


# ===========================================================================
# Inspector API
# ===========================================================================

class TestInspectorAPI:
    async def _build_app(self):
        from fastapi import FastAPI
        from hfa_tools.api.inspector import router, RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        app = FastAPI()
        app.include_router(router)

        reg = RunRegistry()
        app.state.run_registry = reg

        # Build a complete graph
        g   = ExecutionGraph("run-api-01", "acme_corp")
        nid = g.add_node("architect")
        await g.start_node(nid)
        await g.commit_node(nid, tokens=200, cost_cents=8)
        cid = g.add_node("coder", parent_ids=[nid])
        await g.start_node(cid)
        # leave coder running

        await reg.register(g)
        return app, reg

    async def test_get_run_graph_200(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/runs/run-api-01",
                headers={"X-Tenant-ID": "acme_corp"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["run_id"] == "run-api-01"
        assert len(data["nodes"]) == 2

    async def test_get_run_graph_404_for_unknown(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/runs/nonexistent",
                headers={"X-Tenant-ID": "acme_corp"},
            )
        assert resp.status_code == 404

    async def test_get_run_graph_403_wrong_tenant(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/runs/run-api-01",
                headers={"X-Tenant-ID": "evil_corp"},
            )
        assert resp.status_code == 403

    async def test_get_run_graph_400_missing_tenant(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/v1/inspector/runs/run-api-01")
        assert resp.status_code == 400

    async def test_get_run_summary_200(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/runs/run-api-01/summary",
                headers={"X-Tenant-ID": "acme_corp"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_nodes"] == 2
        assert data["total_cost_cents"] == 8
        assert data["is_complete"] is False

    async def test_get_nodes_filtered_by_status(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/runs/run-api-01/nodes?status=done",
                headers={"X-Tenant-ID": "acme_corp"},
            )
        assert resp.status_code == 200
        nodes = resp.json()
        assert len(nodes) == 1
        assert nodes[0]["status"] == "done"

    async def test_get_nodes_invalid_status_400(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/runs/run-api-01/nodes?status=invalid",
                headers={"X-Tenant-ID": "acme_corp"},
            )
        assert resp.status_code == 400

    async def test_list_tenant_runs(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/tenants/acme_corp/runs",
                headers={"X-Tenant-ID": "acme_corp"},
            )
        assert resp.status_code == 200
        runs = resp.json()
        assert len(runs) >= 1
        assert runs[0]["run_id"] == "run-api-01"

    async def test_list_tenant_runs_403_cross_tenant(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/tenants/other_tenant/runs",
                headers={"X-Tenant-ID": "acme_corp"},
            )
        assert resp.status_code == 403

    async def test_node_response_cost_cents_is_int(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/runs/run-api-01/nodes",
                headers={"X-Tenant-ID": "acme_corp"},
            )
        nodes = resp.json()
        for node in nodes:
            assert isinstance(node["cost_cents"], int)

    async def test_summary_cost_cents_is_int(self):
        from httpx import AsyncClient, ASGITransport
        app, _ = await self._build_app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/runs/run-api-01/summary",
                headers={"X-Tenant-ID": "acme_corp"},
            )
        data = resp.json()
        assert isinstance(data["total_cost_cents"], int)


# ===========================================================================
# Production Gate — Sprint 6 cross-cutting checks
# ===========================================================================

class TestProductionGateSprint6:
    def test_no_get_event_loop_in_sprint6_sources(self):
        import ast, os
        base = os.path.dirname(os.path.abspath(__file__))
        dirs = [
            os.path.join(base, "..", "..", "hfa-core", "src"),
            os.path.join(base, "..", "src"),
        ]
        violations = []
        for root_dir in dirs:
            if not os.path.exists(root_dir):
                continue
            for dirpath, _, filenames in os.walk(root_dir):
                for fn in filenames:
                    if not fn.endswith(".py"):
                        continue
                    fpath = os.path.join(dirpath, fn)
                    try:
                        tree = ast.parse(open(fpath).read())
                        for node in ast.walk(tree):
                            if (isinstance(node, ast.Call) and
                                    isinstance(node.func, ast.Attribute) and
                                    node.func.attr == "get_event_loop"):
                                violations.append(f"{fpath}:{node.lineno}")
                    except Exception:
                        pass
        assert violations == [], f"get_event_loop() found: {violations}"

    def test_event_bus_abc_contract(self):
        """InMemoryEventBus implements all abstract methods."""
        from hfa.events.bus import EventBus, InMemoryEventBus
        bus = InMemoryEventBus()
        assert isinstance(bus, EventBus)

    async def test_event_bus_close_is_safe(self):
        from hfa.events.bus import InMemoryEventBus
        bus = InMemoryEventBus()
        await bus.close()
        await bus.close()  # second call must not raise

    async def test_node_selector_fail_closed(self):
        """No nodes → SandboxClusterError, not silent None."""
        from hfa_tools.sandbox.distributed_pool import NodeSelector, SandboxClusterError
        sel = NodeSelector()
        with pytest.raises(SandboxClusterError):
            sel.select([], "python")

    async def test_run_registry_tenant_isolation(self):
        """Registry never returns other tenant's graphs."""
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph
        reg  = RunRegistry()
        acme = ExecutionGraph("run-acme", "acme")
        evil = ExecutionGraph("run-evil", "evil_corp")
        await reg.register(acme)
        await reg.register(evil)
        acme_runs = reg.list_by_tenant("acme")
        ids = [g.run_id for g in acme_runs]
        assert "run-evil" not in ids

    async def test_distributed_pool_slot_never_leaks(self):
        """Active slot must be decremented even when execution fails."""
        from hfa_tools.sandbox.distributed_pool import (
            DistributedSandboxPool, SandboxNode, SandboxExecutionError
        )
        node = SandboxNode(node_id="n1", host="localhost", capacity=5)
        pool = DistributedSandboxPool()
        pool._nodes["n1"] = node

        with patch.object(pool, "_run_on_node", side_effect=RuntimeError("fail")):
            with pytest.raises(SandboxExecutionError):
                await pool.execute("r", "python", "pass", timeout=5)

        assert node.active == 0  # ✅ slot released

    async def test_inspector_api_no_registry_returns_503(self):
        from fastapi import FastAPI
        from hfa_tools.api.inspector import router
        from httpx import AsyncClient, ASGITransport
        app = FastAPI()
        app.include_router(router)
        # No app.state.run_registry

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(
                "/v1/inspector/runs/any",
                headers={"X-Tenant-ID": "acme"},
            )
        assert resp.status_code == 503
