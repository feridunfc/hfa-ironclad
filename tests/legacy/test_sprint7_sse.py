"""
hfa-tools/tests/test_sprint7_sse.py
IRONCLAD Sprint 7 — SSE Resilience & Regression Tests
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException, Request

VALID_TENANT = "acme_corp"


def _make_rid() -> str:
    return f"run-{VALID_TENANT}-{uuid.uuid4()}"


def _make_request(registry, disconnected_side_effect=None) -> AsyncMock:
    req = AsyncMock(spec=Request)
    req.app = SimpleNamespace(state=SimpleNamespace(run_registry=registry))

    if disconnected_side_effect is None:
        req.is_disconnected = AsyncMock(return_value=False)
    else:
        req.is_disconnected = AsyncMock(side_effect=disconnected_side_effect)

    return req


class TestSprint7SSEResilience:
    @pytest.mark.asyncio
    async def test_sse_heartbeat_yields_comments(self, monkeypatch):
        """
        Veri değişmediğinde heartbeat (: heartbeat) gönderilmelidir.
        """
        from hfa_tools.api.inspector import RunRegistry, stream_run_events
        from hfa.obs.run_graph import ExecutionGraph
        import hfa_tools.api.inspector as inspector_mod

        rid = _make_rid()
        graph = ExecutionGraph(rid, VALID_TENANT)

        # Graph boşsa sende complete oluyor; heartbeat testi için incomplete olmalı.
        nid = graph.add_node("architect")
        await graph.start_node(nid)

        registry = RunRegistry()
        await registry.register(graph)

        req = _make_request(
            registry,
            disconnected_side_effect=[False, False, True],
        )

        fake_times = iter([0.0, 0.0, 16.1, 16.2, 16.3, 16.4])
        monkeypatch.setattr(inspector_mod.time, "time", lambda: next(fake_times))

        response = await stream_run_events(
            rid,
            req,
            x_tenant_id=VALID_TENANT,
            interval_ms=1,
        )
        gen = response.body_iterator

        first_event = await anext(gen)
        assert first_event.startswith("data: ")

        first_payload = json.loads(first_event[len("data: "):].strip())
        assert first_payload["running"] >= 1
        assert first_payload["is_complete"] is False

        second_event = await anext(gen)
        assert second_event == ": heartbeat\n\n"

        with pytest.raises(StopAsyncIteration):
            await anext(gen)

    @pytest.mark.asyncio
    async def test_sse_client_disconnect_breaks_loop(self):
        """
        İstemci bağlantıyı kopardığında generator kapanmalıdır.
        """
        from hfa_tools.api.inspector import RunRegistry, stream_run_events
        from hfa.obs.run_graph import ExecutionGraph

        rid = _make_rid()
        graph = ExecutionGraph(rid, VALID_TENANT)

        # Incomplete graph yarat
        nid = graph.add_node("architect")
        await graph.start_node(nid)

        registry = RunRegistry()
        await registry.register(graph)

        req = _make_request(
            registry,
            disconnected_side_effect=[False, True],
        )

        response = await stream_run_events(
            rid,
            req,
            x_tenant_id=VALID_TENANT,
            interval_ms=1,
        )
        gen = response.body_iterator

        first_event = await anext(gen)
        assert first_event.startswith("data: ")

        with pytest.raises(StopAsyncIteration):
            await anext(gen)

    @pytest.mark.asyncio
    async def test_archived_run_yields_complete_and_closes(self):
        """
        Canlı graph yoksa, history snapshot'tan tek seferlik complete event dönmelidir.
        """
        from hfa_tools.api.inspector import RunRegistry, stream_run_events

        rid = _make_rid()
        registry = RunRegistry()

        fake_snapshot = {
            "run_id": rid,
            "tenant_id": VALID_TENANT,
            "nodes": [],
            "edges": {},
            "summary": {
                "run_id": rid,
                "tenant_id": VALID_TENANT,
                "total_nodes": 1,
                "done": 1,
                "failed": 0,
                "running": 0,
                "pending": 0,
                "total_tokens": 0,
                "total_cost_cents": 0,
                "is_complete": True,
                "has_failures": False,
            },
        }

        registry.get_snapshot = AsyncMock(return_value=fake_snapshot)

        req = _make_request(registry)

        response = await stream_run_events(
            rid,
            req,
            x_tenant_id=VALID_TENANT,
            interval_ms=1,
        )
        gen = response.body_iterator

        event = await anext(gen)
        assert event.startswith("event: complete\n")

        payload = json.loads(event.split("data: ", 1)[1].strip())
        assert payload["is_complete"] is True

        with pytest.raises(StopAsyncIteration):
            await anext(gen)

    @pytest.mark.asyncio
    async def test_archived_missing_run_raises_404(self):
        """
        Canlı graph yok ve snapshot da yoksa endpoint çağrısı 404 üretmelidir.
        """
        from hfa_tools.api.inspector import RunRegistry, stream_run_events

        rid = _make_rid()
        registry = RunRegistry()
        registry.get_snapshot = AsyncMock(return_value=None)

        req = _make_request(registry)

        with pytest.raises(HTTPException) as exc_info:
            await stream_run_events(
                rid,
                req,
                x_tenant_id=VALID_TENANT,
                interval_ms=1,
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_live_run_tenant_mismatch_rejected_before_stream(self):
        """
        Canlı graph başka tenant'a aitse stream başlamadan 403 dönmelidir.
        """
        from hfa_tools.api.inspector import RunRegistry, stream_run_events
        from hfa.obs.run_graph import ExecutionGraph

        rid = _make_rid()
        graph = ExecutionGraph(rid, VALID_TENANT)

        # Live graph gerçekten live olsun
        nid = graph.add_node("architect")
        await graph.start_node(nid)

        registry = RunRegistry()
        await registry.register(graph)

        req = _make_request(registry)

        with pytest.raises(HTTPException) as exc_info:
            await stream_run_events(
                rid,
                req,
                x_tenant_id="evil_tenant",
                interval_ms=1,
            )

        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_live_run_emits_final_complete_event(self):
        """
        Canlı run tamamlandığında önce güncel data, sonra final complete event gelmelidir.
        """
        from hfa_tools.api.inspector import RunRegistry, stream_run_events
        from hfa.obs.run_graph import ExecutionGraph

        rid = _make_rid()
        graph = ExecutionGraph(rid, VALID_TENANT)

        # İlk durumda incomplete olmalı
        nid = graph.add_node("architect")
        await graph.start_node(nid)

        registry = RunRegistry()
        await registry.register(graph)

        req = _make_request(
            registry,
            disconnected_side_effect=[False, False, False],
        )

        response = await stream_run_events(
            rid,
            req,
            x_tenant_id=VALID_TENANT,
            interval_ms=1,
        )
        gen = response.body_iterator

        first_event = await anext(gen)
        assert first_event.startswith("data: ")
        first_payload = json.loads(first_event[len("data: "):].strip())
        assert first_payload["is_complete"] is False
        assert first_payload["running"] >= 1

        await graph.commit_node(nid)

        second_event = await anext(gen)
        assert second_event.startswith("data: ")
        second_payload = json.loads(second_event[len("data: "):].strip())
        assert second_payload["is_complete"] is True
        assert second_payload["done"] >= 1

        third_event = await anext(gen)
        assert third_event.startswith("event: complete\n")
        third_payload = json.loads(third_event.split("data: ", 1)[1].strip())
        assert third_payload["is_complete"] is True

        with pytest.raises(StopAsyncIteration):
            await anext(gen)