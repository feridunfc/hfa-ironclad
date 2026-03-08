"""
hfa-tools/src/hfa_tools/api/inspector.py
IRONCLAD Sprint 6 — Run Inspector API

Exposes ExecutionGraph data via REST endpoints for UI consumption.
Mounts at /v1/inspector (alongside /v1/execute).

Endpoints
---------
GET  /v1/inspector/runs/{run_id}          — full graph snapshot
GET  /v1/inspector/runs/{run_id}/nodes    — flat node list with status
GET  /v1/inspector/runs/{run_id}/summary  — summary only (fast polling)
GET  /v1/inspector/runs/{run_id}/events   — SSE stream for live updates
GET  /v1/inspector/tenants/{tenant_id}/runs — list active runs for tenant

Design
------
* RunRegistry keeps a reference to all live ExecutionGraphs (injected from
  RunOrchestrator._active_graphs at startup time).
* SSE (Server-Sent Events) stream allows the UI to receive live node state
  changes without polling.
* Tenant isolation: requests must carry X-Tenant-ID; inspector only returns
  graphs belonging to that tenant.
* Pagination: list endpoints accept ?limit=N&cursor=<run_id>.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop().
* close() is always safe to call.
* 404 on unknown run_id (not 500).
* Tenant mismatch → 403 (same as TenantMiddleware).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from hfa.obs.run_graph import ExecutionGraph, NodeStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/inspector", tags=["inspector"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class NodeResponse(BaseModel):
    """Single node in the execution graph."""
    node_id:     str
    agent_type:  str
    status:      str
    depth:       int
    parent_ids:  List[str]
    started_at:  Optional[float]
    finished_at: Optional[float]
    duration_ms: Optional[float]
    tokens_used: int
    cost_cents:  int
    error:       Optional[str]
    input_hash:  Optional[str]
    output_hash: Optional[str]
    metadata:    Dict[str, Any]


class GraphSummaryResponse(BaseModel):
    """Lightweight summary for polling."""
    run_id:          str
    tenant_id:       str
    total_nodes:     int
    done:            int
    failed:          int
    running:         int
    pending:         int
    total_tokens:    int
    total_cost_cents: int
    is_complete:     bool
    has_failures:    bool


class GraphResponse(BaseModel):
    """Full graph snapshot including all nodes and edges."""
    run_id:   str
    tenant_id: str
    nodes:    List[NodeResponse]
    edges:    Dict[str, List[str]]
    summary:  GraphSummaryResponse


class RunListItem(BaseModel):
    """Item in a tenant run list."""
    run_id:      str
    is_complete: bool
    has_failures:bool
    total_nodes: int
    running:     int


# ---------------------------------------------------------------------------
# RunRegistry — shared reference to orchestrator graphs
# ---------------------------------------------------------------------------

class RunRegistry:
    """
    In-process registry that maps run_id → ExecutionGraph.

    Injected at startup from RunOrchestrator._active_graphs.
    Supports optional TTL-based cleanup of stale completed graphs.

    Usage
    -----
        registry = RunRegistry(graphs=orchestrator._active_graphs)
        # Pass to InspectorRouter at app creation
    """

    def __init__(
            self,
            graphs: Optional[Dict[str, ExecutionGraph]] = None,
            max_age: float = 3600.0,
            cleanup_interval: float = 300.0,  # ✅ Guardian Fix: Dinamik interval eklendi
    ) -> None:
        self._graphs: Dict[str, ExecutionGraph] = graphs if graphs is not None else {}
        self._max_age = max_age
        self._cleanup_interval = cleanup_interval  # ✅ Konfigüre edilebilir süre saklanıyor
        self._added_at: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start background cleanup task."""
        loop = asyncio.get_running_loop()
        self._cleanup_task = loop.create_task(
            self._cleanup_loop(), name="inspector.registry.cleanup"
        )

    async def close(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def register(self, graph: ExecutionGraph) -> None:
        """Explicitly register a graph (for runs not managed by orchestrator)."""
        async with self._lock:
            self._graphs[graph.run_id]   = graph
            self._added_at[graph.run_id] = time.time()

    def get(self, run_id: str) -> Optional[ExecutionGraph]:
        return self._graphs.get(run_id)

    def list_by_tenant(self, tenant_id: str, limit: int = 50) -> List[ExecutionGraph]:
        graphs = [
            g for g in self._graphs.values()
            if g.tenant_id == tenant_id
        ]
        # Most recent first (by run_id lexicographic desc — UUID-based)
        return sorted(graphs, key=lambda g: g.run_id, reverse=True)[:limit]

    async def _cleanup_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._cleanup_interval)  # ✅ Sabit 300 yerine değişken kullanıyor
                now = time.time()
                async with self._lock:
                    stale = [
                        rid for rid, added in self._added_at.items()
                        if now - added > self._max_age and self._graphs.get(rid) and
                           self._graphs[rid].is_complete()
                    ]
                    for rid in stale:
                        self._graphs.pop(rid, None)
                        self._added_at.pop(rid, None)
                if stale:
                    logger.info("RunRegistry: cleaned %d stale completed graphs", len(stale))
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("RunRegistry cleanup error: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------

def _get_registry(request: Request) -> RunRegistry:
    registry = getattr(request.app.state, "run_registry", None)
    if registry is None:
        raise HTTPException(503, "Run registry not initialised")
    return registry


def _require_graph(registry: RunRegistry, run_id: str, tenant_id: str) -> ExecutionGraph:
    graph = registry.get(run_id)
    if graph is None:
        raise HTTPException(404, f"Run {run_id!r} not found")
    if graph.tenant_id != tenant_id:
        raise HTTPException(403, f"Run {run_id!r} does not belong to your tenant")
    return graph


def _require_tenant(x_tenant_id: Optional[str]) -> str:
    if not x_tenant_id:
        raise HTTPException(400, "X-Tenant-ID header required")
    return x_tenant_id


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------

def _node_response(node) -> NodeResponse:
    return NodeResponse(
        node_id     = node.node_id,
        agent_type  = node.agent_type,
        status      = node.status.value,
        depth       = node.depth,
        parent_ids  = node.parent_ids,
        started_at  = node.started_at,
        finished_at = node.finished_at,
        duration_ms = node.duration_ms,
        tokens_used = node.tokens_used,
        cost_cents  = node.cost_cents,
        error       = node.error,
        input_hash  = node.input_hash,
        output_hash = node.output_hash,
        metadata    = node.metadata,
    )


def _summary_response(graph: ExecutionGraph) -> GraphSummaryResponse:
    snap = graph.snapshot()["summary"]
    return GraphSummaryResponse(
        run_id           = graph.run_id,
        tenant_id        = graph.tenant_id,
        total_nodes      = snap["total_nodes"],
        done             = snap["done"],
        failed           = snap["failed"],
        running          = snap["running"],
        pending          = snap["pending"],
        total_tokens     = snap["total_tokens"],
        total_cost_cents = snap["total_cost_cents"],
        is_complete      = snap["is_complete"],
        has_failures     = snap["has_failures"],
    )


def _graph_response(graph: ExecutionGraph) -> GraphResponse:
    snap  = graph.snapshot()
    nodes = [_node_response(graph.get_node(n["node_id"])) for n in snap["nodes"]]
    edges = {k: list(v) for k, v in snap["edges"].items()}
    return GraphResponse(
        run_id    = graph.run_id,
        tenant_id = graph.tenant_id,
        nodes     = nodes,
        edges     = edges,
        summary   = _summary_response(graph),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/runs/{run_id}", response_model=GraphResponse)
async def get_run_graph(
    run_id:       str,
    request:      Request,
    x_tenant_id:  Optional[str] = Header(None),
) -> GraphResponse:
    """
    Return full execution graph for a run.

    The graph includes all nodes (PENDING/RUNNING/DONE/FAILED),
    edges (dependencies), and summary metrics.

    Raises:
        404: run_id not found.
        403: run_id belongs to a different tenant.
        400: X-Tenant-ID header missing.
    """
    tenant_id = _require_tenant(x_tenant_id)
    registry  = _get_registry(request)
    graph     = _require_graph(registry, run_id, tenant_id)
    return _graph_response(graph)


@router.get("/runs/{run_id}/nodes", response_model=List[NodeResponse])
async def get_run_nodes(
    run_id:      str,
    request:     Request,
    status:      Optional[str] = Query(None, description="Filter by status"),
    x_tenant_id: Optional[str] = Header(None),
) -> List[NodeResponse]:
    """
    Return flat list of nodes in the execution graph.

    Args:
        status: Optional filter ("pending", "running", "done", "failed").
    """
    tenant_id = _require_tenant(x_tenant_id)
    registry  = _get_registry(request)
    graph     = _require_graph(registry, run_id, tenant_id)

    snap  = graph.snapshot()
    nodes = [graph.get_node(n["node_id"]) for n in snap["nodes"]]

    if status:
        try:
            filter_status = NodeStatus(status)
            nodes = [n for n in nodes if n.status == filter_status]
        except ValueError:
            raise HTTPException(400, f"Invalid status filter: {status!r}")

    return [_node_response(n) for n in nodes]


@router.get("/runs/{run_id}/summary", response_model=GraphSummaryResponse)
async def get_run_summary(
    run_id:      str,
    request:     Request,
    x_tenant_id: Optional[str] = Header(None),
) -> GraphSummaryResponse:
    """
    Lightweight summary endpoint for fast UI polling.

    Returns only aggregate counts and completion status — no node details.
    Designed for <5ms response under load.
    """
    tenant_id = _require_tenant(x_tenant_id)
    registry  = _get_registry(request)
    graph     = _require_graph(registry, run_id, tenant_id)
    return _summary_response(graph)


@router.get("/runs/{run_id}/events")
async def stream_run_events(
    run_id:      str,
    request:     Request,
    x_tenant_id: Optional[str] = Header(None),
    interval_ms: int = Query(500, ge=100, le=5000),
) -> StreamingResponse:
    """
    Server-Sent Events stream for live run graph updates.

    The client receives a SSE message whenever node state changes.
    The stream closes automatically when the run completes.

    Usage (JavaScript):
        const es = new EventSource(`/v1/inspector/runs/${runId}/events`, {
            headers: { "X-Tenant-ID": tenantId }
        });
        es.onmessage = (e) => {
            const summary = JSON.parse(e.data);
            updateUI(summary);
        };

    Args:
        interval_ms: Polling interval in milliseconds (100–5000).
    """
    tenant_id = _require_tenant(x_tenant_id)
    registry  = _get_registry(request)
    graph     = _require_graph(registry, run_id, tenant_id)

    async def event_generator() -> AsyncGenerator[str, None]:
        prev_snapshot = None
        while True:
            # Check if client disconnected
            if await request.is_disconnected():
                logger.debug("SSE client disconnected: run=%s", run_id)
                break

            snap = graph.snapshot()
            summary = snap["summary"]

            # Only send if state changed
            summary_json = json.dumps(summary, separators=(",", ":"))
            if summary_json != prev_snapshot:
                prev_snapshot = summary_json
                yield f"data: {summary_json}\n\n"

            if summary["is_complete"]:
                # Send final event then close
                yield f"event: complete\ndata: {summary_json}\n\n"
                break

            await asyncio.sleep(interval_ms / 1000)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


@router.get("/tenants/{tenant_id}/runs", response_model=List[RunListItem])
async def list_tenant_runs(
    tenant_id:   str,
    request:     Request,
    limit:       int = Query(50, ge=1, le=200),
    x_tenant_id: Optional[str] = Header(None),
) -> List[RunListItem]:
    """
    List active and recently completed runs for a tenant.

    Returns runs ordered by run_id descending (most recent first).
    Only returns runs belonging to the requesting tenant.

    Args:
        limit: Maximum number of runs to return (1–200).
    """
    requesting_tenant = _require_tenant(x_tenant_id)
    # Tenant can only see their own runs
    if requesting_tenant != tenant_id:
        raise HTTPException(403, "Cannot access another tenant's runs")

    registry = _get_registry(request)
    graphs   = registry.list_by_tenant(tenant_id, limit=limit)

    return [
        RunListItem(
            run_id       = g.run_id,
            is_complete  = g.is_complete(),
            has_failures = g.has_failures(),
            total_nodes  = len(g.snapshot()["nodes"]),
            running      = g.snapshot()["summary"]["running"],
        )
        for g in graphs
    ]
