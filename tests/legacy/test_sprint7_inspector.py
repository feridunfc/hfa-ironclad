"""
hfa-tools/tests/test_sprint7_inspector.py
IRONCLAD Sprint 7 — Inspector Contract Tests

Eight contracts, exactly as named in the review:

  1. test_get_live_graph_returns_ram_only
  2. test_get_snapshot_falls_back_to_redis_json
  3. test_get_tenant_run_summaries_merges_active_and_history
  4. test_archive_run_writes_snapshot_and_index_atomically
  5. test_cleanup_evicts_only_after_successful_archive
  6. test_historical_snapshot_enforces_tenant_isolation_at_endpoint
  7. test_not_found_and_tenant_mismatch_are_distinct_http_responses
  8. test_redis_failure_degrades_to_ram_only_behavior

Design constraints (from review):
  * Uses registry._graphs  (not _active)
  * Uses registry._redis   (not .redis)
  * get_live_graph()       RAM only — no Redis call
  * get_snapshot()         RAM first, Redis fallback
  * JSON payload in Redis  — no pickle
  * Key format             hfa:run:{run_id} / hfa:tenant_runs:{tenant_id}
  * Pipeline               for atomic archive
  * history_ttl_seconds    controls Redis key TTL
  * created_at in summary  drives sort — not run_id
  * tenant isolation       at endpoint layer (not inside registry.get_snapshot)
  * 404 vs 403             are distinct and must not be conflated
"""
from __future__ import annotations

import asyncio
import json
import uuid
from unittest.mock import AsyncMock, MagicMock

TENANT = "acme_corp"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _rid(tenant: str = TENANT) -> str:
    return f"run-{tenant}-{uuid.uuid4().hex[:8]}"


async def _done_graph(run_id: str, tenant: str = TENANT):
    """Create a fully completed single-node ExecutionGraph."""
    from hfa.obs.run_graph import ExecutionGraph
    g   = ExecutionGraph(run_id, tenant)
    nid = g.add_node("architect")
    await g.start_node(nid)
    await g.commit_node(nid, tokens=100, cost_cents=5)
    return g


def _pipeline_mock():
    """
    Return (redis_mock, pipe_mock).

    pipeline() is a sync method returning an async context manager (pipe_mock).
    pipe.set / pipe.zadd are sync (mirrors real redis-py pipeline behaviour).
    pipe.execute() is async.
    """
    pipe = MagicMock()
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__  = AsyncMock(return_value=False)
    pipe.set        = MagicMock()
    pipe.zadd       = MagicMock()
    pipe.execute    = AsyncMock(return_value=["OK", 1])

    redis = AsyncMock()
    redis.pipeline  = MagicMock(return_value=pipe)
    redis.get       = AsyncMock(return_value=None)
    redis.zrevrange = AsyncMock(return_value=[])
    redis.mget      = AsyncMock(return_value=[])
    return redis, pipe


def _make_app(registry):
    from fastapi import FastAPI
    from hfa_tools.api.inspector import router
    app = FastAPI()
    app.include_router(router)
    app.state.run_registry = registry
    return app


# ===========================================================================
# Contract 1 — get_live_graph_returns_ram_only
# ===========================================================================

class TestGetLiveGraphReturnsRamOnly:
    """
    get_live_graph() must return the in-memory ExecutionGraph and never
    touch Redis, even when a Redis client is configured.
    """

    async def test_returns_graph_object_from_ram(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        redis, _ = _pipeline_mock()
        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        g   = ExecutionGraph(rid, TENANT)
        await reg.register(g)

        result = reg.get_live_graph(rid)

        assert result is g                  # same object, not a copy
        redis.get.assert_not_called()       # Redis never touched

    async def test_returns_none_for_unknown_run(self):
        from hfa_tools.api.inspector import RunRegistry

        assert RunRegistry().get_live_graph("no-such-run") is None

    async def test_returns_none_after_successful_archive_and_eviction(self):
        """Once a graph is archived and evicted, get_live_graph → None."""
        from hfa_tools.api.inspector import RunRegistry

        redis, _ = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0   # mark stale immediately

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        assert reg.get_live_graph(rid) is None   # evicted after archive


# ===========================================================================
# Contract 2 — get_snapshot_falls_back_to_redis_json
# ===========================================================================

class TestGetSnapshotFallsBackToRedisJson:
    """
    get_snapshot() must:
      1. Return live graph snapshot from RAM when present (Redis bypassed).
      2. Fall back to Redis when not in RAM.
      3. Return None when absent from both.
      4. Read JSON from Redis — never pickle.
    """

    async def test_ram_hit_bypasses_redis(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        redis, _ = _pipeline_mock()
        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        await reg.register(ExecutionGraph(rid, TENANT))

        snap = await reg.get_snapshot(rid)

        assert snap is not None
        assert snap["run_id"] == rid
        redis.get.assert_not_called()      # RAM hit — no Redis query

    async def test_redis_fallback_when_not_in_ram(self):
        from hfa_tools.api.inspector import RunRegistry

        rid       = _rid()
        fake_snap = {
            "run_id": rid, "tenant_id": TENANT,
            "nodes": [], "edges": {},
            "summary": {
                "run_id": rid, "tenant_id": TENANT, "created_at": 1.0,
                "total_nodes": 1, "done": 1, "failed": 0,
                "running": 0, "pending": 0,
                "total_tokens": 50, "total_cost_cents": 3,
                "is_complete": True, "has_failures": False,
            },
        }
        redis, _ = _pipeline_mock()
        redis.get.return_value = json.dumps(fake_snap).encode()

        reg  = RunRegistry(redis_client=redis)   # run not registered in RAM
        snap = await reg.get_snapshot(rid)

        assert snap is not None
        assert snap["run_id"] == rid
        redis.get.assert_called_once_with(f"hfa:run:{rid}")

    async def test_returns_none_when_absent_everywhere(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, _ = _pipeline_mock()
        redis.get.return_value = None
        reg  = RunRegistry(redis_client=redis)

        assert await reg.get_snapshot("ghost-run") is None

    async def test_archived_payload_is_json_not_pickle(self):
        """Bytes written to Redis must be valid JSON, never pickle."""
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        pipe.set.assert_called_once()
        written = pipe.set.call_args[0][1]
        parsed  = json.loads(written)          # must not raise
        assert parsed["run_id"] == rid


# ===========================================================================
# Contract 3 — get_tenant_run_summaries_merges_active_and_history
# ===========================================================================

class TestGetTenantRunSummariesMergesActiveAndHistory:
    """
    get_tenant_run_summaries() must combine RAM + Redis results,
    sort by created_at (not run_id), and let active (RAM) win on duplicate.
    """

    async def test_combines_ram_and_redis_results(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        rid_live    = _rid()
        rid_history = _rid()

        redis, _ = _pipeline_mock()
        redis.zrevrange.return_value = [rid_history.encode()]
        redis.mget.return_value = [json.dumps({
            "summary": {
                "run_id": rid_history, "tenant_id": TENANT, "created_at": 1.0,
                "total_nodes": 1, "done": 1, "failed": 0,
                "running": 0, "pending": 0,
                "total_tokens": 0, "total_cost_cents": 0,
                "is_complete": True, "has_failures": False,
            }
        })]

        reg = RunRegistry(redis_client=redis)
        await reg.register(ExecutionGraph(rid_live, TENANT))

        summaries = await reg.get_tenant_run_summaries(TENANT)
        ids       = {s["run_id"] for s in summaries}

        assert rid_live    in ids
        assert rid_history in ids

    async def test_sorted_by_created_at_descending_not_run_id(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        reg  = RunRegistry()
        rids = [_rid() for _ in range(4)]

        for i, rid in enumerate(rids):
            await reg.register(ExecutionGraph(rid, TENANT))
            reg._added_at[rid] = float(i * 10)   # 0, 10, 20, 30

        summaries = await reg.get_tenant_run_summaries(TENANT)
        times     = [s["created_at"] for s in summaries]

        assert times == sorted(times, reverse=True), (
            "Must sort by created_at descending, not run_id lexicographic"
        )

    async def test_active_ram_wins_over_historical_redis(self):
        """Same run_id in both sources: RAM data must win."""
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        rid = _rid()
        # Redis has stale summary (0 nodes)
        redis, _ = _pipeline_mock()
        redis.zrevrange.return_value = [rid.encode()]
        redis.mget.return_value = [json.dumps({
            "summary": {
                "run_id": rid, "tenant_id": TENANT, "created_at": 0.0,
                "total_nodes": 0, "done": 0, "failed": 0,
                "running": 0, "pending": 0,
                "total_tokens": 0, "total_cost_cents": 0,
                "is_complete": False, "has_failures": False,
            }
        })]

        # RAM has graph with 2 nodes
        g = ExecutionGraph(rid, TENANT)
        n1 = g.add_node("architect")
        g.add_node("coder", parent_ids=[n1])

        reg = RunRegistry(redis_client=redis)
        await reg.register(g)

        summaries = await reg.get_tenant_run_summaries(TENANT)
        match     = next(s for s in summaries if s["run_id"] == rid)
        assert match["total_nodes"] == 2, "Active (RAM) must win over historical"

    async def test_different_tenants_are_isolated(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        reg   = RunRegistry()
        rid_a = _rid(TENANT)
        rid_b = _rid("tenant_b")

        await reg.register(ExecutionGraph(rid_a, TENANT))
        await reg.register(ExecutionGraph(rid_b, "tenant_b"))

        ids_a = {s["run_id"] for s in await reg.get_tenant_run_summaries(TENANT)}
        assert rid_a in ids_a
        assert rid_b not in ids_a


# ===========================================================================
# Contract 4 — archive_run_writes_snapshot_and_index_atomically
# ===========================================================================

class TestArchiveRunWritesSnapshotAndIndexAtomically:
    """
    _archive_run() must write SET and ZADD inside a single
    redis.pipeline(transaction=True) so both succeed or neither commits.
    """

    async def test_uses_pipeline_transaction(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        redis.pipeline.assert_called_once()   # pipeline opened
        pipe.execute.assert_called_once()     # single flush

    async def test_set_key_format_is_hfa_run_prefix(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        set_key = pipe.set.call_args[0][0]
        assert set_key == f"hfa:run:{rid}"

    async def test_zadd_key_format_is_hfa_tenant_runs_prefix(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        zadd_key = pipe.zadd.call_args[0][0]
        assert zadd_key == f"hfa:tenant_runs:{TENANT}"

    async def test_set_carries_ttl_from_history_ttl_seconds(self):
        """pipe.set must forward ex=history_ttl_seconds."""
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(
            max_age=0.05, cleanup_interval=0.05,
            redis_client=redis, history_ttl_seconds=7200,
        )
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        kw = pipe.set.call_args[1]
        assert kw.get("ex") == 7200, f"Expected ex=7200, got: {kw}"

    async def test_zadd_score_is_float_timestamp(self):
        """Score in ZADD must be the created_at float (not an arbitrary value)."""
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0.0   # float — int 0 would fail isinstance(0, float)

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        mapping = pipe.zadd.call_args[0][1]   # {run_id: score}
        assert rid in mapping
        assert isinstance(mapping[rid], float)


# ===========================================================================
# Contract 5 — cleanup_evicts_only_after_successful_archive
# ===========================================================================

class TestCleanupEvictsOnlyAfterSuccessfulArchive:
    """
    The cleanup loop must archive first, then evict.
    If archive fails the run must remain in RAM for the next cycle.
    Incomplete (RUNNING/PENDING) runs must never be archived.
    """

    async def test_evicts_from_ram_after_successful_archive(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, _ = _pipeline_mock()   # archive succeeds by default
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        assert reg.get_live_graph(rid) is None   # evicted after archive

    async def test_run_stays_in_ram_when_archive_fails(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        pipe.execute.side_effect = ConnectionError("redis down")

        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        assert reg.get_live_graph(rid) is not None, (
            "Run must remain in RAM when archive fails"
        )

    async def test_running_graph_never_evicted(self):
        """A RUNNING graph (is_complete=False) must never be archived."""
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()

        g   = ExecutionGraph(rid, TENANT)
        nid = g.add_node("architect")
        await g.start_node(nid)       # RUNNING — not complete

        await reg.register(g)
        reg._added_at[rid] = 0

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        pipe.execute.assert_not_called()            # archive never triggered
        assert reg.get_live_graph(rid) is not None  # still live in RAM


# ===========================================================================
# Contract 6 — historical_snapshot_enforces_tenant_isolation_at_endpoint
# ===========================================================================

class TestHistoricalSnapshotEnforcesTenantIsolationAtEndpoint:
    """
    Even when a run is served from Redis (archived), the endpoint must
    enforce tenant isolation: wrong X-Tenant-ID → 403, not 200.
    """

    def _archived_snap(self, rid: str, owner: str) -> bytes:
        return json.dumps({
            "run_id": rid, "tenant_id": owner,
            "nodes": [], "edges": {},
            "summary": {
                "run_id": rid, "tenant_id": owner, "created_at": 1.0,
                "total_nodes": 1, "done": 1, "failed": 0,
                "running": 0, "pending": 0,
                "total_tokens": 0, "total_cost_cents": 0,
                "is_complete": True, "has_failures": False,
            },
        }).encode()

    async def test_owner_tenant_gets_200_from_archived_run(self):
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport

        rid   = _rid()
        redis, _ = _pipeline_mock()
        redis.get.return_value = self._archived_snap(rid, TENANT)

        app = _make_app(RunRegistry(redis_client=redis))
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get(
                f"/v1/inspector/runs/{rid}",
                headers={"X-Tenant-ID": TENANT},
            )
        assert resp.status_code == 200

    async def test_wrong_tenant_gets_403_even_for_archived_run(self):
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport

        rid   = _rid()
        redis, _ = _pipeline_mock()
        redis.get.return_value = self._archived_snap(rid, TENANT)

        app = _make_app(RunRegistry(redis_client=redis))
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get(
                f"/v1/inspector/runs/{rid}",
                headers={"X-Tenant-ID": "evil_corp"},   # wrong tenant
            )
        assert resp.status_code == 403


# ===========================================================================
# Contract 7 — not_found_and_tenant_mismatch_are_distinct_http_responses
# ===========================================================================

class TestNotFoundAndTenantMismatchAreDistinctHttpResponses:
    """
    404 = run_id unknown to the system.
    403 = run_id known but owned by a different tenant.
    These must never be conflated.
    """

    async def test_unknown_run_is_404(self):
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport

        app = _make_app(RunRegistry())
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get(
                "/v1/inspector/runs/never-existed",
                headers={"X-Tenant-ID": TENANT},
            )
        assert resp.status_code == 404

    async def test_existing_run_wrong_tenant_is_403(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph
        from httpx import AsyncClient, ASGITransport

        rid = _rid()
        g   = ExecutionGraph(rid, "owner_tenant")
        reg = RunRegistry()
        await reg.register(g)

        app = _make_app(reg)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get(
                f"/v1/inspector/runs/{rid}",
                headers={"X-Tenant-ID": "thief_tenant"},
            )
        assert resp.status_code == 403

    async def test_403_and_404_are_not_the_same_code(self):
        assert 403 != 404   # explicit contract

    async def test_missing_tenant_header_is_400(self):
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport

        app = _make_app(RunRegistry())
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/v1/inspector/runs/any-run")   # no header
        assert resp.status_code == 400

    async def test_no_registry_on_app_state_is_503(self):
        from fastapi import FastAPI
        from hfa_tools.api.inspector import router
        from httpx import AsyncClient, ASGITransport

        app = FastAPI()
        app.include_router(router)
        # Deliberately no run_registry

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get(
                "/v1/inspector/runs/any-run",
                headers={"X-Tenant-ID": TENANT},
            )
        assert resp.status_code == 503


# ===========================================================================
# Contract 8 — redis_failure_degrades_to_ram_only_behavior
# ===========================================================================

class TestRedisFailureDegradesToRamOnlyBehavior:
    """
    When Redis is unavailable, every path must degrade gracefully:
      get_snapshot()               → None  (→ endpoint returns 404)
      get_tenant_run_summaries()   → RAM-only list (no crash)
      _archive_run()               → re-raise, run NOT evicted from RAM
    No unhandled exception must ever escape to the caller.
    """

    async def test_get_snapshot_redis_error_returns_none(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, _ = _pipeline_mock()
        redis.get.side_effect = ConnectionError("redis down")

        reg  = RunRegistry(redis_client=redis)
        snap = await reg.get_snapshot("some-archived-run")
        assert snap is None    # None → endpoint raises 404, not 500

    async def test_list_tenant_redis_error_returns_ram_runs(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        redis, _ = _pipeline_mock()
        redis.zrevrange.side_effect = ConnectionError("redis down")

        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        await reg.register(ExecutionGraph(rid, TENANT))

        summaries = await reg.get_tenant_run_summaries(TENANT)
        assert len(summaries) == 1
        assert summaries[0]["run_id"] == rid   # RAM run still returned

    async def test_archive_failure_does_not_evict_run(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        pipe.execute.side_effect = ConnectionError("redis down")

        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0

        await reg.start()
        await asyncio.sleep(0.35)
        await reg.close()

        assert reg.get_live_graph(rid) is not None, (
            "Run must NOT be evicted when archive fails"
        )

    async def test_endpoint_returns_404_not_500_when_redis_errors(self):
        """Archived run + Redis error = 404 from endpoint, never 500."""
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport

        redis, _ = _pipeline_mock()
        redis.get.side_effect = ConnectionError("redis down")

        app = _make_app(RunRegistry(redis_client=redis))
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get(
                "/v1/inspector/runs/archived-run",
                headers={"X-Tenant-ID": TENANT},
            )
        assert resp.status_code == 404

    async def test_memory_only_mode_fully_functional(self):
        """No redis_client → registry works without any Redis at all."""
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        reg = RunRegistry()   # redis_client=None
        for i in range(3):
            await reg.register(ExecutionGraph(f"run-mem-{i}", TENANT))

        summaries = await reg.get_tenant_run_summaries(TENANT)
        assert len(summaries) == 3

        snap = await reg.get_snapshot("run-mem-0")
        assert snap is not None


# ===========================================================================
# Extra: SSE headers and resilience
# ===========================================================================

class TestSSEResilience:

    async def test_correct_sse_headers_present(self):
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport

        rid = _rid()
        g   = await _done_graph(rid)
        reg = RunRegistry()
        await reg.register(g)

        app = _make_app(reg)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            async with c.stream(
                "GET",
                f"/v1/inspector/runs/{rid}/events?interval_ms=100",
                headers={"X-Tenant-ID": TENANT},
            ) as resp:
                assert resp.headers.get("cache-control") == "no-cache"
                assert resp.headers.get("x-accel-buffering") == "no"

    async def test_archived_run_yields_single_complete_event(self):
        """Non-live run: stream must emit exactly one 'event: complete' then close."""
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport

        rid       = _rid()
        fake_snap = {
            "run_id": rid, "tenant_id": TENANT,
            "nodes": [], "edges": {},
            "summary": {
                "run_id": rid, "tenant_id": TENANT, "created_at": 1.0,
                "total_nodes": 1, "done": 1, "failed": 0,
                "running": 0, "pending": 0,
                "total_tokens": 0, "total_cost_cents": 0,
                "is_complete": True, "has_failures": False,
            },
        }
        redis, _ = _pipeline_mock()
        redis.get.return_value = json.dumps(fake_snap).encode()

        app = _make_app(RunRegistry(redis_client=redis))
        event_lines = []
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            async with c.stream(
                "GET",
                f"/v1/inspector/runs/{rid}/events",
                headers={"X-Tenant-ID": TENANT},
            ) as resp:
                assert resp.status_code == 200
                async for line in resp.aiter_lines():
                    if line.startswith("event:"):
                        event_lines.append(line)

        assert len(event_lines) == 1
        assert "complete" in event_lines[0]

    async def test_sse_tenant_mismatch_returns_403_before_stream(self):
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport

        rid = _rid()
        g   = await _done_graph(rid, tenant=TENANT)
        reg = RunRegistry()
        await reg.register(g)

        app = _make_app(reg)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            async with c.stream(
                "GET",
                f"/v1/inspector/runs/{rid}/events",
                headers={"X-Tenant-ID": "evil_corp"},
            ) as resp:
                assert resp.status_code == 403
