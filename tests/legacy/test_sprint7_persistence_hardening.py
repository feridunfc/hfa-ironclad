"""
hfa-tools/tests/test_sprint7_persistence_hardening.py
IRONCLAD Sprint 7 — Persistence Hardening Tests (Mini Paket 2)

Six test blocks:

  Block 1 — TTL / Retention
  Block 2 — Atomic archive
  Block 3 — Duplicate archive / idempotency
  Block 4 — Summary normalisation
  Block 5 — Expired / not-found semantics
  Block 6 — Redis failure modes

Requires: inspector.py Mini 2 (SUMMARY_REQUIRED_FIELDS, _archive_run bool,
          Optional created_at, separate JSON decode error path).
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from unittest.mock import AsyncMock, MagicMock

TENANT = "acme_corp"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _rid(tenant: str = TENANT) -> str:
    return f"run-{tenant}-{uuid.uuid4().hex[:8]}"


async def _done_graph(run_id: str, tenant: str = TENANT):
    from hfa.obs.run_graph import ExecutionGraph
    g   = ExecutionGraph(run_id, tenant)
    nid = g.add_node("architect")
    await g.start_node(nid)
    await g.commit_node(nid, tokens=100, cost_cents=5)
    return g


def _pipeline_mock():
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


# ---------------------------------------------------------------------------
# Convenience: trigger a cleanup cycle immediately
# ---------------------------------------------------------------------------

async def _force_cleanup(reg, sleep=0.35):
    await reg.start()
    await asyncio.sleep(sleep)
    await reg.close()


# ===========================================================================
# Block 1 — TTL / Retention
# ===========================================================================

class TestTTLRetention:
    """
    Archived keys must carry a TTL so Redis can expire them automatically.
    The TTL must be configurable and default to a sensible value.
    """

    async def test_archive_sets_ttl_on_snapshot_key(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(
            max_age=0.05, cleanup_interval=0.05,
            redis_client=redis, history_ttl_seconds=3600,
        )
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0.0

        await _force_cleanup(reg)

        kwargs = pipe.set.call_args[1]
        assert kwargs.get("ex") == 3600, (
            f"pipe.set must include ex=3600 (history_ttl_seconds); got {kwargs}"
        )

    async def test_default_ttl_is_7_days(self):
        from hfa_tools.api.inspector import RunRegistry
        reg = RunRegistry()
        assert reg._history_ttl == 604_800, (
            "Default history_ttl_seconds must be 604800 (7 days)"
        )

    async def test_ttl_is_configurable(self):
        """Any positive integer must be accepted as TTL."""
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        for ttl in (3600, 86400, 2_592_000):   # 1h, 1d, 30d
            reg = RunRegistry(
                max_age=0.05, cleanup_interval=0.05,
                redis_client=redis, history_ttl_seconds=ttl,
            )
            rid = _rid()
            g   = await _done_graph(rid)
            await reg.register(g)
            reg._added_at[rid] = 0.0
            pipe.execute.reset_mock()
            pipe.set.reset_mock()

            await _force_cleanup(reg)

            kw = pipe.set.call_args[1]
            assert kw.get("ex") == ttl, (
                f"Expected TTL={ttl}, got {kw}"
            )

    async def test_archive_write_contains_snapshot_and_summary(self):
        """The JSON stored in Redis must have 'run_id', 'nodes', and 'summary'."""
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0.0

        await _force_cleanup(reg)

        written = json.loads(pipe.set.call_args[0][1])
        assert written["run_id"] == rid
        assert "nodes"   in written
        assert "summary" in written
        assert "created_at" in written["summary"]

    async def test_memory_only_mode_archive_returns_true(self):
        """With no Redis client, _archive_run must return True (no-op success)."""
        from hfa_tools.api.inspector import RunRegistry

        reg = RunRegistry()
        rid = _rid()
        g   = await _done_graph(rid)
        result = await reg._archive_run(g, time.time())
        assert result is True


# ===========================================================================
# Block 2 — Atomic archive
# ===========================================================================

class TestAtomicArchive:
    """
    SET (snapshot) and ZADD (tenant index) must be written inside a single
    pipeline(transaction=True). On pipeline failure, _archive_run must return
    False and the cleanup loop must NOT evict the run from RAM.
    """

    async def test_pipeline_transaction_is_used(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0.0

        await _force_cleanup(reg)

        redis.pipeline.assert_called_once()
        pipe.execute.assert_called_once()

    async def test_set_and_zadd_in_same_pipeline(self):
        """Both operations must be queued before a single execute() call."""
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0.0

        await _force_cleanup(reg)

        assert pipe.set.called,  "pipe.set must be called inside pipeline"
        assert pipe.zadd.called, "pipe.zadd must be called inside pipeline"
        assert pipe.execute.call_count == 1, "execute must be called exactly once"

    async def test_archive_returns_true_on_success(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, _ = _pipeline_mock()
        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        result = await reg._archive_run(g, time.time())
        assert result is True

    async def test_archive_returns_false_on_pipeline_error(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        pipe.execute.side_effect = ConnectionError("redis down")

        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        result = await reg._archive_run(g, time.time())
        assert result is False

    async def test_archive_false_prevents_eviction(self):
        """If _archive_run returns False, the run must stay in RAM."""
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        pipe.execute.side_effect = ConnectionError("redis down")

        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0.0

        await _force_cleanup(reg)

        assert reg.get_live_graph(rid) is not None, (
            "Run must stay in RAM when archive returns False"
        )

    async def test_archive_never_raises(self):
        """_archive_run must never propagate an exception to the caller."""
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        pipe.execute.side_effect = RuntimeError("unexpected redis crash")

        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        # This must not raise
        result = await reg._archive_run(g, time.time())
        assert result is False   # failed, but did not raise


# ===========================================================================
# Block 3 — Duplicate archive / idempotency
# ===========================================================================

class TestDuplicateArchiveIdempotency:
    """
    Archiving the same run_id twice must be harmless:
    * SET overwrites the previous snapshot (last-write-wins).
    * ZADD overwrites the ZSET score (no duplicate members).
    * Final state remains readable.
    """

    async def test_second_archive_call_does_not_raise(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, _ = _pipeline_mock()
        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        ts  = time.time()

        r1 = await reg._archive_run(g, ts)
        r2 = await reg._archive_run(g, ts)   # duplicate
        assert r1 is True
        assert r2 is True

    async def test_second_archive_overwrites_snapshot(self):
        """Second SET call carries the latest snapshot payload."""
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        ts  = time.time()

        await reg._archive_run(g, ts)
        await reg._archive_run(g, ts)

        # pipe.set should have been called twice
        assert pipe.set.call_count == 2

    async def test_zadd_overwrites_score_no_duplicate_member(self):
        """
        Redis ZSET semantics: ZADD with same member overwrites score.
        Our test verifies we always pass {run_id: created_at} — no appending.
        """
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        ts  = time.time()

        await reg._archive_run(g, ts)
        await reg._archive_run(g, ts)

        # Both calls must use the same mapping structure
        for call in pipe.zadd.call_args_list:
            mapping = call[0][1]   # second positional arg
            assert rid in mapping
            assert mapping[rid] == ts

    async def test_cleanup_archives_only_once_per_run(self):
        """
        Cleanup loop must evict after the first successful archive.
        After eviction, the run is not in RAM → not in stale list → no second archive.
        """
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0.0

        await _force_cleanup(reg)

        assert pipe.execute.call_count == 1, (
            "archive must be called exactly once per run in a normal cleanup cycle"
        )


# ===========================================================================
# Block 4 — Summary normalisation
# ===========================================================================

class TestSummaryNormalisation:
    """
    _normalize_summary() must produce a dict satisfying SUMMARY_REQUIRED_FIELDS
    regardless of what's missing in the ``raw`` input.
    Mixed active + historical data must not produce shape mismatches.
    Sort must always be created_at descending.
    Same run must not appear twice in the merged list.
    """

    def test_all_required_fields_present_from_empty_raw(self):
        from hfa_tools.api.inspector import _normalize_summary, SUMMARY_REQUIRED_FIELDS
        result = _normalize_summary({}, "r1", "t1", 1234.5)
        assert SUMMARY_REQUIRED_FIELDS <= result.keys(), (
            f"Missing fields: {SUMMARY_REQUIRED_FIELDS - result.keys()}"
        )

    def test_required_fields_constant_is_correct_size(self):
        from hfa_tools.api.inspector import SUMMARY_REQUIRED_FIELDS
        assert len(SUMMARY_REQUIRED_FIELDS) == 12

    def test_created_at_none_falls_back_to_zero(self):
        """Missing created_at must not crash — defaults to 0.0."""
        from hfa_tools.api.inspector import _normalize_summary
        result = _normalize_summary({}, "r1", "t1", None)
        assert result["created_at"] == 0.0

    def test_created_at_none_run_sorts_to_bottom(self):
        """A run with created_at=None (→0.0) must appear after runs with timestamps."""
        from hfa_tools.api.inspector import _normalize_summary

        s1 = _normalize_summary({}, "r1", "t1", 100.0)
        s2 = _normalize_summary({}, "r2", "t1", None)   # 0.0
        s3 = _normalize_summary({}, "r3", "t1", 200.0)

        summaries = sorted(
            [s1, s2, s3],
            key=lambda s: s["created_at"],
            reverse=True,
        )
        assert summaries[0]["run_id"] == "r3"    # newest
        assert summaries[-1]["run_id"] == "r2"   # None → 0.0 → oldest

    def test_run_id_and_tenant_id_injected_not_from_raw(self):
        """run_id/tenant_id must come from caller args, never from raw dict."""
        from hfa_tools.api.inspector import _normalize_summary
        result = _normalize_summary(
            {"run_id": "wrong", "tenant_id": "wrong"},
            run_id="correct", tenant_id="correct", created_at=1.0,
        )
        assert result["run_id"]    == "correct"
        assert result["tenant_id"] == "correct"

    def test_total_cost_cents_is_int(self):
        """cost_cents must always be int — never float USD."""
        from hfa_tools.api.inspector import _normalize_summary
        result = _normalize_summary({"total_cost_cents": 42}, "r", "t", 1.0)
        assert isinstance(result["total_cost_cents"], int)
        assert result["total_cost_cents"] == 42

    async def test_merge_deduplicates_same_run_id(self):
        """A run_id appearing in both RAM and Redis must appear exactly once."""
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        rid = _rid()
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

        reg = RunRegistry(redis_client=redis)
        await reg.register(ExecutionGraph(rid, TENANT))

        summaries = await reg.get_tenant_run_summaries(TENANT)
        matched   = [s for s in summaries if s["run_id"] == rid]
        assert len(matched) == 1, "Same run_id must not appear twice"

    async def test_sort_by_created_at_is_always_descending(self):
        """Sort order must hold even with mixed live and archived summaries."""
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        rid_hist = _rid()
        redis, _ = _pipeline_mock()
        redis.zrevrange.return_value = [rid_hist.encode()]
        redis.mget.return_value = [json.dumps({
            "summary": {
                "run_id": rid_hist, "tenant_id": TENANT, "created_at": 1.0,
                "total_nodes": 1, "done": 1, "failed": 0,
                "running": 0, "pending": 0,
                "total_tokens": 0, "total_cost_cents": 0,
                "is_complete": True, "has_failures": False,
            }
        })]

        reg = RunRegistry(redis_client=redis)

        rid_live = _rid()
        await reg.register(ExecutionGraph(rid_live, TENANT))
        reg._added_at[rid_live] = 999.0   # clearly newest

        summaries = await reg.get_tenant_run_summaries(TENANT)
        times     = [s["created_at"] for s in summaries]
        assert times == sorted(times, reverse=True)


# ===========================================================================
# Block 5 — Expired / not-found semantics
# ===========================================================================

class TestExpiredNotFoundSemantics:
    """
    TTL-expired archived runs and never-existed runs both surface as 404.
    No tombstone is kept. There is no 410 distinction in this sprint.
    """

    async def test_ttl_expired_run_get_snapshot_returns_none(self):
        """
        Simulate TTL expiry: Redis returns None for the key.
        get_snapshot must return None → endpoint raises 404.
        """
        from hfa_tools.api.inspector import RunRegistry

        redis, _ = _pipeline_mock()
        redis.get.return_value = None   # key expired / evicted

        reg  = RunRegistry(redis_client=redis)
        snap = await reg.get_snapshot("expired-run-id")
        assert snap is None

    async def test_expired_run_endpoint_returns_404(self):
        """404 for expired-TTL run and 404 for never-existed run are identical."""
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport
        from fastapi import FastAPI
        from hfa_tools.api.inspector import router

        redis, _ = _pipeline_mock()
        redis.get.return_value = None   # expired

        app = FastAPI()
        app.include_router(router)
        app.state.run_registry = RunRegistry(redis_client=redis)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r1 = await c.get("/v1/inspector/runs/expired-run",
                             headers={"X-Tenant-ID": TENANT})
            r2 = await c.get("/v1/inspector/runs/never-existed",
                             headers={"X-Tenant-ID": TENANT})

        assert r1.status_code == 404
        assert r2.status_code == 404   # same response — no 410

    async def test_no_410_gone_response(self):
        """
        The system does not distinguish expired from never-existed.
        No endpoint must ever return 410.
        """
        from hfa_tools.api.inspector import RunRegistry
        from httpx import AsyncClient, ASGITransport
        from fastapi import FastAPI
        from hfa_tools.api.inspector import router

        app = FastAPI()
        app.include_router(router)
        app.state.run_registry = RunRegistry()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/v1/inspector/runs/gone-run",
                               headers={"X-Tenant-ID": TENANT})

        assert resp.status_code != 410, "410 Gone must not be returned in this sprint"
        assert resp.status_code == 404

    async def test_stale_zset_member_after_ttl_expiry(self):
        """
        After TTL expiry, the ZSET may still contain the run_id as a member
        but mget returns None for the expired key.
        get_tenant_run_summaries must silently skip None entries.
        """
        from hfa_tools.api.inspector import RunRegistry

        rid   = _rid()
        redis, _ = _pipeline_mock()
        redis.zrevrange.return_value = [rid.encode()]
        redis.mget.return_value      = [None]   # key expired, ZSET member stale

        reg       = RunRegistry(redis_client=redis)
        summaries = await reg.get_tenant_run_summaries(TENANT)

        assert all(s["run_id"] != rid for s in summaries), (
            "Expired (None mget result) must be silently skipped in summary list"
        )


# ===========================================================================
# Block 6 — Redis failure modes
# ===========================================================================

class TestRedisFailureModes:
    """
    Every Redis call path must degrade gracefully.
    No failure must produce an unhandled exception or a 500 response.
    Live runs must always be served from RAM regardless of Redis state.
    """

    # -- get_snapshot --

    async def test_get_snapshot_redis_get_error_returns_none(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, _ = _pipeline_mock()
        redis.get.side_effect = ConnectionError("redis down")

        snap = await RunRegistry(redis_client=redis).get_snapshot("any-run")
        assert snap is None

    async def test_get_snapshot_malformed_json_returns_none(self):
        """Corrupt archive entry must be treated as not-found."""
        from hfa_tools.api.inspector import RunRegistry

        redis, _ = _pipeline_mock()
        redis.get.return_value = b"{ this is not json !!!"

        snap = await RunRegistry(redis_client=redis).get_snapshot("corrupt-run")
        assert snap is None

    async def test_get_snapshot_live_run_unaffected_by_redis_error(self):
        """A live (RAM) run must be returned even when Redis is fully broken."""
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        redis, _ = _pipeline_mock()
        redis.get.side_effect = ConnectionError("redis down")

        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        await reg.register(ExecutionGraph(rid, TENANT))

        snap = await reg.get_snapshot(rid)
        assert snap is not None    # RAM path unaffected by Redis error
        assert snap["run_id"] == rid

    # -- get_tenant_run_summaries --

    async def test_mget_error_returns_ram_only(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        redis, _ = _pipeline_mock()
        redis.zrevrange.return_value = [b"some-archived-rid"]
        redis.mget.side_effect = ConnectionError("redis mget down")

        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        await reg.register(ExecutionGraph(rid, TENANT))

        summaries = await reg.get_tenant_run_summaries(TENANT)
        assert len(summaries) == 1
        assert summaries[0]["run_id"] == rid

    async def test_zrevrange_error_returns_ram_only(self):
        from hfa_tools.api.inspector import RunRegistry
        from hfa.obs.run_graph import ExecutionGraph

        redis, _ = _pipeline_mock()
        redis.zrevrange.side_effect = TimeoutError("redis timeout")

        reg = RunRegistry(redis_client=redis)
        rid = _rid()
        await reg.register(ExecutionGraph(rid, TENANT))

        summaries = await reg.get_tenant_run_summaries(TENANT)
        assert len(summaries) == 1

    async def test_mget_partial_none_entries_skipped(self):
        """mget may return a mix of bytes and None; None entries must be skipped."""
        from hfa_tools.api.inspector import RunRegistry

        rid_ok  = _rid()
        rid_nil = _rid()
        good_entry = json.dumps({
            "summary": {
                "run_id": rid_ok, "tenant_id": TENANT, "created_at": 1.0,
                "total_nodes": 1, "done": 1, "failed": 0,
                "running": 0, "pending": 0,
                "total_tokens": 0, "total_cost_cents": 0,
                "is_complete": True, "has_failures": False,
            }
        })
        redis, _ = _pipeline_mock()
        redis.zrevrange.return_value = [rid_ok.encode(), rid_nil.encode()]
        redis.mget.return_value      = [good_entry.encode(), None]   # second expired

        summaries = await RunRegistry(redis_client=redis).get_tenant_run_summaries(TENANT)
        ids = {s["run_id"] for s in summaries}
        assert rid_ok  in ids
        assert rid_nil not in ids   # None entry silently dropped

    # -- pipeline / archive --

    async def test_pipeline_execute_error_archive_returns_false(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        pipe.execute.side_effect = OSError("socket error")

        reg    = RunRegistry(redis_client=redis)
        rid    = _rid()
        g      = await _done_graph(rid)
        result = await reg._archive_run(g, time.time())
        assert result is False

    async def test_pipeline_execute_error_no_eviction(self):
        from hfa_tools.api.inspector import RunRegistry

        redis, pipe = _pipeline_mock()
        pipe.execute.side_effect = OSError("socket error")

        reg = RunRegistry(max_age=0.05, cleanup_interval=0.05, redis_client=redis)
        rid = _rid()
        g   = await _done_graph(rid)
        await reg.register(g)
        reg._added_at[rid] = 0.0

        await _force_cleanup(reg)

        assert reg.get_live_graph(rid) is not None

    # -- endpoint 500 guard --

    async def test_endpoint_never_returns_500_on_redis_error(self):
        from hfa_tools.api.inspector import RunRegistry
        from fastapi import FastAPI
        from hfa_tools.api.inspector import router
        from httpx import AsyncClient, ASGITransport

        redis, _ = _pipeline_mock()
        redis.get.side_effect = ConnectionError("redis down")

        app = FastAPI()
        app.include_router(router)
        app.state.run_registry = RunRegistry(redis_client=redis)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get(
                "/v1/inspector/runs/archived-run",
                headers={"X-Tenant-ID": TENANT},
            )

        assert resp.status_code != 500, "Redis error must never produce 500"
        assert resp.status_code == 404
