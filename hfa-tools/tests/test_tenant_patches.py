"""
hfa-tools/tests/test_tenant_patches.py
IRONCLAD Sprint 2 — Tenant middleware patch regression tests.

Covers:
  Patch 1: _header_get() case-insensitive lookup
  Patch 2: validate_run_id_format() called on every run_id path
  Patch 3: UUIDv4 enforcement (version + nil-UUID guard)
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

VALID_V4   = "550e8400-e29b-41d4-a716-446655440000"
VALID_RID  = f"run-acme_corp-{VALID_V4}"

V1_UUID    = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
V1_RID     = f"run-acme_corp-{V1_UUID}"

V3_UUID    = "6fa459ea-ee8a-3ca4-894e-db77e160355e"
NIL_UUID   = "00000000-0000-0000-0000-000000000000"
NIL_RID    = f"run-acme_corp-{NIL_UUID}"


def _make_request(
    headers: dict,
    path: str = "/api/test",
    path_params: dict | None = None,
):
    """Build a MagicMock that quacks like a Starlette Request."""
    req = MagicMock()
    req.url.path     = path
    req.headers      = headers        # plain dict — tests case-insensitivity
    req.path_params  = path_params or {}
    req.state        = MagicMock()
    return req


async def _dispatch(headers, path="/api/test", path_params=None):
    """Run TenantMiddleware.dispatch and return the Response."""
    from hfa_tools.middleware.tenant import TenantMiddleware

    app       = AsyncMock()
    resp_mock = MagicMock()
    resp_mock.headers = {}
    resp_mock.status_code = 200
    app.return_value = resp_mock

    mw = TenantMiddleware(app)

    async def call_next(req):
        return resp_mock

    request = _make_request(headers, path, path_params)
    return await mw.dispatch(request, call_next)


# ===========================================================================
# Patch 1 — _header_get case-insensitive
# ===========================================================================

class TestHeaderGet:
    """_header_get: every casing variant resolves correctly."""

    def test_lowercase_key_lowercase_lookup(self):
        from hfa_tools.middleware.tenant import _header_get
        assert _header_get({"x-tenant-id": "acme"}, "x-tenant-id") == "acme"

    def test_uppercase_key_lowercase_lookup(self):
        from hfa_tools.middleware.tenant import _header_get
        assert _header_get({"X-Tenant-Id": "acme"}, "x-tenant-id") == "acme"

    def test_mixed_key_canonical_lookup(self):
        from hfa_tools.middleware.tenant import _header_get
        assert _header_get({"x-tenant-id": "acme"}, "X-Tenant-Id") == "acme"

    def test_all_caps_key(self):
        from hfa_tools.middleware.tenant import _header_get
        assert _header_get({"X-TENANT-ID": "acme"}, "x-tenant-id") == "acme"

    def test_missing_header_returns_none(self):
        from hfa_tools.middleware.tenant import _header_get
        assert _header_get({}, "x-tenant-id") is None

    def test_other_headers_not_returned(self):
        from hfa_tools.middleware.tenant import _header_get
        headers = {"Authorization": "Bearer tok", "Content-Type": "application/json"}
        assert _header_get(headers, "x-tenant-id") is None

    def test_run_id_header_case_insensitive(self):
        from hfa_tools.middleware.tenant import _header_get
        assert _header_get({"X-Run-ID": VALID_RID}, "x-run-id") == VALID_RID

    @pytest.mark.asyncio
    async def test_middleware_accepts_uppercase_tenant_header(self):
        """Full middleware path: dict header with uppercase key must work."""
        resp = await _dispatch({"X-Tenant-Id": "acme_corp"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_middleware_accepts_lowercase_tenant_header(self):
        resp = await _dispatch({"x-tenant-id": "acme_corp"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_middleware_accepts_all_caps_tenant_header(self):
        resp = await _dispatch({"X-TENANT-ID": "acme_corp"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_middleware_accepts_lowercase_run_id_header(self):
        """x-run-id (lowercase) must be read correctly."""
        resp = await _dispatch({"x-run-id": VALID_RID})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_middleware_accepts_uppercase_run_id_header(self):
        resp = await _dispatch({"X-Run-Id": VALID_RID})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_tenant_propagated_in_response_header(self):
        resp = await _dispatch({"x-tenant-id": "acme_corp"})
        assert resp.headers.get("X-Tenant-Id") == "acme_corp"


# ===========================================================================
# Patch 2 — validate_run_id_format enforced on every path
# ===========================================================================

class TestRunIdFormatEnforcement:
    """validate_run_id_format() is called for header, path, and cross-check paths."""

    # ── header path ─────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_valid_run_id_header_accepted(self):
        resp = await _dispatch({"x-run-id": VALID_RID})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_bad_format_run_id_header_rejected(self):
        resp = await _dispatch({"x-run-id": "job-acme_corp-" + VALID_V4})
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_non_uuid_tail_run_id_header_rejected(self):
        resp = await _dispatch({"x-run-id": "run-acme_corp-not-a-uuid"})
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_cross_check_mismatch_rejected(self):
        """X-Tenant-Id present + X-Run-Id with different tenant → 403."""
        resp = await _dispatch({
            "x-tenant-id": "acme_corp",
            "x-run-id": f"run-evil_corp-{VALID_V4}",
        })
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_cross_check_match_accepted(self):
        resp = await _dispatch({
            "x-tenant-id": "acme_corp",
            "x-run-id": VALID_RID,
        })
        assert resp.status_code == 200

    # ── path param path ──────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_valid_run_id_path_param_accepted(self):
        resp = await _dispatch(
            headers={},
            path=f"/runs/{VALID_RID}",
            path_params={"run_id": VALID_RID},
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_non_uuid_tail_path_run_id_rejected(self):
        bad = "run-acme_corp-BADTAIL"
        resp = await _dispatch(
            headers={},
            path=f"/runs/{bad}",
            path_params={"run_id": bad},
        )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_tenant_id_path_param_with_valid_run_id(self):
        resp = await _dispatch(
            headers={},
            path=f"/tenants/acme_corp/runs/{VALID_RID}",
            path_params={"tenant_id": "acme_corp", "run_id": VALID_RID},
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_tenant_id_path_param_with_mismatch_run_id(self):
        mismatch_rid = f"run-evil_corp-{VALID_V4}"
        resp = await _dispatch(
            headers={},
            path=f"/tenants/acme_corp/runs/{mismatch_rid}",
            path_params={"tenant_id": "acme_corp", "run_id": mismatch_rid},
        )
        assert resp.status_code == 403

    # ── validate_run_id_format unit contract ─────────────────────────────

    def test_returns_tenant_and_uuid(self):
        from hfa_tools.middleware.tenant import validate_run_id_format
        tenant, uid = validate_run_id_format(VALID_RID)
        assert tenant == "acme_corp"
        assert uid    == VALID_V4

    def test_wrong_prefix_raises(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError, match="expected"):
            validate_run_id_format(f"job-acme_corp-{VALID_V4}")

    def test_too_few_segments_raises(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError):
            validate_run_id_format("run-acme_corp")

    def test_empty_tenant_segment_raises(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError, match="tenant"):
            validate_run_id_format(f"run--{VALID_V4}")

    def test_normalises_uuid_to_lowercase(self):
        from hfa_tools.middleware.tenant import validate_run_id_format
        upper_v4  = VALID_V4.upper()
        _, uid = validate_run_id_format(f"run-acme_corp-{upper_v4}")
        assert uid == VALID_V4  # normalised to lowercase

    def test_invalid_uuid_raises(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError, match="valid UUID"):
            validate_run_id_format("run-acme_corp-not-a-real-uuid")


# ===========================================================================
# Patch 3 — UUIDv4 enforcement (version + nil-UUID guard)
# ===========================================================================

class TestUUIDv4Enforcement:
    """validate_run_id_format() must reject non-v4 and nil UUIDs."""

    def test_v4_accepted(self):
        from hfa_tools.middleware.tenant import validate_run_id_format
        tenant, uid = validate_run_id_format(VALID_RID)
        assert tenant == "acme_corp"

    def test_v1_rejected(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError, match="UUIDv4"):
            validate_run_id_format(V1_RID)

    def test_v1_error_message_contains_version(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError) as exc_info:
            validate_run_id_format(V1_RID)
        assert "version 1" in str(exc_info.value)

    def test_v3_rejected(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError, match="UUIDv4"):
            validate_run_id_format(f"run-acme_corp-{V3_UUID}")

    def test_v5_rejected(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        v5 = str(uuid.uuid5(uuid.NAMESPACE_DNS, "example.com"))
        with pytest.raises(TenantFormatError, match="UUIDv4"):
            validate_run_id_format(f"run-acme_corp-{v5}")

    def test_nil_uuid_rejected(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError, match="nil"):
            validate_run_id_format(NIL_RID)

    def test_nil_uuid_error_message_clear(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError) as exc_info:
            validate_run_id_format(NIL_RID)
        msg = str(exc_info.value)
        assert "nil" in msg.lower() or "nil UUID" in msg

    @pytest.mark.asyncio
    async def test_middleware_rejects_v1_uuid_in_run_id_header(self):
        resp = await _dispatch({"x-run-id": V1_RID})
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_middleware_rejects_nil_uuid_in_run_id_header(self):
        resp = await _dispatch({"x-run-id": NIL_RID})
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_middleware_rejects_v1_uuid_in_run_id_path(self):
        resp = await _dispatch(
            headers={},
            path=f"/runs/{V1_RID}",
            path_params={"run_id": V1_RID},
        )
        assert resp.status_code == 403

    def test_different_v4_uuids_all_accepted(self):
        """Multiple independent UUIDv4 values — all valid."""
        from hfa_tools.middleware.tenant import validate_run_id_format

        for _ in range(10):
            fresh = str(uuid.uuid4())
            rid   = f"run-acme_corp-{fresh}"
            tenant, returned_uuid = validate_run_id_format(rid)
            assert tenant == "acme_corp"
            assert returned_uuid == fresh

    def test_uuid_version_check_is_exact(self):
        """Confirm Python uuid module version attribute matches expectation."""
        v4 = uuid.uuid4()
        assert v4.version == 4
        v1 = uuid.uuid1()
        assert v1.version == 1


# ===========================================================================
# Combined flow: all 3 patches in one request
# ===========================================================================

class TestCombinedPatches:
    """End-to-end: lowercase dict header + run_id enforced + UUIDv4 required."""

    @pytest.mark.asyncio
    async def test_all_good(self):
        """Lowercase dict headers, valid v4 run_id → 200."""
        resp = await _dispatch({
            "x-tenant-id": "acme_corp",
            "x-run-id": VALID_RID,
        })
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_lowercase_run_id_header_with_v1_uuid_rejected(self):
        """Lowercase header key + v1 UUID tail → 403 (all 3 patches active)."""
        resp = await _dispatch({"x-run-id": V1_RID})
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_uppercase_run_id_header_with_nil_uuid_rejected(self):
        resp = await _dispatch({"X-Run-Id": NIL_RID})
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_mixed_case_tenant_header_with_wrong_run_id_tenant(self):
        resp = await _dispatch({
            "X-Tenant-ID": "acme_corp",
            "x-run-id":    f"run-other_corp-{VALID_V4}",
        })
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_tenant_context_source_set_correctly(self):
        """When using run_id header only, source must be 'run_id_header'."""
        from hfa_tools.middleware.tenant import TenantMiddleware

        app = AsyncMock()
        mw  = TenantMiddleware(app)
        req = _make_request({"x-run-id": VALID_RID})

        captured_state = {}

        async def call_next(r):
            captured_state["tenant"] = r.state.tenant
            resp = MagicMock()
            resp.headers     = {}
            resp.status_code = 200
            return resp

        await mw.dispatch(req, call_next)
        ctx = captured_state["tenant"]
        assert ctx.tenant_id == "acme_corp"
        assert ctx.run_id    == VALID_RID
        assert ctx.source    == "run_id_header"

    @pytest.mark.asyncio
    async def test_no_tenant_no_run_id_returns_400(self):
        resp = await _dispatch({})
        assert resp.status_code == 400
