"""
tests/test_tenant_patches.py
IRONCLAD — Tenant middleware patch tests (Fixed imports)

Covers:
- _header_get(): case-insensitive header lookup
- validate_run_id_format(): strict run-id format with UUIDv4
- TenantFormatError exceptions
"""
import uuid
import pytest
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime

from hfa_tools.middleware.tenant import (
    TenantMiddleware,
    _header_get,
    validate_run_id_format,
    TenantFormatError,
    ParsedRunId,
)


class TestHeaderGet:
    """Test case-insensitive header lookup function."""

    def test_header_get_dict_case_insensitive(self):
        """Test dict header lookup is case-insensitive."""
        headers = {
            "X-Tenant-Id": "acme",
            "x-run-id": "run-acme-550e8400-e29b-41d4-a716-446655440000",
            "CONTENT-TYPE": "application/json",
        }

        assert _header_get(headers, "x-tenant-id") == "acme"
        assert _header_get(headers, "X-RUN-ID") == headers["x-run-id"]
        assert _header_get(headers, "content-type") == "application/json"

    def test_header_get_starlette_headers_case_insensitive(self):
        """Test Starlette Headers object is case-insensitive."""
        # Mock Starlette Headers
        class MockHeaders:
            def __init__(self, headers):
                self._headers = headers

            def get(self, key):
                # Starlette Headers are case-insensitive internally
                key_lower = key.lower()
                for k, v in self._headers.items():
                    if k.lower() == key_lower:
                        return v
                return None

        headers = MockHeaders({
            "X-Tenant-Id": "acme",
            "X-Run-Id": "run-acme-550e8400-e29b-41d4-a716-446655440000",
        })

        assert _header_get(headers, "x-tenant-id") == "acme"
        assert _header_get(headers, "X-RUN-ID") == "run-acme-550e8400-e29b-41d4-a716-446655440000"

    def test_header_get_missing_returns_none(self):
        """Test missing header returns None."""
        assert _header_get({}, "x-tenant-id") is None
        assert _header_get(None, "x-tenant-id") is None
        assert _header_get({"x-other": "value"}, "x-tenant-id") is None


class TestRunIdFormatEnforcement:
    """Test strict run-id format validation."""

    def test_validate_run_id_format_valid_uuid4(self):
        """Test valid run-id with UUIDv4 passes validation."""
        tenant = "acme_corp"
        valid_uuid = uuid.uuid4()
        run_id = f"run-{tenant}-{valid_uuid}"

        parsed = validate_run_id_format(run_id, expected_tenant_id=tenant)

        assert isinstance(parsed, ParsedRunId)
        assert parsed.prefix == "run"
        assert parsed.tenant_id == tenant
        assert parsed.uuid == str(valid_uuid)
        assert parsed.uuid_version == 4

    def test_validate_run_id_with_delimiter_in_tenant(self):
        """Test tenant_id with hyphens works correctly (split on first 2)."""
        tenant = "acme-prod-us"
        valid_uuid = uuid.uuid4()
        run_id = f"run-{tenant}-{valid_uuid}"

        parsed = validate_run_id_format(run_id, expected_tenant_id=tenant)

        assert parsed.tenant_id == tenant
        assert parsed.prefix == "run"

    def test_validate_run_id_format_rejects_wrong_prefix(self):
        """Test run-id with wrong prefix raises TenantFormatError."""
        tenant = "acme_corp"
        run_id = f"job-{tenant}-{uuid.uuid4()}"

        with pytest.raises(TenantFormatError, match="prefix"):
            validate_run_id_format(run_id, expected_tenant_id=tenant)

    def test_validate_run_id_format_rejects_tenant_mismatch(self):
        """Test run-id with wrong tenant_id raises TenantFormatError."""
        tenant = "acme_corp"
        run_id = f"run-evilcorp-{uuid.uuid4()}"

        with pytest.raises(TenantFormatError, match="tenant"):
            validate_run_id_format(run_id, expected_tenant_id=tenant)

    def test_validate_run_id_format_rejects_non_uuid(self):
        """Test run-id with invalid UUID format raises TenantFormatError."""
        tenant = "acme_corp"
        run_id = f"run-{tenant}-not-a-uuid"

        with pytest.raises(TenantFormatError, match="UUID"):
            validate_run_id_format(run_id, expected_tenant_id=tenant)

    def test_validate_run_id_format_rejects_nil_uuid(self):
        """Test nil UUID (all zeros) is rejected."""
        tenant = "acme_corp"
        nil_uuid = "00000000-0000-0000-0000-000000000000"
        run_id = f"run-{tenant}-{nil_uuid}"

        with pytest.raises(TenantFormatError, match="nil UUID"):
            validate_run_id_format(run_id, expected_tenant_id=tenant)

    def test_validate_run_id_format_rejects_non_v4(self):
        """Test UUID version other than 4 is rejected."""
        tenant = "acme_corp"
        # UUIDv1 example
        v1_uuid = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
        run_id = f"run-{tenant}-{v1_uuid}"

        with pytest.raises(TenantFormatError, match="version 4"):
            validate_run_id_format(run_id, expected_tenant_id=tenant)

    def test_validate_run_id_format_insufficient_parts(self):
        """Test run-id with too few parts raises error."""
        run_id = "run-acme_corp"  # Missing UUID

        with pytest.raises(TenantFormatError, match="format"):
            validate_run_id_format(run_id, expected_tenant_id="acme_corp")

    def test_validate_run_id_format_too_many_parts(self):
        """Test run-id with extra hyphens still works (split limit)."""
        tenant = "acme_corp"
        uuid_val = uuid.uuid4()
        run_id = f"run-{tenant}-{uuid_val}-extra"  # Extra part

        # Should still parse correctly because we split on first 2 hyphens
        parsed = validate_run_id_format(run_id, expected_tenant_id=tenant)
        assert parsed.tenant_id == tenant
        assert parsed.uuid == f"{uuid_val}-extra"  # UUID + extra joined


class TestTenantMiddleware:
    """Test TenantMiddleware integration."""

    @pytest.fixture
    def middleware(self):
        """Create TenantMiddleware instance."""
        return TenantMiddleware(app=MagicMock())

    @pytest.mark.asyncio
    async def test_middleware_extracts_tenant_id(self, middleware):
        """Test middleware extracts tenant_id from header."""
        # Mock request
        request = AsyncMock()
        request.headers = {"x-tenant-id": "acme_corp"}
        request.method = "GET"
        request.url.path = "/v1/execute"

        # Mock response
        call_next = AsyncMock()

        # Call dispatch
        response = await middleware.dispatch(request, call_next)

        # Verify tenant_id set in request state
        assert request.state.tenant_id == "acme_corp"

    @pytest.mark.asyncio
    async def test_middleware_rejects_missing_tenant(self, middleware):
        """Test middleware rejects requests without tenant header."""
        request = AsyncMock()
        request.headers = {}
        request.method = "GET"

        call_next = AsyncMock()

        with pytest.raises(Exception) as exc:  # Should raise HTTPException
            await middleware.dispatch(request, call_next)

        assert "X-Tenant-ID" in str(exc.value)

    @pytest.mark.asyncio
    async def test_middleware_validates_tenant_characters(self, middleware):
        """Test middleware rejects tenant_id with invalid characters."""
        request = AsyncMock()
        request.headers = {"x-tenant-id": "acme@corp$"}  # Invalid chars
        request.method = "GET"

        call_next = AsyncMock()

        with pytest.raises(Exception) as exc:
            await middleware.dispatch(request, call_next)

        assert "invalid characters" in str(exc.value).lower()

    @pytest.mark.asyncio
    async def test_middleware_verifies_ownership_post(self, middleware):
        """Test middleware verifies ownership for POST requests."""
        # Mock request
        request = AsyncMock()
        request.headers = {"x-tenant-id": "acme_corp"}
        request.method = "POST"
        request.url.path = "/v1/execute"

        # Mock body with run_id
        request.body = AsyncMock(return_value=b'{"run_id": "run-acme_corp-550e8400-e29b-41d4-a716-446655440000"}')

        # Mock ownership check to succeed
        middleware._check_ownership = AsyncMock(return_value=True)

        call_next = AsyncMock()

        # Call dispatch
        response = await middleware.dispatch(request, call_next)

        # Verify ownership check called
        middleware._check_ownership.assert_called_once()

    @pytest.mark.asyncio
    async def test_middleware_rejects_wrong_tenant_post(self, middleware):
        """Test middleware rejects POST with wrong tenant in body."""
        # Mock request
        request = AsyncMock()
        request.headers = {"x-tenant-id": "acme_corp"}
        request.method = "POST"

        # Mock body with run_id from different tenant
        request.body = AsyncMock(return_value=b'{"run_id": "run-evilcorp-550e8400-e29b-41d4-a716-446655440000"}')

        # Mock ownership check to fail
        middleware._check_ownership = AsyncMock(return_value=False)

        call_next = AsyncMock()

        with pytest.raises(Exception) as exc:
            await middleware.dispatch(request, call_next)

        assert "does not belong to tenant" in str(exc.value)