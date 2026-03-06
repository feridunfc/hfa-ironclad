"""
hfa-tools/src/hfa_tools/middleware/tenant.py
IRONCLAD — Tenant middleware with proper exports
"""
from dataclasses import dataclass
import re
import uuid
from typing import Optional, Dict, Any, Union
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

# ============================================================================
# Public exports - Bunlar testler tarafından import ediliyor
# ============================================================================

@dataclass
class ParsedRunId:
    """Parsed run ID components."""
    prefix: str
    tenant_id: str
    uuid: str
    uuid_version: int


class TenantFormatError(Exception):
    """Raised when tenant/run ID format is invalid."""
    pass


def _header_get(headers: Union[Dict, Any], key: str) -> Optional[str]:
    """
    Case-insensitive header lookup.

    Args:
        headers: Dict or Starlette Headers object
        key: Header name (case-insensitive)

    Returns:
        Header value or None
    """
    if not headers:
        return None

    key_lower = key.lower()

    # Dict lookup
    if isinstance(headers, dict):
        for k, v in headers.items():
            if k.lower() == key_lower:
                return v
        return None

    # Starlette Headers object
    if hasattr(headers, 'get'):
        return headers.get(key)

    return None


def validate_run_id_format(run_id: str, expected_tenant_id: str) -> ParsedRunId:
    """
    Validate run-id format: {prefix}-{tenant_id}-{uuid4}

    Args:
        run_id: Run ID to validate
        expected_tenant_id: Expected tenant ID

    Returns:
        ParsedRunId components

    Raises:
        TenantFormatError: If format is invalid
    """
    parts = run_id.split('-', 2)
    if len(parts) < 3:
        raise TenantFormatError(f"Invalid run-id format (expected 3 parts): {run_id}")

    prefix, tenant, uuid_str = parts[0], parts[1], parts[2]

    # Check prefix
    if prefix != "run":
        raise TenantFormatError(f"Invalid prefix (expected 'run', got '{prefix}')")

    # Check tenant match
    if tenant != expected_tenant_id:
        raise TenantFormatError(
            f"Tenant mismatch: '{tenant}' != '{expected_tenant_id}'"
        )

    # Validate UUID
    try:
        u = uuid.UUID(uuid_str)
        if u.version != 4:
            raise TenantFormatError(f"UUID version {u.version} != 4")
        if u.int == 0:
            raise TenantFormatError("Nil UUID not allowed")
    except ValueError as e:
        raise TenantFormatError(f"Invalid UUID: {uuid_str}") from e

    return ParsedRunId(
        prefix=prefix,
        tenant_id=tenant,
        uuid=uuid_str,
        uuid_version=4
    )


# ============================================================================
# Tenant Middleware
# ============================================================================

class TenantMiddleware(BaseHTTPMiddleware):
    """
    Middleware for tenant isolation.

    Features:
    - Extracts tenant ID from X-Tenant-ID header
    - Validates tenant ID format
    - Verifies resource ownership for POST requests
    """

    def __init__(self, app):
        super().__init__(app)
        self._tenant_pattern = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_.-]{2,98}[a-zA-Z0-9]$')

    async def dispatch(self, request: Request, call_next):
        # Extract tenant ID from header
        tenant_id = _header_get(request.headers, "x-tenant-id")
        if not tenant_id:
            raise HTTPException(400, "X-Tenant-ID header required")

        # Validate tenant ID format
        if not self._tenant_pattern.match(tenant_id):
            raise HTTPException(400, "X-Tenant-ID contains invalid characters")

        # Store in request state
        request.state.tenant_id = tenant_id

        # For POST requests, verify ownership
        if request.method == "POST":
            await self._verify_ownership(request, tenant_id)

        # Process request
        response = await call_next(request)
        response.headers["X-Tenant-ID"] = tenant_id
        return response

    async def _verify_ownership(self, request: Request, tenant_id: str):
        """Verify that resources in request body belong to tenant."""
        try:
            body = await request.json()
        except Exception:
            return  # No JSON body, skip ownership check

        # Check run_id fields
        for field in ["run_id", "plan_id", "code_set_id"]:
            if field in body:
                resource_id = body[field]
                if resource_id and not await self._check_ownership(resource_id, tenant_id):
                    raise HTTPException(
                        403,
                        f"{field} '{resource_id}' does not belong to tenant '{tenant_id}'"
                    )

    async def _check_ownership(self, resource_id: str, tenant_id: str) -> bool:
        """Check if resource belongs to tenant."""
        try:
            # Try to parse as run-id
            if resource_id.startswith("run-"):
                parsed = validate_run_id_format(resource_id, tenant_id)
                return parsed.tenant_id == tenant_id
        except TenantFormatError:
            pass

        # For other resource types, check prefix
        if resource_id.startswith(f"plan-{tenant_id}-"):
            return True
        if resource_id.startswith(f"code-{tenant_id}-"):
            return True

        return False


# ============================================================================
# Export all public symbols
# ============================================================================

__all__ = [
    "TenantMiddleware",
    "ParsedRunId",
    "TenantFormatError",
    "_header_get",
    "validate_run_id_format",
]