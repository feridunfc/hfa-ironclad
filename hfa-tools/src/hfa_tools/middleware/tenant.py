"""
hfa-tools/src/hfa_tools/middleware/tenant.py
IRONCLAD — Tenant extraction, validation, and request-scoped injection.

Tenant ID format (CRITICAL)
---------------------------
  resource_id = "run-acme_corp-550e8400-e29b-41d4-a716-446655440000"
  parts = resource_id.split("-", 2)
  # → ["run", "acme_corp", "550e8400-e29b-41d4-a716-446655440000"]
  assert parts[1] == tenant_id   # ✅ exact segment match, not startswith

The '-' delimiter is non-negotiable. Any '_' delimiter would allow
"run_evilcorp_acme_corp_uuid" to bypass tenant checks.

Patches in this revision
------------------------
Patch 1 — _header_get(): case-insensitive header access
  HTTP spec (RFC 7230) requires header names to be treated
  case-insensitively.  Starlette Headers does this automatically for real
  requests, but plain dict mocks (unit tests) do NOT.  _header_get()
  normalises header names to lowercase before lookup, so "X-Tenant-Id",
  "x-tenant-id", and "X-TENANT-ID" all resolve correctly in every context.

Patch 2 — validate_run_id_format() enforced on every run_id path
  All three extraction branches (header, run_id header, path params) now
  call validate_run_id_format() unconditionally.  There is no fallback to
  the old extract_tenant_from_resource_id() inside _extract_tenant().

Patch 3 — UUIDv4 enforcement in validate_run_id_format()
  uuid.UUID() accepts v1–v5 and the nil UUID.  IRONCLAD run_ids must use
  UUIDv4 for sufficient entropy.  The function now checks:
    - u.version == 4          (must be v4)
    - u.int != 0              (nil UUID "00000000-…" rejected)
  Callers receive a TenantFormatError with a clear message on violation.
"""
from __future__ import annotations

import logging
import re
import uuid
from functools import lru_cache
from typing import Callable, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Canonical header names — always compared in lowercase (see _header_get)
_TENANT_HEADER = "x-tenant-id"
_RUN_ID_HEADER = "x-run-id"

# Tenant ID: alphanumeric, underscore, dot, dash; 3–100 chars
_TENANT_REGEX = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.\-]{1,98}[a-zA-Z0-9]$")

_RESOURCE_ID_SPLIT_LIMIT = 2


# ---------------------------------------------------------------------------
# Patch 1 — case-insensitive header helper
# ---------------------------------------------------------------------------

def _header_get(headers, name: str) -> Optional[str]:
    """
    Case-insensitive header lookup compatible with both Starlette Headers
    and plain dict mocks used in unit tests.

    HTTP spec (RFC 7230 §3.2) mandates case-insensitive field names.
    Starlette Headers implements this natively; dict mocks do not.
    This helper normalises ``name`` to lowercase and falls back to an
    exhaustive case-insensitive scan so unit tests using plain dicts work.

    Args:
        headers: Starlette Headers object **or** any dict-like mapping.
        name:    Header name (any casing: "X-Tenant-Id", "x-tenant-id", …).

    Returns:
        Header value string, or None if not present.

    Examples:
        >>> _header_get({"x-tenant-id": "acme"}, "X-Tenant-Id")
        'acme'
        >>> _header_get({"X-Tenant-Id": "acme"}, "x-tenant-id")
        'acme'
    """
    lower_name = name.lower()

    # Fast path: Starlette Headers / MutableHeaders are already case-insensitive.
    # Standard dict keys are case-sensitive — try the lowercase key first.
    value = headers.get(lower_name)
    if value is not None:
        return value

    # Slow path: plain dict with mixed-case keys (unit tests).
    for key, val in headers.items():
        if key.lower() == lower_name:
            return val

    return None


# ---------------------------------------------------------------------------
# Pure validation helpers (no I/O — safe to LRU-cache)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=4096)
def is_valid_tenant_id(tenant_id: str) -> bool:
    """
    Validate tenant_id format using compiled regex.
    Result is LRU-cached — subsequent calls for the same ID are O(1).

    Args:
        tenant_id: Candidate tenant identifier string.

    Returns:
        True if valid, False otherwise.
    """
    return bool(_TENANT_REGEX.fullmatch(tenant_id))


def extract_tenant_from_resource_id(resource_id: str) -> Optional[str]:
    """
    Extract tenant_id from a resource_id using the canonical '-' delimiter.

    Format: "<resource_type>-<tenant_id>-<uuid>"
    Split: resource_id.split("-", 2) → [type, tenant_id, uuid_tail]

    Note: This function performs NO UUID validation — use
    validate_run_id_format() when UUID entropy is required.

    Args:
        resource_id: Full resource identifier string.

    Returns:
        Extracted tenant_id segment, or None if format is invalid.

    Example:
        >>> extract_tenant_from_resource_id("run-acme_corp-550e8400-e29b-41d4")
        'acme_corp'
    """
    try:
        parts = resource_id.split("-", _RESOURCE_ID_SPLIT_LIMIT)
        if len(parts) != 3:
            return None
        return parts[1] if parts[1] else None
    except Exception:
        return None


def validate_run_id_format(run_id: str) -> tuple[str, str]:
    """
    Enforce IRONCLAD run_id format: ``run-<tenant_id>-<uuidv4>``.

    Patch 2 — called on every run_id path (header, path param).
    Patch 3 — enforces UUIDv4; rejects nil UUID and all other versions.

    Checks (in order):
      1. Format:  exactly "run-<tenant>-<tail>" (3 dash-segments).
      2. Version: tail must be a UUIDv4 (u.version == 4).
      3. Non-nil: rejects the nil UUID (all-zeros).
      4. Tenant:  tenant segment must pass is_valid_tenant_id().

    Args:
        run_id: Candidate run identifier.

    Returns:
        (tenant_id, normalised_uuid_str) — both validated.

    Raises:
        TenantFormatError: On any validation failure.

    Example:
        >>> validate_run_id_format("run-acme_corp-550e8400-e29b-41d4-a716-446655440000")
        ('acme_corp', '550e8400-e29b-41d4-a716-446655440000')
    """
    # ── 1. Structure check ────────────────────────────────────────────────
    parts = run_id.split("-", 2)
    if len(parts) != 3 or parts[0] != "run":
        raise TenantFormatError(
            f"Invalid run_id format: {run_id!r} "
            "(expected 'run-<tenant_id>-<uuidv4>')"
        )
    tenant_seg = parts[1]
    uuid_tail  = parts[2]

    # ── 2. UUID parse ────────────────────────────────────────────────────
    try:
        parsed = uuid.UUID(uuid_tail)
    except (ValueError, AttributeError) as exc:
        raise TenantFormatError(
            f"run_id UUID tail is not a valid UUID in {run_id!r}: {exc}"
        ) from exc

    # ── 3. Patch 3: UUIDv4 enforcement ──────────────────────────────────
    if parsed.version != 4:
        raise TenantFormatError(
            f"run_id UUID tail must be UUIDv4 (got version {parsed.version}) "
            f"in {run_id!r}"
        )

    # ── 4. Nil UUID guard ────────────────────────────────────────────────
    if parsed.int == 0:
        raise TenantFormatError(
            f"run_id UUID tail must not be the nil UUID in {run_id!r}"
        )

    # ── 5. Tenant segment validation ────────────────────────────────────
    if not is_valid_tenant_id(tenant_seg):
        raise TenantFormatError(
            f"Invalid tenant segment in run_id: {run_id!r}"
        )

    return tenant_seg, str(parsed)


def assert_tenant_owns_resource(tenant_id: str, resource_id: str) -> None:
    """
    Assert that the authenticated tenant owns the given resource.

    Uses exact segment comparison — NOT startswith() — to prevent
    tenant bypass via crafted resource IDs.

    Args:
        tenant_id:   Authenticated tenant identifier.
        resource_id: Resource identifier to validate ownership of.

    Raises:
        TenantMismatchError: If the resource does not belong to the tenant.
        TenantFormatError:   If the resource_id format is invalid.
    """
    extracted = extract_tenant_from_resource_id(resource_id)
    if extracted is None:
        raise TenantFormatError(
            f"Cannot extract tenant from resource_id={resource_id!r}"
        )
    if extracted != tenant_id:
        raise TenantMismatchError(
            f"Tenant mismatch: authenticated={tenant_id!r} "
            f"resource_owner={extracted!r} resource_id={resource_id!r}"
        )


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class TenantError(Exception):
    """Base class for tenant middleware errors."""


class TenantMissingError(TenantError):
    """Request has no tenant identification."""


class TenantFormatError(TenantError):
    """Tenant ID or run_id does not match required format."""


class TenantMismatchError(TenantError):
    """Authenticated tenant does not match resource owner."""


# ---------------------------------------------------------------------------
# Request-scoped TenantContext
# ---------------------------------------------------------------------------

class TenantContext:
    """
    Tenant context injected into request.state by the middleware.

    Attributes:
        tenant_id:  Validated tenant identifier.
        run_id:     Optional run identifier (from header or path param).
        source:     Where the tenant_id was extracted from.
    """

    __slots__ = ("tenant_id", "run_id", "source")

    def __init__(self, tenant_id: str, run_id: Optional[str], source: str) -> None:
        self.tenant_id = tenant_id
        self.run_id    = run_id
        self.source    = source

    def __repr__(self) -> str:
        return (
            f"TenantContext(tenant_id={self.tenant_id!r}, "
            f"run_id={self.run_id!r}, source={self.source!r})"
        )


# ---------------------------------------------------------------------------
# TenantMiddleware
# ---------------------------------------------------------------------------

class TenantMiddleware(BaseHTTPMiddleware):
    """
    FastAPI/Starlette middleware that extracts and validates tenant context.

    Extraction order (Patch 2: validate_run_id_format enforced on all paths):
      1. X-Tenant-Id header (explicit, highest priority)
         — if X-Run-Id also present, its tenant segment must match.
      2. X-Run-Id header → validate_run_id_format() → tenant segment.
      3. Path param {tenant_id} + optional {run_id} validated by format.
      4. Path param {run_id} alone → validate_run_id_format().

    All header reads go through _header_get() for case-insensitive access
    (Patch 1) so dict mocks in tests behave identically to Starlette Headers.

    On validation failure:
      400 — missing tenant identification
      403 — format invalid, UUID wrong version, or tenant mismatch

    Usage
    -----
    app.add_middleware(TenantMiddleware, skip_paths={"/health", "/metrics"})

    Downstream access:
      tenant_ctx: TenantContext = request.state.tenant

    Middleware stack order (FastAPI add_middleware — last-added runs first):
      app.add_middleware(TenantRateLimitMiddleware, redis=r)  # 2nd — reads state.tenant
      app.add_middleware(TenantMiddleware)                     # 1st — writes state.tenant

    Args:
        app:            ASGI application.
        skip_paths:     Set of path prefixes to bypass tenant check.
        require_run_id: When True, also require X-Run-Id header or path param.
    """

    def __init__(
        self,
        app: ASGIApp,
        skip_paths: Optional[set[str]] = None,
        require_run_id: bool = False,
    ) -> None:
        super().__init__(app)
        self._skip_paths: set[str] = (
            skip_paths or {"/health", "/metrics", "/docs", "/openapi.json"}
        )
        self._require_run_id = require_run_id
        logger.info(
            "TenantMiddleware initialised: skip_paths=%s require_run_id=%s",
            self._skip_paths, require_run_id,
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip health/metrics endpoints
        for skip in self._skip_paths:
            if request.url.path.startswith(skip):
                return await call_next(request)

        try:
            tenant_id, run_id, source = self._extract_tenant(request)
        except TenantMissingError as exc:
            logger.warning("Tenant missing: %s path=%s", exc, request.url.path)
            return Response(
                content=f"Missing tenant identification: {exc}",
                status_code=400,
            )
        except (TenantFormatError, TenantMismatchError) as exc:
            logger.warning("Tenant validation failed: %s", exc)
            return Response(
                content=f"Tenant validation failed: {exc}",
                status_code=403,
            )
        except Exception as exc:
            logger.error("Unexpected error in TenantMiddleware: %s", exc, exc_info=True)
            return Response(content="Internal server error", status_code=500)

        if self._require_run_id and not run_id:
            logger.warning("run_id required but missing: path=%s", request.url.path)
            return Response(content="X-Run-Id header required", status_code=400)

        request.state.tenant = TenantContext(
            tenant_id=tenant_id,
            run_id=run_id,
            source=source,
        )
        logger.debug("Tenant context injected: %r", request.state.tenant)

        response = await call_next(request)
        response.headers["X-Tenant-Id"] = tenant_id
        return response

    # ------------------------------------------------------------------
    # Private extraction logic
    # ------------------------------------------------------------------

    def _extract_tenant(
        self,
        request: Request,
    ) -> tuple[str, Optional[str], str]:
        """
        Try each extraction source in priority order.

        Patch 1: All header reads use _header_get() (case-insensitive).
        Patch 2: validate_run_id_format() called on every run_id path.

        Returns:
            (tenant_id, run_id, source_name)

        Raises:
            TenantMissingError:  No tenant identification found.
            TenantFormatError:   Format/UUID validation failed.
            TenantMismatchError: run_id tenant does not match header tenant.
        """
        # ✅ Patch 1: case-insensitive header reads
        run_id: Optional[str] = _header_get(request.headers, _RUN_ID_HEADER)

        # --- 1. Explicit X-Tenant-Id header ---
        header_tenant = _header_get(request.headers, _TENANT_HEADER)
        if header_tenant:
            self._validate_format(header_tenant)
            if run_id:
                # ✅ Patch 2: validate_run_id_format mandatory on run_id header
                # ✅ Patch 3: UUID version check inside validate_run_id_format
                extracted, _ = validate_run_id_format(run_id)
                if extracted != header_tenant:
                    raise TenantMismatchError(
                        f"X-Tenant-Id {header_tenant!r} does not match "
                        f"run_id owner {extracted!r}"
                    )
            return header_tenant, run_id, "header"

        # --- 2. Derive tenant from X-Run-Id (UUID entropy + version enforced) ---
        if run_id:
            # ✅ Patch 2 + 3: validate format AND UUIDv4 in one call
            extracted, _ = validate_run_id_format(run_id)
            self._validate_format(extracted)
            return extracted, run_id, "run_id_header"

        # --- 3. Path parameters ---
        path_params = request.path_params
        if "tenant_id" in path_params:
            tenant_id   = path_params["tenant_id"]
            self._validate_format(tenant_id)
            path_run_id = path_params.get("run_id")
            if path_run_id:
                # ✅ Patch 2 + 3: format + UUIDv4 on path run_id
                extracted, _ = validate_run_id_format(path_run_id)
                if extracted != tenant_id:
                    raise TenantMismatchError(
                        f"Path tenant_id {tenant_id!r} does not match "
                        f"run_id owner {extracted!r}"
                    )
            return tenant_id, path_run_id, "path_param"

        if "run_id" in path_params:
            path_run = path_params["run_id"]
            # ✅ Patch 2 + 3: format + UUIDv4 on sole path run_id
            extracted, _ = validate_run_id_format(path_run)
            self._validate_format(extracted)
            return extracted, path_run, "path_run_id"

        raise TenantMissingError(
            f"No tenant identifier found in headers or path "
            f"for {request.url.path!r}"
        )

    @staticmethod
    def _validate_format(tenant_id: str) -> None:
        if not is_valid_tenant_id(tenant_id):
            raise TenantFormatError(
                f"Invalid tenant_id format: {tenant_id!r} "
                r"(must match ^[a-zA-Z0-9][a-zA-Z0-9_.\-]{1,98}[a-zA-Z0-9]$)"
            )


# ---------------------------------------------------------------------------
# FastAPI dependency helper
# ---------------------------------------------------------------------------

def get_tenant_context(request: Request) -> TenantContext:
    """
    FastAPI dependency: inject TenantContext from request.state.

    Usage:
        @router.get("/plans")
        async def list_plans(tenant: TenantContext = Depends(get_tenant_context)):
            ...

    Raises:
        RuntimeError: If TenantMiddleware was not installed (programming error).
    """
    ctx = getattr(request.state, "tenant", None)
    if ctx is None:
        raise RuntimeError(
            "TenantContext not found on request.state — "
            "ensure TenantMiddleware is installed before using get_tenant_context()"
        )
    return ctx
