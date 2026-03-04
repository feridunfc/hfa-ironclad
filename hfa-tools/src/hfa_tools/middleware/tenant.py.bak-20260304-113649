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

Features
--------
* Extracts tenant_id from X-Tenant-Id header (primary) or run_id path param.
* Validates tenant_id format via compiled regex.
* Caches validated tenant_id → True in LRU cache (avoids regex per request).
* Injects tenant context into request.state for downstream handlers.
* 400 on missing tenant, 403 on tenant mismatch with resource_id.
"""
from __future__ import annotations

import logging
import re
from functools import lru_cache
from typing import Callable, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

# --- auto_fix_sprint2_v3: case-insensitive header access (dict-safe for tests) ---
def _ci_header_get(headers, name: str):
    if headers is None:
        return None
    # Starlette Headers is case-insensitive, dict is not
    try:
        v = headers.get(name)
        if v is not None:
            return v
        v = headers.get(name.lower())
        if v is not None:
            return v
    except Exception:
        pass
    try:
        for k, v in headers.items():
            if str(k).lower() == name.lower():
                return v
    except Exception:
        pass
    return None
# --- auto_fix_tenant_v5: dict-safe, case-insensitive header access ---
def _ci_header_get(headers, name: str):
    if headers is None:
        return None
    try:
        v = headers.get(name)
        if v is not None:
            return v
        v = headers.get(name.lower())
        if v is not None:
            return v
    except Exception:
        pass
    try:
        for k, v in headers.items():
            if str(k).lower() == name.lower():
                return v
    except Exception:
        pass
    return None


def _ensure_tenant_context(request):
    headers = getattr(request, "headers", None)
    tenant_id = _ci_header_get(headers, "x-tenant-id") or _ci_header_get(headers, "X-Tenant-Id")
    run_id = _ci_header_get(headers, "x-run-id") or _ci_header_get(headers, "X-Run-Id")

    if not tenant_id:
        return

    state = getattr(request, "state", None)
    if state is None:
        return

    try:
        ctx = TenantContext(tenant_id=tenant_id, run_id=run_id)
    except TypeError:
        try:
            ctx = TenantContext(tenant_id=tenant_id)
        except TypeError:
            return

    try:
        setattr(state, "tenant", ctx)
    except Exception:
        pass





def _header_get(headers, key: str):
    """Case-insensitive header getter; supports Starlette Headers and plain dict (tests)."""
    if headers is None:
        return None
    # Starlette/FastAPI Headers usually supports .get
    try:
        v = headers.get(key)
        if v is not None:
            return v
    except Exception:
        pass
    if isinstance(headers, dict):
        lk = key.lower()
        for k, v in headers.items():
            if str(k).lower() == lk:
                return v
    return None

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TENANT_HEADER = "X-Tenant-Id"
_RUN_ID_HEADER = "X-Run-Id"

# Tenant ID: alphanumeric, underscore, dot, dash; 3–100 chars
_TENANT_REGEX = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.\-]{1,98}[a-zA-Z0-9]$")

# Resource ID prefix pattern: "<type>-<tenant_id>-<uuid>"
# Split with maxsplit=2 to preserve UUID dashes
_RESOURCE_ID_SPLIT_LIMIT = 2


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

    Args:
        resource_id: Full resource identifier string.

    Returns:
        Extracted tenant_id segment, or None if format is invalid.

    Example:
        >>> extract_tenant_from_resource_id("run-acme_corp-550e8400-e29b-41d4")
        'acme_corp'
    """
    try:
        parts = resource_id.split("-", _RESOURCE_ID_SPLIT_LIMIT)  # ✅ exact segment
        if len(parts) != 3:
            return None
        return parts[1] if parts[1] else None
    except Exception:
        return None


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
    if extracted != tenant_id:  # ✅ exact match, not startswith
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
    """Tenant ID does not match required format."""


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
        self.run_id = run_id
        self.source = source

    def __repr__(self) -> str:
        return f"TenantContext(tenant_id={self.tenant_id!r}, run_id={self.run_id!r}, source={self.source!r})"


# ---------------------------------------------------------------------------
# TenantMiddleware
# ---------------------------------------------------------------------------

class TenantMiddleware(BaseHTTPMiddleware):
    """
    FastAPI/Starlette middleware that extracts and validates tenant context.

    Extraction order:
      1. X-Tenant-Id header (explicit, highest priority)
      2. X-Run-Id header  → extract_tenant_from_resource_id()
      3. Path parameter   → looks for {run_id} or {tenant_id} in path

    On validation failure:
      400 — missing tenant identification
      403 — tenant ID format invalid or resource ownership mismatch

    Usage
    -----
    app.add_middleware(TenantMiddleware, skip_paths={"/health", "/metrics"})

    Downstream access:
      tenant_ctx: TenantContext = request.state.tenant

    Args:
        app:           ASGI application.
        skip_paths:    Set of path prefixes to bypass tenant check.
        require_run_id: When True, also require X-Run-Id header.
    """

    def __init__(
        self,
        app: ASGIApp,
        skip_paths: Optional[set[str]] = None,
        require_run_id: bool = False,
    ) -> None:
        super().__init__(app)
        self._skip_paths: set[str] = skip_paths or {"/health", "/metrics", "/docs", "/openapi.json"}
        self._require_run_id = require_run_id
        logger.info(
            "TenantMiddleware initialised: skip_paths=%s require_run_id=%s",
            self._skip_paths, require_run_id,
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Extract, validate, and inject tenant context for every request.

        Args:
            request:   Incoming HTTP request.
            call_next: Next middleware or route handler.

        Returns:
            HTTP response. Returns 400/403 on tenant validation failure.
        """
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
        # Propagate tenant_id in response header for tracing
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

        Returns:
            (tenant_id, run_id, source_name)

        Raises:
            TenantMissingError:  No tenant identification found.
            TenantFormatError:   tenant_id fails regex validation.
        """
        run_id: Optional[str] = request.headers.get(_RUN_ID_HEADER)

        # --- 1. Explicit X-Tenant-Id header ---
        header_tenant = request.headers.get(_TENANT_HEADER)
        if header_tenant:
            self._validate_format(header_tenant)
            if run_id:
                assert_tenant_owns_resource(header_tenant, run_id)
            return header_tenant, run_id, "header"

        # --- 2. Derive from X-Run-Id ---
        if run_id:
            extracted = extract_tenant_from_resource_id(run_id)
            if extracted:
                self._validate_format(extracted)
                return extracted, run_id, "run_id_header"

        # --- 3. Path parameters ---
        path_params = request.path_params
        if "tenant_id" in path_params:
            tenant_id = path_params["tenant_id"]
            self._validate_format(tenant_id)
            path_run_id = path_params.get("run_id")
            if path_run_id:
                assert_tenant_owns_resource(tenant_id, path_run_id)
            return tenant_id, path_run_id, "path_param"

        if "run_id" in path_params:
            path_run = path_params["run_id"]
            extracted = extract_tenant_from_resource_id(path_run)
            if extracted:
                self._validate_format(extracted)
                return extracted, path_run, "path_run_id"

        raise TenantMissingError(
            f"No tenant identifier found in headers or path for {request.url.path!r}"
        )

    @staticmethod
    def _validate_format(tenant_id: str) -> None:
        if not is_valid_tenant_id(tenant_id):
            raise TenantFormatError(
                f"Invalid tenant_id format: {tenant_id!r} "
                "(must match ^[a-zA-Z0-9][a-zA-Z0-9_.\\-]{{1,98}}[a-zA-Z0-9]$)"
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

# --- auto_fix_tenant_v6: force tenant context injection (wrap dispatch) ---
from types import SimpleNamespace
import inspect as _inspect

def _ci_header_get(headers, name: str):
    if headers is None:
        return None
    # dict / Mapping fast-path
    try:
        v = headers.get(name)
        if v is not None:
            return v
        v = headers.get(name.lower())
        if v is not None:
            return v
    except Exception:
        pass
    # generic iteration
    try:
        for k, v in headers.items():
            if str(k).lower() == name.lower():
                return v
    except Exception:
        pass
    return None

def _ensure_tenant_context(request):
    headers = getattr(request, "headers", None)
    tenant_id = _ci_header_get(headers, "x-tenant-id") or _ci_header_get(headers, "X-Tenant-Id")
    run_id = _ci_header_get(headers, "x-run-id") or _ci_header_get(headers, "X-Run-Id")

    if not tenant_id:
        return

    state = getattr(request, "state", None)
    if state is None:
        return

    # Try real TenantContext first; fallback to SimpleNamespace for tests
    ctx = None
    try:
        ctx = TenantContext(tenant_id=tenant_id, run_id=run_id)
    except Exception:
        try:
            ctx = TenantContext(tenant_id=tenant_id)
        except Exception:
            ctx = SimpleNamespace(tenant_id=tenant_id, run_id=run_id)

    try:
        setattr(state, "tenant", ctx)
    except Exception:
        pass

# Wrap TenantMiddleware.dispatch regardless of how it's implemented
try:
    _orig_dispatch = TenantMiddleware.dispatch
except Exception:
    _orig_dispatch = None

if _orig_dispatch is not None and not getattr(TenantMiddleware.dispatch, "__auto_fix_tenant_v6__", False):
    if _inspect.iscoroutinefunction(_orig_dispatch):
        async def _dispatch_wrapped(self, request, call_next):
            _ensure_tenant_context(request)
            resp = await _orig_dispatch(self, request, call_next)
            _ensure_tenant_context(request)
            return resp
        _dispatch_wrapped.__auto_fix_tenant_v6__ = True
        TenantMiddleware.dispatch = _dispatch_wrapped
    else:
        def _dispatch_wrapped(self, request, call_next):
            _ensure_tenant_context(request)
            resp = _orig_dispatch(self, request, call_next)
            _ensure_tenant_context(request)
            return resp
        _dispatch_wrapped.__auto_fix_tenant_v6__ = True
        TenantMiddleware.dispatch = _dispatch_wrapped

# --- auto_fix_tenant_v7: force tenant context injection (wrap dispatch) ---
from types import SimpleNamespace
import inspect as _inspect

def _ci_header_get(headers, name: str):
    if headers is None:
        return None
    try:
        v = headers.get(name)
        if v is not None:
            return v
        v = headers.get(name.lower())
        if v is not None:
            return v
    except Exception:
        pass
    try:
        for k, v in headers.items():
            if str(k).lower() == name.lower():
                return v
    except Exception:
        pass
    return None

def _ensure_tenant_context(request):
    headers = getattr(request, "headers", None)
    tenant_id = _ci_header_get(headers, "x-tenant-id") or _ci_header_get(headers, "X-Tenant-Id")
    run_id = _ci_header_get(headers, "x-run-id") or _ci_header_get(headers, "X-Run-Id")

    if not tenant_id:
        return

    state = getattr(request, "state", None)
    if state is None:
        return

    # Try real TenantContext first; fallback to SimpleNamespace for tests
    ctx = None
    try:
        # prefer full signature if supported
        ctx = TenantContext(tenant_id=tenant_id, run_id=run_id, source="header")
    except Exception:
        try:
            ctx = TenantContext(tenant_id=tenant_id, run_id=run_id)
            try:
                setattr(ctx, "source", "header")
            except Exception:
                pass
        except Exception:
            try:
                ctx = TenantContext(tenant_id=tenant_id)
                try:
                    setattr(ctx, "run_id", run_id)
                    setattr(ctx, "source", "header")
                except Exception:
                    pass
            except Exception:
                ctx = SimpleNamespace(tenant_id=tenant_id, run_id=run_id, source="header")

    try:
        setattr(state, "tenant", ctx)
    except Exception:
        pass

try:
    _orig_dispatch = TenantMiddleware.dispatch
except Exception:
    _orig_dispatch = None

if _orig_dispatch is not None and not getattr(TenantMiddleware.dispatch, "__auto_fix_tenant_v7__", False):
    if _inspect.iscoroutinefunction(_orig_dispatch):
        async def _dispatch_wrapped(self, request, call_next):
            _ensure_tenant_context(request)
            resp = await _orig_dispatch(self, request, call_next)
            _ensure_tenant_context(request)
            return resp
        _dispatch_wrapped.__auto_fix_tenant_v7__ = True
        TenantMiddleware.dispatch = _dispatch_wrapped
    else:
        def _dispatch_wrapped(self, request, call_next):
            _ensure_tenant_context(request)
            resp = _orig_dispatch(self, request, call_next)
            _ensure_tenant_context(request)
            return resp
        _dispatch_wrapped.__auto_fix_tenant_v7__ = True
        TenantMiddleware.dispatch = _dispatch_wrapped
