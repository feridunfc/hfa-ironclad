"""
hfa-tools/src/hfa_tools/middleware/tenant.py

IRONCLAD — Tenant extraction, validation, and request-scoped injection.

Tenant ID format (CRITICAL)
---------------------------
resource_id = "run-acme_corp-550e8400-e29b-41d4-a716-446655440000"
parts = resource_id.split("-", 2)  # → ["run", "acme_corp", "uuid..."]
assert parts[1] == tenant_id  # ✅ exact segment match, not startswith

The '-' delimiter is non-negotiable.
Any '_' delimiter would allow "run_evilcorp_acme_corp_uuid" to bypass tenant checks.

Features
--------
* Extracts tenant_id from X-Tenant-Id header (primary) or run_id header/path.
* Validates tenant_id format via compiled regex (LRU cached).
* Injects TenantContext into request.state.tenant.
* 400 on missing tenant, 403 on format/mismatch.
"""

from __future__ import annotations

import logging
import re
from functools import lru_cache
from typing import Callable, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

logger = logging.getLogger(__name__)

_TENANT_HEADER = "X-Tenant-Id"
_RUN_ID_HEADER = "X-Run-Id"

# Tenant ID: alphanumeric, underscore, dot, dash; 3–100 chars (start/end alnum)
_TENANT_REGEX = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.\\-]{1,98}[a-zA-Z0-9]$")
_RESOURCE_ID_SPLIT_LIMIT = 2  # split("-", 2) => 3 parts


def _header_get(headers, key: str):
    """
    Case-insensitive header getter; supports Starlette Headers and plain dict (tests).
    """
    if headers is None:
        return None

    # Starlette Headers supports .get with case-insensitivity; dict doesn't.
    try:
        v = headers.get(key)
        if v is not None:
            return v
    except Exception:
        pass

    # dict / mapping fallback (case-insensitive)
    try:
        items = headers.items()
    except Exception:
        return None

    lk = str(key).lower()
    for k, v in items:
        if str(k).lower() == lk:
            return v
    return None


@lru_cache(maxsize=4096)
def is_valid_tenant_id(tenant_id: str) -> bool:
    return bool(_TENANT_REGEX.fullmatch(tenant_id))


def extract_tenant_from_resource_id(resource_id: str) -> Optional[str]:
    """
    Extract tenant_id from resource_id with canonical '-' delimiter.

    Format: <type>-<tenant_id>-<uuid_tail>
    resource_id.split("-", 2) -> [type, tenant_id, uuid_tail]
    """
    try:
        parts = resource_id.split("-", _RESOURCE_ID_SPLIT_LIMIT)
        if len(parts) != 3:
            return None
        return parts[1] or None
    except Exception:
        return None


def assert_tenant_owns_resource(tenant_id: str, resource_id: str) -> None:
    extracted = extract_tenant_from_resource_id(resource_id)
    if extracted is None:
        raise TenantFormatError(f"Cannot extract tenant from resource_id={resource_id!r}")
    if extracted != tenant_id:
        raise TenantMismatchError(
            f"Tenant mismatch: authenticated={tenant_id!r} "
            f"resource_owner={extracted!r} resource_id={resource_id!r}"
        )


class TenantError(Exception):
    """Base class for tenant middleware errors."""


class TenantMissingError(TenantError):
    """Request has no tenant identification."""


class TenantFormatError(TenantError):
    """Tenant ID does not match required format."""


class TenantMismatchError(TenantError):
    """Authenticated tenant does not match resource owner."""


class TenantContext:
    """
    Tenant context injected into request.state by the middleware.

    Attributes:
      tenant_id: Validated tenant identifier.
      run_id: Optional run identifier (from header or path param).
      source: Where tenant_id was extracted from ("header", "run_id_header", "path_param", "path_run_id").
    """

    __slots__ = ("tenant_id", "run_id", "source")

    def __init__(self, tenant_id: str, run_id: Optional[str], source: str) -> None:
        self.tenant_id = tenant_id
        self.run_id = run_id
        self.source = source

    def __repr__(self) -> str:
        return (
            f"TenantContext(tenant_id={self.tenant_id!r}, "
            f"run_id={self.run_id!r}, source={self.source!r})"
        )


class TenantMiddleware(BaseHTTPMiddleware):
    """
    FastAPI/Starlette middleware that extracts and validates tenant context.

    Extraction order:
      1) X-Tenant-Id header
      2) X-Run-Id header -> derive tenant segment
      3) Path params: tenant_id / run_id

    On failure:
      400 missing tenant
      403 invalid tenant format or mismatch
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
            self._skip_paths,
            require_run_id,
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        for skip in self._skip_paths:
            if request.url.path.startswith(skip):
                return await call_next(request)

        try:
            tenant_id, run_id, source = self._extract_tenant(request)
        except TenantMissingError as exc:
            logger.warning("Tenant missing: %s path=%s", exc, request.url.path)
            return Response(content=f"Missing tenant identification: {exc}", status_code=400)
        except (TenantFormatError, TenantMismatchError) as exc:
            logger.warning("Tenant validation failed: %s", exc)
            return Response(content=f"Tenant validation failed: {exc}", status_code=403)
        except Exception as exc:
            logger.error("Unexpected error in TenantMiddleware: %s", exc, exc_info=True)
            return Response(content="Internal server error", status_code=500)

        if self._require_run_id and not run_id:
            logger.warning("run_id required but missing: path=%s", request.url.path)
            return Response(content="X-Run-Id header required", status_code=400)

        request.state.tenant = TenantContext(tenant_id=tenant_id, run_id=run_id, source=source)

        response = await call_next(request)
        response.headers["X-Tenant-Id"] = tenant_id
        return response

    def _extract_tenant(self, request: Request) -> tuple[str, Optional[str], str]:
        headers = getattr(request, "headers", None)

        run_id: Optional[str] = _header_get(headers, _RUN_ID_HEADER)

        # 1) Explicit X-Tenant-Id header
        header_tenant = _header_get(headers, _TENANT_HEADER)
        if header_tenant:
            self._validate_format(header_tenant)
            if run_id:
                assert_tenant_owns_resource(header_tenant, run_id)
            return header_tenant, run_id, "header"

        # 2) Derive from X-Run-Id header
        if run_id:
            extracted = extract_tenant_from_resource_id(run_id)
            if extracted:
                self._validate_format(extracted)
                return extracted, run_id, "run_id_header"

        # 3) Path parameters
        path_params = getattr(request, "path_params", {}) or {}
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
                "(must match ^[a-zA-Z0-9][a-zA-Z0-9_.\\-]{1,98}[a-zA-Z0-9]$)"
            )


def get_tenant_context(request: Request) -> TenantContext:
    ctx = getattr(request.state, "tenant", None)
    if ctx is None:
        raise RuntimeError(
            "TenantContext not found on request.state — ensure TenantMiddleware is installed"
        )
    return ctx
