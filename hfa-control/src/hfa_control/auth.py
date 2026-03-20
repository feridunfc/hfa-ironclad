"""
hfa-control/src/hfa_control/auth.py
IRONCLAD Sprint 17 — Operator authentication

Design
------
The control plane exposes two auth tiers:

  Public   — liveness/readiness probes (no auth)
  Operator — mutating and sensitive read endpoints (X-CP-Auth header)
  Tenant   — tenant-scoped endpoints (X-Tenant-ID header)

Operator auth
-------------
Validates the X-CP-Auth header against CP_AUTH_SECRET using
hmac.compare_digest — constant-time comparison prevents timing attacks.

Security modes
--------------
  CP_AUTH_SECRET unset / empty:
    Production: HARD FAIL — all operator endpoints return 503 (unsafe to serve)
    Development (APP_ENV=development|test): warn and allow (backward compat)

  CP_AUTH_SECRET set:
    All operator endpoints require the exact secret value.
    Wrong value → 403 Forbidden.
    Missing header → 401 Unauthorized.

IRONCLAD rules
--------------
* hmac.compare_digest — never == for secret comparison.
* No print() — logging only.
* Fail-closed in production: missing secret = 503, not 200.
"""

from __future__ import annotations

import hmac
import logging
import os

from fastapi import HTTPException

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Secret loading
# ---------------------------------------------------------------------------

_CP_AUTH_SECRET: str = os.environ.get("CP_AUTH_SECRET", "").strip()
_APP_ENV: str = os.environ.get("APP_ENV", "development").strip().lower()

_PRODUCTION_ENVS = {"production", "staging"}
_IS_PRODUCTION = _APP_ENV in _PRODUCTION_ENVS

if not _CP_AUTH_SECRET:
    if _IS_PRODUCTION:
        logger.critical(
            "SECURITY: CP_AUTH_SECRET is not set in %s environment. "
            "Operator endpoints will return 503. "
            "Set CP_AUTH_SECRET to a strong random secret.",
            _APP_ENV.upper(),
        )
    else:
        logger.warning(
            "CP_AUTH_SECRET not set (APP_ENV=%s). "
            "Operator auth is DISABLED — acceptable for development only. "
            "Never run without CP_AUTH_SECRET in production.",
            _APP_ENV,
        )
else:
    if len(_CP_AUTH_SECRET) < 32:
        logger.warning(
            "CP_AUTH_SECRET is shorter than 32 characters. "
            "Use a strong random secret (e.g. openssl rand -hex 32)."
        )
    logger.info(
        "CP_AUTH_SECRET loaded (len=%d, env=%s)", len(_CP_AUTH_SECRET), _APP_ENV
    )


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


def require_operator(x_cp_auth: str = "") -> None:
    """
    Validate the X-CP-Auth header for operator-tier endpoints.

    Raises:
        HTTPException 503: CP_AUTH_SECRET not configured in production.
        HTTPException 401: Header missing or empty.
        HTTPException 403: Header value does not match secret.
    """
    if not _CP_AUTH_SECRET:
        if _IS_PRODUCTION:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Operator auth is not configured. "
                    "Set CP_AUTH_SECRET environment variable."
                ),
            )
        # Development/test: allow without secret (log warning)
        logger.debug(
            "require_operator: skipped (CP_AUTH_SECRET not set, non-production)"
        )
        return

    if not x_cp_auth:
        raise HTTPException(
            status_code=401,
            detail="X-CP-Auth header required for operator endpoints.",
        )

    # Constant-time comparison — prevent timing oracle attacks
    if not hmac.compare_digest(x_cp_auth.encode(), _CP_AUTH_SECRET.encode()):
        logger.warning(
            "require_operator: rejected — invalid X-CP-Auth (len=%d)",
            len(x_cp_auth),
        )
        raise HTTPException(
            status_code=403,
            detail="Invalid operator credentials.",
        )


def require_tenant(x_tenant_id: str = "") -> str:
    """
    Validate and return the X-Tenant-ID header.

    Raises:
        HTTPException 400: Header missing or empty.
    """
    if not x_tenant_id or not x_tenant_id.strip():
        raise HTTPException(
            status_code=400,
            detail="X-Tenant-ID header required.",
        )
    return x_tenant_id.strip()


def auth_status() -> dict:
    """
    Return auth configuration status for health/readiness endpoints.
    Never exposes the secret value.
    """
    return {
        "operator_auth_enabled": bool(_CP_AUTH_SECRET),
        "app_env": _APP_ENV,
        "production_mode": _IS_PRODUCTION,
        "secret_length": len(_CP_AUTH_SECRET) if _CP_AUTH_SECRET else 0,
    }
