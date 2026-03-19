"""
hfa-control/src/hfa_control/auth.py
IRONCLAD Sprint 17.1 --- HMAC timing-safe authentication
"""

from __future__ import annotations

import hmac
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_SECRET: Optional[bytes] = None
_AUTH_ENABLED: bool = False


def _is_production() -> bool:
    """Runtime production check (not cached at import)."""
    return os.getenv("ENVIRONMENT", "development").lower() == "production"


def _get_secret() -> Optional[bytes]:
    """Lazy-load auth secret from environment."""
    global _SECRET, _AUTH_ENABLED
    if _SECRET is not None:
        return _SECRET

    secret_str = os.getenv("CP_AUTH_SECRET", "")
    if not secret_str:
        if _is_production():
            logger.critical("CP_AUTH_SECRET is empty in production — authentication impossible!")
            _AUTH_ENABLED = False
            _SECRET = None
            return None

        logger.warning("CP_AUTH_SECRET not set — authentication disabled (development mode)")
        _AUTH_ENABLED = False
        _SECRET = b""
        return b""

    _SECRET = secret_str.encode("utf-8")
    _AUTH_ENABLED = True
    return _SECRET


def is_enabled() -> bool:
    _get_secret()
    return _AUTH_ENABLED


def reset_cache() -> None:
    global _SECRET, _AUTH_ENABLED
    _SECRET = None
    _AUTH_ENABLED = False


def validate_operator_token(token: str) -> bool:
    secret = _get_secret()
    if secret is None and _is_production():
        return False
    if not secret:
        return True
    if not token:
        return False
    expected = hmac.new(secret, b"operator", "sha256").hexdigest()
    return hmac.compare_digest(token, expected)


def validate_tenant_token(token: str, tenant_id: str) -> bool:
    secret = _get_secret()
    if secret is None and _is_production():
        return False
    if not secret:
        return True
    if not token or not tenant_id:
        return False
    expected = hmac.new(secret, f"tenant:{tenant_id}".encode(), "sha256").hexdigest()
    return hmac.compare_digest(token, expected)
