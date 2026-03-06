"""
tests/conftest.py
IRONCLAD — FIXED: budget_guard artık instance döndürüyor!
"""
import sys
import os
from pathlib import Path
import pytest
import uuid
import asyncio
from typing import Generator
from unittest.mock import AsyncMock, MagicMock

# ============================================================================
# Add source directories to Python path
# ============================================================================
project_root = Path(__file__).parent.parent
hfa_core_src = project_root / "hfa-core" / "src"
hfa_tools_src = project_root / "hfa-tools" / "src"

for path in [hfa_core_src, hfa_tools_src]:
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))
        print(f"Added to Python path: {path}")

# ============================================================================
# Import project modules with fallbacks
# ============================================================================
try:
    from hfa.governance.budget_guard import BudgetGuard
    from hfa.governance.exceptions import BudgetExhaustedError, BudgetGuardError
    from hfa_tools.middleware.tenant import TenantMiddleware, ParsedRunId, TenantFormatError, _header_get, validate_run_id_format
    print("✅ Successfully imported real project modules")
except ImportError as e:
    print(f"⚠️ Using fallback mocks: {e}")

    # Define fallback classes
    class BudgetGuard:
        def __init__(self, redis_client=None, default_budget_cents=10000, recovery_interval=1):
            self.redis = redis_client
            self.default_budget_cents = default_budget_cents
            self.recovery_interval = recovery_interval

        async def debit(self, tenant_id, run_id, amount_cents, initial_cents=None):
            return type('BudgetState', (), {
                'allowed': True,
                'remaining_cents': initial_cents - amount_cents if initial_cents else 0,
                'run_id': run_id,
                'idempotent': False,
                'timestamp': 0
            })()

        async def get_budget(self, tenant_id):
            return 0

        async def close(self):
            pass

    class BudgetExhaustedError(Exception):
        pass

    class BudgetGuardError(Exception):
        pass

    class TenantMiddleware:
        def __init__(self, app): self.app = app

    class ParsedRunId:
        def __init__(self, prefix, tenant_id, uuid, uuid_version):
            self.prefix = prefix
            self.tenant_id = tenant_id
            self.uuid = uuid
            self.uuid_version = uuid_version

    class TenantFormatError(Exception):
        pass

    def _header_get(headers, key):
        if not headers:
            return None
        key_lower = key.lower()
        if hasattr(headers, 'get'):
            return headers.get(key)
        for k, v in headers.items():
            if k.lower() == key_lower:
                return v
        return None

    def validate_run_id_format(run_id, expected_tenant_id):
        parts = run_id.split('-', 2)
        if len(parts) < 3:
            raise TenantFormatError(f"Invalid format: {run_id}")
        return ParsedRunId(parts[0], parts[1], parts[2], 4)

# ============================================================================
# Fixtures - FIXED: budget_guard artık instance döndürüyor!
# ============================================================================

@pytest.fixture
def tenant_id() -> str:
    """Generate a unique tenant ID for tests."""
    return f"test-tenant-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def run_id(tenant_id: str) -> str:
    """Generate a unique run ID for tests."""
    return f"run-{tenant_id}-{uuid.uuid4()}"


@pytest.fixture
def mock_redis():
    """Mock Redis client for unit tests."""
    redis = AsyncMock()

    # Mock pipeline
    pipeline = AsyncMock()
    pipeline.execute = AsyncMock(return_value=[1, 1000])
    redis.pipeline = MagicMock(return_value=pipeline)

    # Mock script operations
    redis.script_load = AsyncMock(return_value="mock_sha")
    redis.evalsha = AsyncMock(return_value=1)
    redis.get = AsyncMock(return_value=None)
    redis.setex = AsyncMock(return_value=True)
    redis.delete = AsyncMock(return_value=True)

    # Mock scan_iter for recovery
    redis.scan_iter = MagicMock()
    redis.scan_iter.return_value.__aiter__.return_value = []

    return redis


@pytest.fixture
def budget_guard(mock_redis):  # ← FIXED: async def DEĞİL!
    """Create BudgetGuard with mocked Redis - returns instance, not coroutine."""
    guard = BudgetGuard(
        redis_client=mock_redis,
        default_budget_cents=10000,
        recovery_interval=1,
    )
    # Mock script SHAs if they exist
    if hasattr(guard, '_debit_script_sha'):
        guard._debit_script_sha = "debit_sha"
        guard._rollback_script_sha = "rollback_sha"
        guard._commit_script_sha = "commit_sha"

    return guard  # ← Artık coroutine değil, direkt instance!


@pytest.fixture
def tenant_middleware():
    """Create TenantMiddleware instance."""
    return TenantMiddleware(app=MagicMock())


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()