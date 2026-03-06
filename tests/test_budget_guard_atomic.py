"""
tests/test_budget_guard_atomic.py
IRONCLAD — Fixed unit tests for BudgetGuard
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone
import time

from hfa.governance.budget_guard import BudgetGuard, BudgetState, usd_to_cents
from hfa.governance.exceptions import BudgetExhaustedError, BudgetGuardError


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    redis = AsyncMock()

    # Mock Redis pipeline
    pipeline = AsyncMock()
    pipeline.execute = AsyncMock(return_value=[1, 1000])  # success, remaining
    redis.pipeline = MagicMock(return_value=pipeline)

    # Mock script loading
    redis.script_load = AsyncMock(return_value="mock_sha")

    # Mock evalsha
    redis.evalsha = AsyncMock(return_value=1)  # 1 = success

    return redis


@pytest.fixture
async def guard(mock_redis):
    """Create BudgetGuard with mocked Redis."""
    guard = BudgetGuard(
        redis_client=mock_redis,
        default_budget_cents=10000,  # $100
        recovery_interval=1,  # Fast recovery for tests
    )
    # Mock script SHAs
    guard._debit_script_sha = "debit_sha"
    guard._rollback_script_sha = "rollback_sha"
    guard._commit_script_sha = "commit_sha"
    return guard


class TestBudgetGuardUnit:
    """Unit tests for BudgetGuard (mocked Redis)."""

    @pytest.mark.asyncio
    async def test_evalsha_called_with_unpacked_args(self, guard, mock_redis, tenant_id, run_id):
        """
        Verify that evalsha is called with unpacked args, not lists.
        This is a IRONCLAD requirement from V5.3.
        """
        # Arrange - initialize tenant first
        initial_cents = 10000
        mock_redis.evalsha.return_value = 1  # success

        # Act - debit with proper amount
        state = await guard.debit(
            tenant_id=tenant_id,
            run_id=run_id,
            amount_cents=500,
            initial_cents=initial_cents
        )

        # Assert - verify evalsha was called with unpacked args
        mock_redis.evalsha.assert_called_once()
        call_args = mock_redis.evalsha.call_args

        # First arg is SHA
        assert call_args[0][0] == guard._debit_script_sha

        # Second arg is numkeys (should be 2)
        assert call_args[0][1] == 2

        # Rest are unpacked keys and args (not lists)
        args = call_args[0][2:]  # Everything after numkeys

        # First two should be keys (not wrapped in list)
        assert args[0] == f"budget:{tenant_id}:remaining"
        assert args[1] == f"budget:{tenant_id}:lock:{run_id}"

        # Rest are args (not wrapped in list)
        assert args[2] == "500"  # amount
        assert args[3] == "60"  # ttl
        assert args[4] == run_id
        assert args[5] == str(initial_cents)

        # Verify state
        assert state.allowed is True
        assert state.remaining_cents == 9500  # 10000 - 500

    @pytest.mark.asyncio
    async def test_debit_negative_amount_raises(self, guard, tenant_id, run_id):
        """Test debit with negative amount raises ValueError."""
        with pytest.raises(ValueError, match=r">= 1"):
            await guard.debit(
                tenant_id=tenant_id,
                run_id=run_id,
                amount_cents=-500,
                initial_cents=10000
            )

    @pytest.mark.asyncio
    async def test_debit_zero_amount_raises(self, guard, tenant_id, run_id):
        """Test debit with zero amount raises ValueError."""
        with pytest.raises(ValueError, match=r">= 1"):
            await guard.debit(
                tenant_id=tenant_id,
                run_id=run_id,
                amount_cents=0,
                initial_cents=10000
            )

    @pytest.mark.asyncio
    async def test_redis_connection_failure_handling(self, guard, mock_redis, tenant_id, run_id):
        """Test handling of Redis failures."""
        # Arrange - make Redis fail
        mock_redis.evalsha.side_effect = Exception("Connection refused")

        # Act & Assert - should raise BudgetGuardError (fail-closed)
        with pytest.raises(BudgetGuardError, match="debit failed"):
            await guard.debit(
                tenant_id=tenant_id,
                run_id=run_id,
                amount_cents=500,
                initial_cents=10000
            )

    @pytest.mark.asyncio
    async def test_get_budget_nonexistent(self, guard, mock_redis, tenant_id):
        """Test getting budget for non-existent tenant returns zero."""
        # Arrange
        mock_redis.get.return_value = None

        # Act
        budget = await guard.get_budget(tenant_id)

        # Assert
        assert budget == 0

    @pytest.mark.asyncio
    async def test_debit_success_with_state(self, guard, mock_redis, tenant_id, run_id):
        """Test successful debit returns correct BudgetState."""
        # Arrange
        mock_redis.evalsha.return_value = 1  # success

        # Act
        state = await guard.debit(
            tenant_id=tenant_id,
            run_id=run_id,
            amount_cents=500,
            initial_cents=10000
        )

        # Assert - BudgetState fields
        assert state.allowed is True
        assert state.remaining_cents == 9500
        assert state.run_id == run_id
        assert state.idempotent is False  # First call
        assert isinstance(state.timestamp, float)

    @pytest.mark.asyncio
    async def test_debit_insufficient_funds(self, guard, mock_redis, tenant_id, run_id):
        """Test debit with insufficient funds raises BudgetExhaustedError."""
        # Arrange - Redis returns -1 for insufficient funds
        mock_redis.evalsha.return_value = -1

        # Act & Assert
        with pytest.raises(BudgetExhaustedError, match="Insufficient budget"):
            await guard.debit(
                tenant_id=tenant_id,
                run_id=run_id,
                amount_cents=500,
                initial_cents=100
            )

    @pytest.mark.asyncio
    async def test_debit_already_locked(self, guard, mock_redis, tenant_id, run_id):
        """Test debit when run is already locked returns idempotent state."""
        # Arrange - Redis returns 0 for locked
        mock_redis.evalsha.return_value = 0

        # Act
        state = await guard.debit(
            tenant_id=tenant_id,
            run_id=run_id,
            amount_cents=500,
            initial_cents=10000
        )

        # Assert - Should return locked state
        assert state.allowed is False
        assert state.remaining_cents == 0
        assert state.idempotent is False