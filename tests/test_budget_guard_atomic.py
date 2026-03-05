"""
tests/test_budget_guard_atomic.py
IRONCLAD — Atomic budget guard tests for Sprint 2 BudgetGuard API

This file contains both unit tests (mocked Redis) and integration tests (real Redis).
Run integration tests with: pytest -m integration
"""
import sys
import os

# hfa-core/src dizinini path'e ekle
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'hfa-core', 'src')))

import pytest
import asyncio
import uuid
from unittest.mock import AsyncMock, patch, call, MagicMock
from typing import List, Tuple, Optional

import redis.asyncio as aioredis
import pytest_asyncio

from hfa.governance.budget_guard import BudgetGuard

# Eğer TenantBudget modeli yoksa, geçici olarak burada tanımla
try:
    from hfa.governance.models import TenantBudget
except ImportError:
    from dataclasses import dataclass
    
    @dataclass
    class TenantBudget:
        """Tenant budget model (geçici tanım)"""
        tenant_id: str
        limit_cents: int
        spent_cents: int
        status: str
        run_ids: List[str]


# ----------------------------------------------------------------------
# UNIT TESTS (Mocked Redis) - Fast, deterministic
# ----------------------------------------------------------------------

class TestBudgetGuardUnit:
    """Unit tests with mocked Redis - no real connection needed."""

    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        redis = AsyncMock()
        redis.evalsha = AsyncMock()
        redis.pipeline = MagicMock()
        redis.set = AsyncMock()
        redis.get = AsyncMock()
        redis.delete = AsyncMock()
        return redis

    @pytest.fixture
    def guard(self, mock_redis):
        """Create guard with mocked Redis."""
        return BudgetGuard(mock_redis)

    @pytest.fixture
    def tenant_id(self):
        """Valid tenant ID matching middleware regex."""
        return f"test-tenant-{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def run_id(self, tenant_id):
        """Valid run ID in IRONCLAD format."""
        return f"run-{tenant_id}-{uuid.uuid4()}"

    # ------------------------------------------------------------------
    # TEST 1: evalsha unpack format verification (CRITICAL)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_evalsha_called_with_unpacked_args(self, guard, mock_redis, tenant_id, run_id):
        """
        Verify that evalsha is called with unpacked args, not lists.
        This is a IRONCLAD requirement from V5.3.
        """
        # Arrange
        guard._debit_script_sha = "test_sha_123"
        
        # Mock _ensure_tenant_exists to return True
        with patch.object(guard, '_ensure_tenant_exists', AsyncMock(return_value=True)):
            # Act
            await guard.debit(tenant_id, run_id, 500)
        
        # Assert - evalsha called exactly once
        mock_redis.evalsha.assert_called_once()
        args, kwargs = mock_redis.evalsha.call_args
        
        # Check format: evalsha(sha, numkeys, key1, key2, key3, arg1, arg2)
        assert len(args) >= 3, "Not enough arguments to evalsha"
        assert args[0] == "test_sha_123", "First arg should be SHA"
        
        # numkeys should be positive integer
        numkeys = args[1]
        assert isinstance(numkeys, int), "numkeys should be integer"
        assert numkeys > 0, "numkeys should be positive"
        
        # Keys should be unpacked (not in a list)
        keys = args[2:2+numkeys]
        assert len(keys) == numkeys, f"Expected {numkeys} keys, got {len(keys)}"
        
        # CRITICAL: No nested lists allowed
        for i, arg in enumerate(args[2:], 2):
            assert not isinstance(arg, list), f"Argument {i} is a list - should be unpacked"
        
        # Verify keys are strings (don't assert exact format, just type)
        for key in keys:
            assert isinstance(key, str), "Keys should be strings"

    # ------------------------------------------------------------------
    # TEST 2: recover_run() pipeline await verification - CORRECTED
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_recover_run_awaits_pipeline(self, guard, mock_redis, tenant_id, run_id):
        """
        Test that recover_run properly awaits pipeline operations.
        
        CRITICAL: redis-py pipeline is NOT awaitable, only execute() is.
        """
        # Arrange - Create mock pipeline (not async)
        mock_pipeline = MagicMock()
        mock_pipeline.set = MagicMock(return_value=mock_pipeline)  # Returns self for chaining
        mock_pipeline.execute = AsyncMock(return_value=[True, True, True])
        
        # Configure redis.pipeline to return our mock
        mock_redis.pipeline.return_value = mock_pipeline
        
        # Act
        await guard.recover_run(tenant_id, run_id, limit_cents=10000, spent_cents=2000)
        
        # Assert - pipeline was created and execute was awaited
        mock_redis.pipeline.assert_called_once()
        mock_pipeline.set.assert_called()  # Called at least once
        mock_pipeline.execute.assert_awaited_once()

    # ------------------------------------------------------------------
    # TEST 3: debit() with negative amount raises ValueError
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_debit_negative_amount_raises(self, guard, tenant_id, run_id):
        """Test debit with negative amount raises ValueError."""
        with pytest.raises(ValueError, match="negative"):
            await guard.debit(tenant_id, run_id, -500)

    # ------------------------------------------------------------------
    # TEST 4: Redis connection failure handled gracefully
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_redis_connection_failure_handling(self, guard, mock_redis, tenant_id, run_id):
        """Test graceful handling of Redis failures."""
        # Arrange
        mock_redis.evalsha.side_effect = Exception("Connection refused")
        
        with patch.object(guard, '_ensure_tenant_exists', AsyncMock(return_value=True)):
            # Act
            result = await guard.debit(tenant_id, run_id, 500)
        
        # Assert - should fail gracefully, not raise
        assert result is False

    # ------------------------------------------------------------------
    # TEST 5: get_tenant_budget returns None for non-existent tenant
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_get_tenant_budget_nonexistent(self, guard, mock_redis, tenant_id):
        """Test getting budget for non-existent tenant returns None."""
        # Arrange
        mock_redis.get.return_value = None
        
        # Act
        budget = await guard.get_tenant_budget(tenant_id)
        
        # Assert
        assert budget is None


# ----------------------------------------------------------------------
# INTEGRATION TESTS (Real Redis) - Run with -m integration
# ----------------------------------------------------------------------

@pytest.mark.integration
class TestBudgetGuardIntegration:
    """Integration tests with real Redis - test atomicity and concurrency."""

    @pytest_asyncio.fixture
    async def redis_client(self):
        """Create isolated Redis DB for testing, skip if Redis not available."""
        redis_url = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
        
        try:
            client = aioredis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=1  # Quick fail
            )
            # Test connection
            await client.ping()
            await client.flushdb()
            yield client
            await client.flushdb()
            await client.close()
        except Exception as e:
            pytest.skip(f"Redis not available: {e}")

    @pytest_asyncio.fixture
    async def guard(self, redis_client):
        """Create budget guard with real Redis."""
        guard = BudgetGuard(redis_client)
        yield guard

    @pytest.fixture
    def tenant_id(self):
        """Valid tenant ID matching middleware regex."""
        # Format: alnum start, alnum end, allowed chars in middle
        return f"test-tenant-{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def run_id(self, tenant_id):
        """Valid run ID in IRONCLAD format."""
        return f"run-{tenant_id}-{uuid.uuid4()}"

    # ------------------------------------------------------------------
    # TEST 6: initialise() - Tenant budget initialization
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_initialise_creates_tenant_budget(self, guard, tenant_id):
        """Test that initialise creates a tenant budget with correct limit."""
        # Act
        await guard.initialise(tenant_id, 10000)  # 100.00
        
        # Assert
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget is not None
        assert budget.tenant_id == tenant_id
        assert budget.limit_cents == 10000
        assert budget.spent_cents == 0
        assert budget.status == "active"
        assert budget.run_ids == []

    @pytest.mark.asyncio
    async def test_initialise_idempotent(self, guard, tenant_id):
        """Test that initialising twice doesn't reset spent amount."""
        # First initialise
        await guard.initialise(tenant_id, 10000)
        
        # Spend some budget
        run = f"run-{tenant_id}-{uuid.uuid4()}"
        await guard.debit(tenant_id, run, 2000)
        
        # Second initialise (should not reset)
        await guard.initialise(tenant_id, 10000)
        
        # Assert - spent amount preserved
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.spent_cents == 2000

    # ------------------------------------------------------------------
    # TEST 7: debit() - Basic functionality
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_debit_success(self, guard, tenant_id, run_id):
        """Test successful debit operation."""
        # Arrange
        await guard.initialise(tenant_id, 10000)
        
        # Act
        result = await guard.debit(tenant_id, run_id, 2500)
        
        # Assert
        assert result is True
        
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.spent_cents == 2500
        assert run_id in budget.run_ids
        assert budget.status == "active"

    @pytest.mark.asyncio
    async def test_debit_insufficient_funds(self, guard, tenant_id, run_id):
        """Test debit fails when insufficient funds."""
        # Arrange
        await guard.initialise(tenant_id, 1000)  # 10.00 limit
        
        # Act - Try to debit more than limit
        result = await guard.debit(tenant_id, run_id, 1500)
        
        # Assert
        assert result is False
        
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.spent_cents == 0
        assert run_id not in budget.run_ids

    @pytest.mark.asyncio
    async def test_debit_exact_limit(self, guard, tenant_id, run_id):
        """Test debit with exact limit amount."""
        # Arrange
        await guard.initialise(tenant_id, 5000)
        
        # Act
        result = await guard.debit(tenant_id, run_id, 5000)
        
        # Assert
        assert result is True
        
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.spent_cents == 5000
        assert budget.status == "exhausted"

    @pytest.mark.asyncio
    async def test_debit_duplicate_run_id(self, guard, tenant_id, run_id):
        """Test that same run_id cannot be debited twice."""
        # Arrange
        await guard.initialise(tenant_id, 10000)
        
        # First debit
        result1 = await guard.debit(tenant_id, run_id, 2000)
        
        # Second debit with same run_id
        result2 = await guard.debit(tenant_id, run_id, 3000)
        
        # Assert
        assert result1 is True
        assert result2 is False  # Should fail
        
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.spent_cents == 2000  # Only first counted

    # ------------------------------------------------------------------
    # TEST 8: freeze() - Freezing runs
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_freeze_existing_run(self, guard, tenant_id, run_id):
        """Test freezing an existing run."""
        # Arrange
        await guard.initialise(tenant_id, 10000)
        await guard.debit(tenant_id, run_id, 2000)
        
        # Act
        result = await guard.freeze(tenant_id, run_id)
        
        # Assert
        assert result is True

    @pytest.mark.asyncio
    async def test_freeze_prevents_further_debits(self, guard, tenant_id, run_id):
        """Test that frozen run cannot be debited again."""
        # Arrange
        await guard.initialise(tenant_id, 10000)
        await guard.debit(tenant_id, run_id, 2000)
        await guard.freeze(tenant_id, run_id)
        
        # Act - Try to debit again
        result = await guard.debit(tenant_id, run_id, 1000)
        
        # Assert
        assert result is False
        
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.spent_cents == 2000  # Unchanged

    # ------------------------------------------------------------------
    # TEST 9: recover_run() - CORRECTED for Sprint2 contract
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_recover_run_restores_state(self, guard, tenant_id, run_id):
        """
        Test that recover_run restores tenant budget state.
        
        CRITICAL: recover_run does NOT reset spent to zero.
        It restores a previously known state (limit_cents, spent_cents).
        """
        # Arrange
        await guard.initialise(tenant_id, 10000)
        await guard.debit(tenant_id, run_id, 2000)
        
        # Get current state
        budget_before = await guard.get_tenant_budget(tenant_id)
        
        # Act - recover with EXACT current state
        await guard.recover_run(
            tenant_id, 
            run_id, 
            limit_cents=budget_before.limit_cents, 
            spent_cents=budget_before.spent_cents
        )
        
        # Assert - state should be identical (not reset)
        budget_after = await guard.get_tenant_budget(tenant_id)
        assert budget_after.spent_cents == budget_before.spent_cents
        assert budget_after.limit_cents == budget_before.limit_cents
        assert budget_after.status == budget_before.status

    @pytest.mark.asyncio
    async def test_recover_frozen_run(self, guard, tenant_id, run_id):
        """
        Test recovering a frozen run.
        After recover, run should be usable again with NEW run_id.
        """
        # Arrange
        await guard.initialise(tenant_id, 10000)
        await guard.debit(tenant_id, run_id, 2000)
        await guard.freeze(tenant_id, run_id)
        
        # Get current state
        budget = await guard.get_tenant_budget(tenant_id)
        
        # Act - recover with current state
        await guard.recover_run(
            tenant_id, 
            run_id, 
            limit_cents=budget.limit_cents, 
            spent_cents=budget.spent_cents
        )
        
        # Should be able to debit with a NEW run_id (not same run_id)
        new_run_id = f"run-{tenant_id}-{uuid.uuid4()}"
        result = await guard.debit(tenant_id, new_run_id, 1000)
        assert result is True
        
        # Total spent should be 3000 (2000 + 1000)
        budget_final = await guard.get_tenant_budget(tenant_id)
        assert budget_final.spent_cents == 3000

    @pytest.mark.asyncio
    async def test_recover_exhausted_tenant(self, guard, tenant_id, run_id):
        """
        Test recover on exhausted tenant - status should become active
        if spent < limit after recovery.
        """
        # Arrange - exhaust the tenant
        await guard.initialise(tenant_id, 1000)
        await guard.debit(tenant_id, run_id, 1000)
        
        # Verify exhausted
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.status == "exhausted"
        
        # Act - recover with LOWER spent amount (simulating refund/rollback)
        await guard.recover_run(
            tenant_id, 
            run_id, 
            limit_cents=1000, 
            spent_cents=500  # Lower than before
        )
        
        # Assert - status should be active again
        budget_after = await guard.get_tenant_budget(tenant_id)
        assert budget_after.spent_cents == 500
        assert budget_after.status == "active"  # No longer exhausted

    # ------------------------------------------------------------------
    # TEST 10: CONCURRENCY TESTS - Atomicity verification
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_concurrent_debits_within_limit(self, guard, tenant_id):
        """Test concurrent debits when total <= limit."""
        # Arrange
        await guard.initialise(tenant_id, 10000)  # 100.00 limit
        
        async def debit_task(amount: int) -> Tuple[bool, str]:
            run = f"run-{tenant_id}-{uuid.uuid4()}"
            result = await guard.debit(tenant_id, run, amount)
            return result, run
        
        # Act - 10 concurrent debits of 500 cents each (total 5000)
        tasks = [debit_task(500) for _ in range(10)]
        results = await asyncio.gather(*tasks)
        
        # Assert
        successes = [r for r in results if r[0] is True]
        assert len(successes) == 10
        
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.spent_cents == 5000
        assert budget.status == "active"

    @pytest.mark.asyncio
    async def test_concurrent_debits_exceed_limit(self, guard, tenant_id):
        """Test concurrent debits when total exceeds limit - atomicity critical."""
        # Arrange
        await guard.initialise(tenant_id, 5000)  # 50.00 limit
        
        async def debit_task(amount: int) -> Tuple[bool, str]:
            run = f"run-{tenant_id}-{uuid.uuid4()}"
            result = await guard.debit(tenant_id, run, amount)
            return result, run
        
        # Act - 10 concurrent debits of 1000 cents each (total 10000)
        tasks = [debit_task(1000) for _ in range(10)]
        results = await asyncio.gather(*tasks)
        
        # Assert - Exactly 5 should succeed (5 * 1000 = 5000)
        successes = [r for r in results if r[0] is True]
        failures = [r for r in results if r[0] is False]
        
        assert len(successes) == 5
        assert len(failures) == 5
        
        # CRITICAL: Total spent must NEVER exceed limit
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.spent_cents == 5000
        assert budget.spent_cents <= budget.limit_cents
        assert budget.status == "exhausted"

    @pytest.mark.asyncio
    async def test_concurrent_same_run_id_fails(self, guard, tenant_id):
        """Test concurrent debits with same run ID - only one succeeds."""
        # Arrange
        await guard.initialise(tenant_id, 10000)
        run_id = f"run-{tenant_id}-{uuid.uuid4()}"
        
        async def debit_task():
            return await guard.debit(tenant_id, run_id, 1000)
        
        # Act - 5 concurrent debits with same run_id
        tasks = [debit_task() for _ in range(5)]
        results = await asyncio.gather(*tasks)
        
        # Assert - Exactly one should succeed
        successes = [r for r in results if r is True]
        failures = [r for r in results if r is False]
        
        assert len(successes) == 1
        assert len(failures) == 4
        
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.spent_cents == 1000  # Only one counted

    @pytest.mark.asyncio
    async def test_concurrent_freeze_and_debit(self, guard, tenant_id):
        """
        Test concurrent freeze and debit.
        
        CRITICAL: This test is non-deterministic by nature.
        We verify the invariant: spent never exceeds limit,
        and no exceptions are raised.
        """
        # Arrange
        await guard.initialise(tenant_id, 10000)
        run_id = f"run-{tenant_id}-{uuid.uuid4()}"
        await guard.debit(tenant_id, run_id, 2000)
        
        async def freeze_task():
            return await guard.freeze(tenant_id, run_id)
        
        async def debit_task():
            return await guard.debit(tenant_id, run_id, 1000)
        
        # Act - Concurrent freeze and debit
        freeze_result, debit_result = await asyncio.gather(
            freeze_task(),
            debit_task(),
            return_exceptions=True
        )
        
        # Assert - No exceptions
        assert not isinstance(freeze_result, Exception)
        assert not isinstance(debit_result, Exception)
        
        # Invariant must hold
        budget = await guard.get_tenant_budget(tenant_id)
        assert budget.spent_cents <= budget.limit_cents


# ----------------------------------------------------------------------
# PERFORMANCE TEST - Optional, can be slow
# ----------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.slow
class TestBudgetGuardPerformance:
    """Performance tests - marked slow, not run by default."""

    @pytest_asyncio.fixture
    async def redis_client(self):
        """Create Redis client with skip if unavailable."""
        redis_url = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
        try:
            client = aioredis.from_url(redis_url, decode_responses=True)
            await client.ping()
            await client.flushdb()
            yield client
            await client.flushdb()
            await client.close()
        except Exception as e:
            pytest.skip(f"Redis not available: {e}")

    @pytest_asyncio.fixture
    async def guard(self, redis_client):
        guard = BudgetGuard(redis_client)
        yield guard

    @pytest.fixture
    def tenant_id(self):
        return f"perf-test-{uuid.uuid4().hex[:8]}"

    @pytest.mark.asyncio
    async def test_high_concurrency_mixed_operations(self, guard, tenant_id):
        """Test high concurrency with mixed operations - 100 concurrent tasks."""
        # Arrange
        await guard.initialise(tenant_id, 100000)  # 1000.00 limit
        
        async def mixed_worker(i: int) -> bool:
            """Mix of operations."""
            run = f"run-{tenant_id}-{uuid.uuid4()}"
            
            if i % 3 == 0:
                # Just debit
                return await guard.debit(tenant_id, run, 1000)
            elif i % 3 == 1:
                # Debit then freeze
                if await guard.debit(tenant_id, run, 1000):
                    return await guard.freeze(tenant_id, run)
                return False
            else:
                # Debit, get state, recover
                if await guard.debit(tenant_id, run, 1000):
                    budget = await guard.get_tenant_budget(tenant_id)
                    return await guard.recover_run(
                        tenant_id, 
                        run, 
                        limit_cents=budget.limit_cents, 
                        spent_cents=budget.spent_cents - 1000  # Rollback effect
                    )
                return False
        
        # Act - 100 concurrent mixed operations
        tasks = [mixed_worker(i) for i in range(100)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Assert
        assert len(results) == 100
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Got exceptions: {exceptions}"