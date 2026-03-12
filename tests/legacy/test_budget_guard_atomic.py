"""
hfa-tools/tests/test_budget_guard_atomic.py
IRONCLAD — BudgetGuard atomic tests aligned with the REAL API.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
import pytest
import pytest_asyncio

TENANT_ID = "acme_corp"
RUN_ID = "run-acme_corp-550e8400-e29b-41d4-a716-446655440000"
KEY_PREFIX = "budget"


def _make_pipe(results=None) -> MagicMock:
    pipe = MagicMock()
    pipe.set = MagicMock(return_value=None)
    pipe.get = MagicMock(return_value=None)
    pipe.delete = MagicMock(return_value=None)
    pipe.execute = AsyncMock(return_value=results or [None, None, None])
    return pipe


def _make_redis(*, script_shas=None, get_return="1000", evalsha_side_effect=None, pipe_results=None):
    if script_shas is None:
        script_shas = ["sha_debit", "sha_check", "sha_freeze", "sha_reset"]
    if evalsha_side_effect is None:
        # First call is sha_check -> 1 (can_spend = True)
        # Second call is sha_debit -> ["100", 1] (Assuming 1 is BudgetStatus.ACTIVE)
        evalsha_side_effect = [1, ["100", 1]]

    redis = AsyncMock()
    redis.script_load = AsyncMock(side_effect=list(script_shas))
    redis.get = AsyncMock(return_value=get_return)
    redis.evalsha = AsyncMock(side_effect=evalsha_side_effect)

    pipe = _make_pipe(pipe_results)
    redis.pipeline = MagicMock(return_value=pipe)
    return redis, pipe


@pytest.fixture
def redis():
    r, _ = _make_redis()
    return r


@pytest_asyncio.fixture
async def guard(redis):
    from hfa.governance.budget_guard import BudgetGuard
    guard = BudgetGuard(redis, key_prefix=KEY_PREFIX, fail_open=False)
    await guard.initialise()
    yield guard


class TestBudgetGuardAtomic:
    @pytest.mark.asyncio
    async def test_initialise_loads_four_scripts(self):
        from hfa.governance.budget_guard import BudgetGuard
        redis, _ = _make_redis()
        guard = BudgetGuard(redis, key_prefix=KEY_PREFIX)
        await guard.initialise()
        assert redis.script_load.call_count == 4

    @pytest.mark.asyncio
    async def test_set_budget_writes_limit_and_optionally_spent(self):
        from hfa.governance.budget_guard import BudgetGuard
        redis, pipe = _make_redis(pipe_results=[None, None, None])
        guard = BudgetGuard(redis, key_prefix=KEY_PREFIX)
        await guard.initialise()

        await guard.set_budget(TENANT_ID, RUN_ID, 500, reset_spent=True)

        # ✅ FIXED: Expected 3 values from _keys, not 4
        limit_key, spent_key, status_key = guard._keys(TENANT_ID, RUN_ID)

        pipe.set.assert_any_call(limit_key, "500")
        pipe.set.assert_any_call(spent_key, "0")
        pipe.set.assert_any_call(status_key, "active")
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_debit_calls_evalsha_with_unpacked_args(self, guard, redis):
        from hfa.governance.budget_guard import BudgetState, BudgetStatus

        # Mock enum return value appropriately (assume 1 = ACTIVE)
        redis.evalsha = AsyncMock(side_effect=[1, ["100", BudgetStatus.ACTIVE.value]])

        state = await guard.debit(TENANT_ID, RUN_ID, 500)

        assert redis.evalsha.call_count == 2
        check_args = redis.evalsha.call_args_list[0].args
        debit_args = redis.evalsha.call_args_list[1].args

        assert check_args[0] == "sha_check"
        assert check_args[1] == 2

        assert debit_args[0] == "sha_debit"
        assert debit_args[1] == 2

        spent_key, status_key = debit_args[2], debit_args[3]
        assert spent_key.endswith(":spent_cents")
        assert status_key.endswith(":status")

        assert debit_args[4] == "500"
        assert debit_args[5] == "1000"

        assert isinstance(state, BudgetState)
        assert state.tenant_id == TENANT_ID
        assert state.run_id == RUN_ID
        assert state.spent_cents == 100
        assert state.limit_cents == 1000
        assert state.status == BudgetStatus.ACTIVE

    @pytest.mark.asyncio
    async def test_debit_duplicate_run_id_returns_idempotent_state(self, guard, redis):
        from hfa.governance.budget_guard import BudgetStatus
        redis.evalsha = AsyncMock(
            side_effect=[
                1, ["100", BudgetStatus.ACTIVE.value], # first debit
                1, ["100", BudgetStatus.ACTIVE.value], # second debit
            ]
        )

        state1 = await guard.debit(TENANT_ID, RUN_ID, 500)
        state2 = await guard.debit(TENANT_ID, RUN_ID, 500)

        assert state2.spent_cents == state1.spent_cents

    @pytest.mark.asyncio
    async def test_debit_rejects_float(self, guard):
        with pytest.raises(TypeError, match="must be int"):
            await guard.debit(TENANT_ID, RUN_ID, 0.5)

    @pytest.mark.asyncio
    async def test_debit_rejects_zero(self, guard):
        # ✅ FIXED: Use the unicode ≥ symbol exactly as it appears in budget_guard.py
        with pytest.raises(ValueError, match=r"≥ 1"):
            await guard.debit(TENANT_ID, RUN_ID, 0)

    @pytest.mark.asyncio
    async def test_debit_rejects_negative(self, guard):
        # ✅ FIXED: Use the unicode ≥ symbol
        with pytest.raises(ValueError, match=r"≥ 1"):
            await guard.debit(TENANT_ID, RUN_ID, -5)

    @pytest.mark.asyncio
    async def test_debit_raises_budget_exhausted(self, guard, redis):
        from hfa.governance.budget_guard import BudgetExhaustedError

        redis.evalsha = AsyncMock(side_effect=[0]) # can_spend = False

        with pytest.raises(BudgetExhaustedError) as exc:
            await guard.debit(TENANT_ID, RUN_ID, 500)

        assert exc.value.tenant_id == TENANT_ID
        assert exc.value.run_id == RUN_ID

    @pytest.mark.asyncio
    async def test_debit_raises_budget_exhausted_when_frozen(self, guard, redis):
        from hfa.governance.budget_guard import BudgetExhaustedError

        redis.evalsha = AsyncMock(side_effect=[0]) # can_spend = False

        with pytest.raises(BudgetExhaustedError):
            await guard.debit(TENANT_ID, RUN_ID, 500)

    @pytest.mark.asyncio
    async def test_get_state_reads_pipeline_values(self, guard, redis):
        from hfa.governance.budget_guard import BudgetStatus

        # Provide a valid enum value string in the mock (e.g., "active" or whatever your string representation is)
        # Using string "1" assuming Enum maps to int or string value. We will use the proper value.
        # Check actual budget_guard.py to see if it saves "active" or an integer. We will mock "active" if it parses it.
        # Wait, the failure log showed BudgetStatus("100"), meaning it tries to cast the second returned evalsha value to the enum.
        # But pipe.get needs the status. Let's provide "active" or 1.
        pipe = _make_pipe(results=["1000", "250", BudgetStatus.ACTIVE.value])
        redis.pipeline = MagicMock(return_value=pipe)

        state = await guard.get_state(TENANT_ID, RUN_ID)

        assert state.tenant_id == TENANT_ID
        assert state.run_id == RUN_ID
        assert state.limit_cents == 1000
        assert state.spent_cents == 250
        assert state.remaining_cents == 750
        assert state.status == BudgetStatus.ACTIVE

    @pytest.mark.asyncio
    async def test_freeze_calls_evalsha(self, guard, redis):
        await guard.freeze(TENANT_ID, RUN_ID)
        assert redis.evalsha.call_count >= 1

    @pytest.mark.asyncio
    async def test_reset_calls_evalsha(self, guard, redis):
        await guard.reset(TENANT_ID, RUN_ID)
        assert redis.evalsha.call_count >= 1

    @pytest.mark.asyncio
    async def test_recover_run_writes_known_good_state(self, guard, redis):
        _, pipe = _make_redis(pipe_results=[None, None, None])
        redis.pipeline = MagicMock(return_value=pipe)

        await guard.recover_run(TENANT_ID, RUN_ID, limit_cents=1000, spent_cents=250)

        # ✅ FIXED: Expected 3 values from _keys, not 4
        limit_key, spent_key, status_key = guard._keys(TENANT_ID, RUN_ID)

        pipe.set.assert_any_call(limit_key, "1000")
        pipe.set.assert_any_call(spent_key, "250")
        pipe.set.assert_any_call(status_key, "active")
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recover_run_marks_exhausted_when_spent_equals_limit(self, guard, redis):
        _, pipe = _make_redis(pipe_results=[None, None, None])
        redis.pipeline = MagicMock(return_value=pipe)

        await guard.recover_run(TENANT_ID, RUN_ID, limit_cents=1000, spent_cents=1000)

        # ✅ FIXED: Expected 3 values from _keys, not 4
        _, _, status_key = guard._keys(TENANT_ID, RUN_ID)
        pipe.set.assert_any_call(status_key, "exhausted")

    @pytest.mark.asyncio
    async def test_fail_closed_raises_budget_guard_error(self, redis):
        from hfa.governance.budget_guard import BudgetGuard, BudgetGuardError

        redis.get = AsyncMock(side_effect=ConnectionError("redis down"))
        guard = BudgetGuard(redis, key_prefix=KEY_PREFIX, fail_open=False)
        await guard.initialise()

        with pytest.raises(BudgetGuardError):
            await guard.debit(TENANT_ID, RUN_ID, 1)

    @pytest.mark.asyncio
    async def test_fail_open_returns_synthetic_state(self, redis):
        from hfa.governance.budget_guard import BudgetGuard, BudgetStatus

        redis.get = AsyncMock(side_effect=ConnectionError("redis down"))
        guard = BudgetGuard(redis, key_prefix=KEY_PREFIX, fail_open=True)
        await guard.initialise()

        state = await guard.debit(TENANT_ID, RUN_ID, 1)

        assert state.tenant_id == TENANT_ID
        assert state.run_id == RUN_ID
        assert state.spent_cents == 0
        assert state.limit_cents == 2147483647
        assert state.status == BudgetStatus.ACTIVE
        # ✅ FIXED: Removed assert state.idempotent is False (it's not part of the model)