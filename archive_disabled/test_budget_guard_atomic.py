"""
tests/test_budget_guard_atomic.py
IRONCLAD — BudgetGuard atomic tests aligned with the real Sprint 2 API.

Covers:
- initialise() script loading
- set_budget()
- debit() atomic + idempotent Lua path
- get_state()
- freeze()
- reset()
- recover_run()
- Option A: _maybe_await compatibility with AsyncMock pipeline calls
- fail_open / fail_closed behavior
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

# Guardian Notu: NameError hatasını çözen global sabitler buradadır.
TENANT_ID = "acme_corp"
RUN_ID = "run-acme_corp-550e8400-e29b-41d4-a716-446655440000"
KEY_PREFIX = "budget"


def _make_pipe(results=None) -> MagicMock:
    """
    Production-like redis pipeline mock:
    queue methods are sync, execute() is async.
    """
    pipe = MagicMock()
    pipe.set = MagicMock(return_value=None)
    pipe.get = MagicMock(return_value=None)
    pipe.delete = MagicMock(return_value=None)
    pipe.execute = AsyncMock(return_value=results or [None, None, None])
    return pipe


def _make_redis(
    *,
    script_shas=None,
    get_return="1000",
    evalsha_return=None,
    pipe_results=None,
):
    """
    Return (redis_mock, pipe_mock).
    """
    if script_shas is None:
        script_shas = ["sha_debit", "sha_freeze", "sha_reset"]
    if evalsha_return is None:
        # [allowed, new_spent, status, idempotent]
        evalsha_return = ["1", "100", "active", "0"]

    redis = AsyncMock()
    redis.script_load = AsyncMock(side_effect=list(script_shas))
    redis.get = AsyncMock(return_value=get_return)
    redis.evalsha = AsyncMock(return_value=evalsha_return)

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
    async def test_initialise_loads_three_scripts(self):
        from hfa.governance.budget_guard import BudgetGuard

        redis, _ = _make_redis()
        guard = BudgetGuard(redis, key_prefix=KEY_PREFIX)

        await guard.initialise()

        assert redis.script_load.call_count == 3
        assert guard._sha_debit == "sha_debit"
        assert guard._sha_freeze == "sha_freeze"
        assert guard._sha_reset == "sha_reset"

    @pytest.mark.asyncio
    async def test_set_budget_writes_limit_and_optionally_spent(self):
        from hfa.governance.budget_guard import BudgetGuard

        redis, pipe = _make_redis(pipe_results=[None, None, None])
        guard = BudgetGuard(redis, key_prefix=KEY_PREFIX)
        await guard.initialise()

        await guard.set_budget(TENANT_ID, RUN_ID, 500, reset_spent=True)

        limit_key, spent_key, status_key, run_ids_key = guard._keys(TENANT_ID, RUN_ID)

        pipe.set.assert_any_call(limit_key, "500")
        pipe.set.assert_any_call(spent_key, "0")
        pipe.set.assert_any_call(status_key, "active")
        pipe.delete.assert_called_with(run_ids_key)
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_debit_calls_evalsha_with_unpacked_args(self, guard, redis):
        from hfa.governance.budget_guard import BudgetState, BudgetStatus

        state = await guard.debit(TENANT_ID, RUN_ID, 500)

        redis.evalsha.assert_awaited_once()
        args = redis.evalsha.call_args.args

        assert args[0] == "sha_debit"
        assert args[1] == 3

        spent_key, status_key, run_ids_key = args[2], args[3], args[4]
        assert spent_key.endswith(":spent_cents")
        assert status_key.endswith(":status")
        assert run_ids_key.endswith(":run_ids")

        assert args[5] == "500"
        assert args[6] == "1000"
        assert args[7] == RUN_ID

        assert isinstance(state, BudgetState)
        assert state.tenant_id == TENANT_ID
        assert state.run_id == RUN_ID
        assert state.spent_cents == 100
        assert state.limit_cents == 1000
        assert state.remaining_cents == 900
        assert state.status == BudgetStatus.ACTIVE
        assert state.idempotent is False

    @pytest.mark.asyncio
    async def test_debit_duplicate_run_id_returns_idempotent_state(self, guard, redis):
        # second call returns idempotent=1
        redis.evalsha = AsyncMock(
            side_effect=[
                ["1", "100", "active", "0"],
                ["1", "100", "active", "1"],
            ]
        )

        state1 = await guard.debit(TENANT_ID, RUN_ID, 500)
        state2 = await guard.debit(TENANT_ID, RUN_ID, 500)

        assert state1.idempotent is False
        assert state2.idempotent is True
        assert state2.spent_cents == state1.spent_cents

    @pytest.mark.asyncio
    async def test_debit_rejects_float(self, guard):
        with pytest.raises(TypeError, match="must be int"):
            await guard.debit(TENANT_ID, RUN_ID, 0.5)

    @pytest.mark.asyncio
    async def test_debit_rejects_zero(self, guard):
        with pytest.raises(ValueError, match=r">= 1"):
            await guard.debit(TENANT_ID, RUN_ID, 0)

    @pytest.mark.asyncio
    async def test_debit_rejects_negative(self, guard):
        with pytest.raises(ValueError, match=r">= 1"):
            await guard.debit(TENANT_ID, RUN_ID, -5)

    @pytest.mark.asyncio
    async def test_debit_raises_budget_exhausted(self, guard, redis):
        from hfa.governance.budget_guard import BudgetExhaustedError

        redis.evalsha = AsyncMock(return_value=["0", "1000", "active", "0"])

        with pytest.raises(BudgetExhaustedError) as exc:
            await guard.debit(TENANT_ID, RUN_ID, 500)

        assert exc.value.tenant_id == TENANT_ID
        assert exc.value.run_id == RUN_ID

    @pytest.mark.asyncio
    async def test_debit_raises_budget_exhausted_when_frozen(self, guard, redis):
        from hfa.governance.budget_guard import BudgetExhaustedError

        redis.evalsha = AsyncMock(return_value=["0", "500", "frozen", "0"])

        with pytest.raises(BudgetExhaustedError):
            await guard.debit(TENANT_ID, RUN_ID, 500)

    @pytest.mark.asyncio
    async def test_get_state_reads_pipeline_values(self, guard, redis):
        from hfa.governance.budget_guard import BudgetStatus

        pipe = _make_pipe(results=["1000", "250", "active"])
        redis.pipeline = MagicMock(return_value=pipe)

        state = await guard.get_state(TENANT_ID, RUN_ID)

        assert state.tenant_id == TENANT_ID
        assert state.run_id == RUN_ID
        assert state.limit_cents == 1000
        assert state.spent_cents == 250
        assert state.remaining_cents == 750
        assert state.status == BudgetStatus.ACTIVE

        pipe.get.assert_any_call(f"{KEY_PREFIX}:{TENANT_ID}:{RUN_ID}:limit_cents")
        pipe.get.assert_any_call(f"{KEY_PREFIX}:{TENANT_ID}:{RUN_ID}:spent_cents")
        pipe.get.assert_any_call(f"{KEY_PREFIX}:{TENANT_ID}:{RUN_ID}:status")
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_freeze_calls_evalsha(self, guard, redis):
        await guard.freeze(TENANT_ID, RUN_ID)

        redis.evalsha.assert_awaited_once()
        args = redis.evalsha.call_args.args
        assert args[0] == "sha_freeze"
        assert args[1] == 1
        assert args[2].endswith(":status")

    @pytest.mark.asyncio
    async def test_reset_calls_evalsha(self, guard, redis):
        await guard.reset(TENANT_ID, RUN_ID)

        redis.evalsha.assert_awaited_once()
        args = redis.evalsha.call_args.args
        assert args[0] == "sha_reset"
        assert args[1] == 3
        assert args[2].endswith(":spent_cents")
        assert args[3].endswith(":status")
        assert args[4].endswith(":run_ids")

    @pytest.mark.asyncio
    async def test_recover_run_writes_known_good_state(self, guard, redis):
        _, pipe = _make_redis(pipe_results=[None, None, None])
        redis.pipeline = MagicMock(return_value=pipe)

        await guard.recover_run(TENANT_ID, RUN_ID, limit_cents=1000, spent_cents=250)

        limit_key, spent_key, status_key, _ = guard._keys(TENANT_ID, RUN_ID)

        pipe.set.assert_any_call(limit_key, "1000")
        pipe.set.assert_any_call(spent_key, "250")
        pipe.set.assert_any_call(status_key, "active")
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recover_run_marks_exhausted_when_spent_equals_limit(self, guard, redis):
        _, pipe = _make_redis(pipe_results=[None, None, None])
        redis.pipeline = MagicMock(return_value=pipe)

        await guard.recover_run(TENANT_ID, RUN_ID, limit_cents=1000, spent_cents=1000)

        pipe.set.assert_any_call(f"{KEY_PREFIX}:{TENANT_ID}:{RUN_ID}:status", "exhausted")

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
        assert state.idempotent is False