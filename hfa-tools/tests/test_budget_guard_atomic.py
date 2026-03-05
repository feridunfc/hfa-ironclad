"""
hfa-tools/tests/test_budget_guard_atomic.py
IRONCLAD Sprint 2 — BudgetGuard Option A + B tests.

Test structure
--------------
Unit tests (no Redis required)
  TestMaybeAwait          — _maybe_await helper: coroutine vs plain value
  TestBudgetGuardUnit     — mock Redis: evalsha args, pipeline wrapping,
                            idempotency, error paths
  TestBudgetStateModel    — dataclass invariants + idempotent flag

Integration tests (pytest.mark.integration — require real Redis on 127.0.0.1:6379/15)
  TestBudgetGuardIntegration — full pipeline: set_budget, debit, idempotency,
                               freeze, recover, exhaustion, concurrency

Run only unit tests (default):
    pytest hfa-tools/tests/test_budget_guard_atomic.py

Run everything including integration:
    pytest hfa-tools/tests/test_budget_guard_atomic.py --integration
"""
from __future__ import annotations

import asyncio
import inspect
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# pytest marker setup
# ---------------------------------------------------------------------------

def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: requires a running Redis on 127.0.0.1:6379/15",
    )


def pytest_addoption(parser):
    try:
        parser.addoption(
            "--integration",
            action="store_true",
            default=False,
            help="Also run integration tests that require Redis",
        )
    except ValueError:
        pass


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--integration", default=False):
        skip_integration = pytest.mark.skip(
            reason="Pass --integration to run tests that require Redis"
        )
        for item in items:
            if item.get_closest_marker("integration"):
                item.add_marker(skip_integration)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _make_mock_redis(
    script_shas=("sha_debit", "sha_freeze", "sha_reset"),
    get_return="500",
    evalsha_return=None,
    pipeline_results=None,
):
    """
    Build a fully mock aioredis.Redis where:
      - script_load() returns SHAs in order
      - get() returns get_return
      - evalsha() returns evalsha_return
      - pipeline().execute() returns pipeline_results

    All pipeline queue calls (set/get/setnx/delete) return None by default
    to match production redis-py behaviour.
    """
    redis = AsyncMock()
    redis.script_load = AsyncMock(side_effect=list(script_shas))
    redis.get         = AsyncMock(return_value=get_return)

    if evalsha_return is None:
        evalsha_return = ["1", "10", "active", "0"]
    redis.evalsha = AsyncMock(return_value=evalsha_return)

    pipe = MagicMock()
    # Pipeline queue calls are sync None in production
    pipe.set    = MagicMock(return_value=None)
    pipe.get    = MagicMock(return_value=None)
    pipe.setnx  = MagicMock(return_value=None)
    pipe.delete = MagicMock(return_value=None)
    pipe.incr   = MagicMock(return_value=None)
    pipe.expire = MagicMock(return_value=None)
    if pipeline_results is None:
        pipeline_results = [None, None, None]
    pipe.execute = AsyncMock(return_value=pipeline_results)
    redis.pipeline = MagicMock(return_value=pipe)

    return redis, pipe


async def _initialised_guard(redis=None, **kw):
    from hfa.governance.budget_guard import BudgetGuard

    if redis is None:
        redis, _ = _make_mock_redis()
    guard = BudgetGuard(redis, **kw)
    await guard.initialise()
    return guard, redis


# ===========================================================================
# TestMaybeAwait
# ===========================================================================

class TestMaybeAwait:
    """_maybe_await helper: ensures both coroutines and plain values are handled."""

    @pytest.mark.asyncio
    async def test_plain_value_returned_as_is(self):
        from hfa.governance.budget_guard import _maybe_await

        result = await _maybe_await(None)
        assert result is None

        result = await _maybe_await(42)
        assert result == 42

        result = await _maybe_await("hello")
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_coroutine_is_awaited(self):
        from hfa.governance.budget_guard import _maybe_await

        async def _coro():
            return "resolved"

        result = await _maybe_await(_coro())
        assert result == "resolved"

    @pytest.mark.asyncio
    async def test_async_mock_return_is_awaited(self):
        """Simulates the AsyncMock.set() scenario in tests."""
        from hfa.governance.budget_guard import _maybe_await

        mock_set = AsyncMock(return_value="ok")
        coro = mock_set("some_key", "some_val")
        assert inspect.isawaitable(coro)

        result = await _maybe_await(coro)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_no_runtime_warning_with_async_mock(self):
        """
        Ensure no 'coroutine was never awaited' RuntimeWarning.
        We capture warnings to assert none are raised.
        """
        import warnings
        from hfa.governance.budget_guard import _maybe_await

        async_fn = AsyncMock(return_value=None)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await _maybe_await(async_fn("key"))

        runtime_warnings = [
            w for w in caught
            if "coroutine" in str(w.message).lower()
        ]
        assert not runtime_warnings, f"Got RuntimeWarning: {runtime_warnings}"


# ===========================================================================
# TestBudgetStateModel
# ===========================================================================

class TestBudgetStateModel:
    """BudgetState dataclass: remaining_cents computation + idempotent flag."""

    def test_remaining_cents_computed_correctly(self):
        from hfa.governance.budget_guard import BudgetState, BudgetStatus

        state = BudgetState(
            tenant_id="acme", run_id="run-acme-001",
            spent_cents=300, limit_cents=500,
            status=BudgetStatus.ACTIVE,
        )
        assert state.remaining_cents == 200

    def test_remaining_cents_floored_at_zero(self):
        from hfa.governance.budget_guard import BudgetState, BudgetStatus

        state = BudgetState(
            tenant_id="acme", run_id="run-acme-001",
            spent_cents=600, limit_cents=500,
            status=BudgetStatus.EXHAUSTED,
        )
        assert state.remaining_cents == 0

    def test_idempotent_false_by_default(self):
        from hfa.governance.budget_guard import BudgetState, BudgetStatus

        state = BudgetState(
            tenant_id="acme", run_id="run-acme-001",
            spent_cents=0, limit_cents=500,
            status=BudgetStatus.ACTIVE,
        )
        assert state.idempotent is False

    def test_idempotent_flag_set(self):
        from hfa.governance.budget_guard import BudgetState, BudgetStatus

        state = BudgetState(
            tenant_id="acme", run_id="run-acme-001",
            spent_cents=10, limit_cents=500,
            status=BudgetStatus.ACTIVE,
            idempotent=True,
        )
        assert state.idempotent is True

    def test_usd_properties_are_display_only(self):
        from hfa.governance.budget_guard import BudgetState, BudgetStatus

        state = BudgetState(
            tenant_id="acme", run_id="run-acme-001",
            spent_cents=100, limit_cents=500,
            status=BudgetStatus.ACTIVE,
        )
        assert state.spent_usd == 1.0
        assert state.limit_usd == 5.0
        assert state.remaining_usd == 4.0


# ===========================================================================
# TestBudgetGuardUnit
# ===========================================================================

class TestBudgetGuardUnit:
    """Unit tests with fully mocked Redis — no network required."""

    # ── Initialise ──────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_initialise_loads_exactly_three_scripts(self):
        redis, _ = _make_mock_redis()
        guard, redis = await _initialised_guard(redis)
        assert redis.script_load.call_count == 3

    @pytest.mark.asyncio
    async def test_initialise_sets_sha_fields(self):
        guard, _ = await _initialised_guard()
        assert guard._sha_debit  == "sha_debit"
        assert guard._sha_freeze == "sha_freeze"
        assert guard._sha_reset  == "sha_reset"

    @pytest.mark.asyncio
    async def test_debit_before_initialise_raises(self):
        from hfa.governance.budget_guard import BudgetGuard, BudgetGuardError

        redis, _ = _make_mock_redis()
        guard = BudgetGuard(redis)
        with pytest.raises(BudgetGuardError, match="not initialised"):
            await guard.debit("acme", "run-acme-001", 10)

    # ── Option B: evalsha arg layout ────────────────────────────────────

    @pytest.mark.asyncio
    async def test_debit_evalsha_uses_3_keys_unpacked(self):
        """
        IRONCLAD rule + Option B:
        evalsha(sha, 3, spent_key, status_key, run_ids_key, amount, limit, run_id)
        num_keys must be the integer 3, not a list.
        """
        guard, redis = await _initialised_guard()
        redis.evalsha = AsyncMock(return_value=["1", "10", "active", "0"])

        await guard.debit("acme", "run-acme-001", 10)

        call_args = redis.evalsha.call_args.args
        assert call_args[0] == "sha_debit"
        assert call_args[1] == 3,           f"num_keys must be int 3, got {call_args[1]}"
        assert isinstance(call_args[2], str), "spent_key must be string"
        assert isinstance(call_args[3], str), "status_key must be string"
        assert isinstance(call_args[4], str), "run_ids_key must be string"
        assert call_args[5] == "10",          "amount_cents as string"
        assert call_args[6] == "500",         "limit_cents as string"
        assert call_args[7] == "run-acme-001", "run_id passed to Lua"

    @pytest.mark.asyncio
    async def test_debit_no_nested_list_args(self):
        """Regression: evalsha must never receive a list as KEYS or ARGV."""
        guard, redis = await _initialised_guard()
        redis.evalsha = AsyncMock(return_value=["1", "10", "active", "0"])

        await guard.debit("acme", "run-acme-001", 10)

        for i, arg in enumerate(redis.evalsha.call_args.args):
            assert not isinstance(arg, list), \
                f"evalsha arg[{i}]={arg!r} must not be a list"

    # ── Option B: idempotency ────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_debit_first_call_idempotent_false(self):
        guard, redis = await _initialised_guard()
        redis.evalsha = AsyncMock(return_value=["1", "10", "active", "0"])

        state = await guard.debit("acme", "run-acme-001", 10)
        assert state.idempotent is False

    @pytest.mark.asyncio
    async def test_debit_duplicate_run_id_idempotent_true(self):
        """Lua returns idempotent=1 → state.idempotent=True, no double charge."""
        guard, redis = await _initialised_guard()
        # First call: fresh debit
        redis.evalsha = AsyncMock(
            side_effect=[
                ["1", "10", "active", "0"],   # first call
                ["1", "10", "active", "1"],   # second call — idempotent replay
            ]
        )

        s1 = await guard.debit("acme", "run-acme-001", 10)
        s2 = await guard.debit("acme", "run-acme-001", 10)

        assert s1.idempotent is False
        assert s2.idempotent is True
        assert s2.spent_cents == s1.spent_cents  # no double charge

    @pytest.mark.asyncio
    async def test_debit_run_ids_key_passed_to_lua(self):
        """run_ids_key must include ':run_ids' suffix."""
        guard, redis = await _initialised_guard()
        redis.evalsha = AsyncMock(return_value=["1", "10", "active", "0"])

        await guard.debit("acme", "run-acme-001", 10)

        call_args = redis.evalsha.call_args.args
        run_ids_key = call_args[4]  # KEYS[3]
        assert run_ids_key.endswith(":run_ids"), \
            f"run_ids_key must end with ':run_ids', got {run_ids_key!r}"

    # ── denied / error paths ─────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_debit_denied_frozen_raises(self):
        from hfa.governance.budget_guard import BudgetExhaustedError

        guard, redis = await _initialised_guard()
        redis.evalsha = AsyncMock(return_value=["0", "500", "frozen", "0"])

        with pytest.raises(BudgetExhaustedError):
            await guard.debit("acme", "run-acme-001", 10)

    @pytest.mark.asyncio
    async def test_debit_denied_exhausted_raises(self):
        from hfa.governance.budget_guard import BudgetExhaustedError

        guard, redis = await _initialised_guard()
        redis.evalsha = AsyncMock(return_value=["0", "500", "exhausted", "0"])

        with pytest.raises(BudgetExhaustedError) as exc_info:
            await guard.debit("acme", "run-acme-001", 10)

        assert exc_info.value.attempted_cents == 10

    @pytest.mark.asyncio
    async def test_debit_redis_failure_fail_closed(self):
        from hfa.governance.budget_guard import BudgetGuardError

        guard, redis = await _initialised_guard()
        redis.evalsha = AsyncMock(side_effect=ConnectionError("Redis down"))

        with pytest.raises(BudgetGuardError, match="debit Redis failure"):
            await guard.debit("acme", "run-acme-001", 10)

    @pytest.mark.asyncio
    async def test_debit_redis_failure_fail_open(self):
        guard, redis = await _initialised_guard(fail_open=True)
        redis.evalsha = AsyncMock(side_effect=ConnectionError("Redis down"))

        state = await guard.debit("acme", "run-acme-001", 10)
        assert state.spent_cents == 0
        assert state.limit_cents == 2_147_483_647

    @pytest.mark.asyncio
    async def test_debit_rejects_float_amount(self):
        from hfa.governance.budget_guard import BudgetGuardError

        guard, redis = await _initialised_guard()

        with pytest.raises(TypeError, match="amount_cents must be int"):
            await guard.debit("acme", "run-acme-001", 0.1)

    @pytest.mark.asyncio
    async def test_debit_rejects_zero_amount(self):
        guard, redis = await _initialised_guard()

        with pytest.raises(ValueError, match=">="):
            await guard.debit("acme", "run-acme-001", 0)

    # ── Option A: pipeline wrapping ─────────────────────────────────────

    @pytest.mark.asyncio
    async def test_set_budget_no_runtime_warning_with_async_mock(self):
        """
        Option A regression: if pipe.set returns a coroutine (AsyncMock),
        _maybe_await must prevent RuntimeWarning: coroutine was never awaited.
        """
        import warnings

        redis = AsyncMock()
        redis.script_load = AsyncMock(side_effect=["sha_debit", "sha_freeze", "sha_reset"])
        redis.get = AsyncMock(return_value="500")

        pipe = AsyncMock()
        pipe.execute = AsyncMock(return_value=[None, None, None])
        redis.pipeline = MagicMock(return_value=pipe)

        guard = __import__("hfa.governance.budget_guard", fromlist=["BudgetGuard"]).BudgetGuard
        g = guard(redis)
        await g.initialise()

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await g.set_budget("acme", "run-acme-001", 500)

        runtime_warnings = [
            w for w in caught
            if "coroutine" in str(w.message).lower()
        ]
        assert not runtime_warnings, f"RuntimeWarnings: {runtime_warnings}"

    @pytest.mark.asyncio
    async def test_recover_run_no_runtime_warning(self):
        import warnings
        from hfa.governance.budget_guard import BudgetGuard

        redis = AsyncMock()
        redis.script_load = AsyncMock(side_effect=["sha_debit", "sha_freeze", "sha_reset"])
        redis.get = AsyncMock(return_value="500")

        pipe = AsyncMock()
        pipe.execute = AsyncMock(return_value=[None, None, None])
        redis.pipeline = MagicMock(return_value=pipe)

        g = BudgetGuard(redis)
        await g.initialise()

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await g.recover_run("acme", "run-acme-001", 500, 200)

        runtime_warnings = [
            w for w in caught
            if "coroutine" in str(w.message).lower()
        ]
        assert not runtime_warnings, f"RuntimeWarnings: {runtime_warnings}"

    @pytest.mark.asyncio
    async def test_pipeline_execute_still_awaited(self):
        """pipe.execute() must always be awaited regardless of queue call type."""
        redis, pipe = _make_mock_redis()
        guard = __import__("hfa.governance.budget_guard", fromlist=["BudgetGuard"]).BudgetGuard
        g = guard(redis)
        await g.initialise()

        await g.set_budget("acme", "run-acme-001", 500)

        pipe.execute.assert_awaited_once()

    # ── reset / freeze keys ─────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_reset_evalsha_passes_run_ids_key(self):
        """reset() must pass run_ids_key as KEYS[3] to Lua."""
        guard, redis = await _initialised_guard()
        redis.evalsha = AsyncMock(return_value=1)

        await guard.reset("acme", "run-acme-001")

        call_args = redis.evalsha.call_args.args
        assert call_args[1] == 3, "reset must pass num_keys=3"
        run_ids_key = call_args[4]
        assert run_ids_key.endswith(":run_ids")

    @pytest.mark.asyncio
    async def test_freeze_evalsha_passes_1_key(self):
        guard, redis = await _initialised_guard()
        redis.evalsha = AsyncMock(return_value=1)

        await guard.freeze("acme", "run-acme-001")

        call_args = redis.evalsha.call_args.args
        assert call_args[1] == 1, "freeze must pass num_keys=1"

    # ── _keys helper ─────────────────────────────────────────────────────

    def test_keys_returns_four_tuple(self):
        from hfa.governance.budget_guard import BudgetGuard
        g = BudgetGuard(MagicMock())
        keys = g._keys("acme_corp", "run-acme_corp-uuid")
        assert len(keys) == 4
        assert keys[0].endswith(":limit_cents")
        assert keys[1].endswith(":spent_cents")
        assert keys[2].endswith(":status")
        assert keys[3].endswith(":run_ids")

    def test_keys_prefix_respected(self):
        from hfa.governance.budget_guard import BudgetGuard
        g = BudgetGuard(MagicMock(), key_prefix="myns")
        keys = g._keys("t1", "r1")
        assert all(k.startswith("myns:t1:r1:") for k in keys)


# ===========================================================================
# Integration tests (require Redis on 127.0.0.1:6379/15)
# ===========================================================================

@pytest.fixture
async def real_redis():
    """Async fixture — returns a real aioredis.Redis on DB 15."""
    try:
        import redis.asyncio as aioredis
    except ImportError:
        pytest.skip("redis-py not installed")

    r = aioredis.Redis(host="127.0.0.1", port=6379, db=15, decode_responses=True)
    try:
        await r.ping()
    except Exception:
        pytest.skip("Redis not available on 127.0.0.1:6379")
    yield r
    await r.flushdb()
    await r.aclose()


@pytest.fixture
async def guard(real_redis):
    from hfa.governance.budget_guard import BudgetGuard

    g = BudgetGuard(real_redis)
    await g.initialise()
    return g


RUN_ID = "run-acme_corp-550e8400-e29b-41d4-a716-446655440000"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_basic_debit(guard):
    await guard.set_budget("acme_corp", RUN_ID, limit_cents=500)
    state = await guard.debit("acme_corp", RUN_ID, 50)

    assert state.spent_cents == 50
    assert state.remaining_cents == 450
    assert state.idempotent is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_idempotent_debit(guard):
    """Same run_id debited twice — second call is idempotent replay."""
    await guard.set_budget("acme_corp", RUN_ID, limit_cents=500)

    s1 = await guard.debit("acme_corp", RUN_ID, 50)
    s2 = await guard.debit("acme_corp", RUN_ID, 50)

    assert s1.idempotent is False
    assert s2.idempotent is True
    assert s2.spent_cents == s1.spent_cents  # no double-charge


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_exhaustion(guard):
    from hfa.governance.budget_guard import BudgetExhaustedError

    await guard.set_budget("acme_corp", RUN_ID, limit_cents=100)
    await guard.debit("acme_corp", RUN_ID, 100)

    state = await guard.get_state("acme_corp", RUN_ID)
    assert state.status.value == "exhausted"

    with pytest.raises(BudgetExhaustedError):
        await guard.debit("acme_corp", RUN_ID + "-second", 1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_freeze_denies(guard):
    from hfa.governance.budget_guard import BudgetExhaustedError

    await guard.set_budget("acme_corp", RUN_ID, limit_cents=500)
    await guard.freeze("acme_corp", RUN_ID)

    with pytest.raises(BudgetExhaustedError):
        await guard.debit("acme_corp", RUN_ID + "-second", 10)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_recover_preserves_spent(guard):
    """recover_run must restore state without zeroing spent."""
    await guard.recover_run("acme_corp", RUN_ID, 500, 200)
    state = await guard.get_state("acme_corp", RUN_ID)

    assert state.spent_cents == 200
    assert state.limit_cents == 500
    assert state.status.value == "active"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_recover_exhausted_when_spent_gte_limit(guard):
    await guard.recover_run("acme_corp", RUN_ID, 500, 500)
    state = await guard.get_state("acme_corp", RUN_ID)
    assert state.status.value == "exhausted"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_reset_clears_idempotency_set(guard):
    """After reset(), the same run_id can be debited again (not idempotent replay)."""
    await guard.set_budget("acme_corp", RUN_ID, limit_cents=500)
    s1 = await guard.debit("acme_corp", RUN_ID, 50)
    assert s1.idempotent is False

    await guard.reset("acme_corp", RUN_ID)

    # Re-budget after reset
    await guard.set_budget("acme_corp", RUN_ID, limit_cents=500)
    s2 = await guard.debit("acme_corp", RUN_ID, 50)
    assert s2.idempotent is False  # fresh debit, not replay


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_concurrent_debits(guard):
    """
    N concurrent workers each debit 10 cents against a 100-cent budget.
    Total spent must never exceed 100 — atomic Lua prevents TOCTOU.
    """
    limit = 100
    per_worker = 10
    num_workers = 20   # more workers than budget allows
    await guard.set_budget("acme_corp", RUN_ID, limit_cents=limit)

    async def _worker(n: int):
        from hfa.governance.budget_guard import BudgetExhaustedError
        run = f"{RUN_ID}-w{n}"
        try:
            state = await guard.debit("acme_corp", run, per_worker)
            return state.spent_cents
        except BudgetExhaustedError:
            return None

    results = await asyncio.gather(*(_worker(i) for i in range(num_workers)))

    successful = [r for r in results if r is not None]
    # At most limit // per_worker workers can succeed
    assert len(successful) <= limit // per_worker

    state = await guard.get_state("acme_corp", RUN_ID)
    assert state.spent_cents <= limit


@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_pipeline_option_a_no_warning(guard):
    """
    Option A regression: no RuntimeWarning with real Redis (smoke test).
    """
    import warnings
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await guard.set_budget("acme_corp", RUN_ID, limit_cents=500)
        await guard.recover_run("acme_corp", RUN_ID, 500, 100)

    runtime_warnings = [
        w for w in caught
        if "coroutine" in str(w.message).lower()
    ]
    assert not runtime_warnings
