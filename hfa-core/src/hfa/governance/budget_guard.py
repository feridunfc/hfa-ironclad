"""
hfa-core/src/hfa/governance/budget_guard.py
IRONCLAD — Atomic budget enforcement via Redis Lua + crash recovery.

Monetary units
--------------
ALL amounts are INTEGER CENTS (1 USD = 100 cents). No float arithmetic
in the hot path — eliminates IEEE-754 drift in Redis comparisons.

  $1.00  → 100 cents
  $0.003 → 1 cent  (usd_to_cents() rounds UP via math.ceil)

Public API uses ``_cents`` suffix throughout to make the unit explicit.
Convenience helpers ``usd_to_cents()`` / ``cents_to_usd()`` are provided
for callers that work in USD floats (e.g. LLM token cost calculators).

Redis key layout
----------------
  budget:<tenant>:<run_id>:limit_cents   INT  — total budget cap
  budget:<tenant>:<run_id>:spent_cents   INT  — running total
  budget:<tenant>:<run_id>:status        STR  — active|exhausted|frozen
  budget:<tenant>:<run_id>:run_ids       SET  — idempotency set (seen run_ids)

Design — what changed in this revision
---------------------------------------
Option A — _maybe_await helper
  redis-py pipeline queue calls (pipe.set / pipe.get / pipe.setnx) are
  synchronous in production but return coroutines under AsyncMock in tests.
  Every pipeline queue call is now wrapped with ``await _maybe_await(res)``
  so the code works correctly in both environments with zero RuntimeWarning.

Option B — Idempotent Lua debit
  debit() runs a single EVALSHA that atomically:
    1. Checks if run_id was already processed (idempotency SET).
    2. Checks frozen / exhausted status.
    3. Verifies spend headroom.
    4. Debits and records run_id in idempotency set.
  A duplicate run_id call returns allowed=1, idempotent=1 and does NOT
  subtract from the budget again — safe for at-least-once retry patterns.

IRONCLAD invariants preserved:
  evalsha called with unpacked positional args (never list)
  No print() — logging only
  No asyncio.get_event_loop()
  All monetary values are integer cents
"""
from __future__ import annotations

import inspect
import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Option A — _maybe_await helper
# ---------------------------------------------------------------------------

async def _maybe_await(value: Any) -> Any:
    """
    Await ``value`` if it is a coroutine / awaitable, otherwise return as-is.

    redis-py pipeline queue operations (pipe.set, pipe.get, pipe.setnx …)
    are synchronous in production — they just append to the buffer and
    return None.  Under AsyncMock in tests they return a coroutine.

    Wrapping every pipeline queue call with ``await _maybe_await(...)``
    eliminates the RuntimeWarning: coroutine was never awaited warning
    in both environments, with no conditional logic in the caller.

    Args:
        value: Return value of a pipeline queue call.

    Returns:
        Original value or resolved coroutine result.
    """
    if inspect.isawaitable(value):
        return await value
    return value


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------

def usd_to_cents(usd: float) -> int:
    """
    Convert USD float to integer cents, rounding UP (ceil).
    Minimum debit is always 1 cent — never under-charges.

    Args:
        usd: Amount in USD (e.g. 0.003).

    Returns:
        Ceiling integer cents (e.g. 1 for 0.003).

    Raises:
        ValueError: If usd is negative.
    """
    if usd < 0:
        raise ValueError(f"usd must be non-negative, got {usd}")
    return max(1, math.ceil(usd * 100))


def cents_to_usd(cents: int) -> float:
    """Convert integer cents to USD float (display / logging only)."""
    return cents / 100.0


# ---------------------------------------------------------------------------
# Lua scripts
# ---------------------------------------------------------------------------

# ATOMIC_DEBIT_SCRIPT — Option B: idempotent + no TOCTOU, single round-trip
#
# KEYS[1] = spent_key
# KEYS[2] = status_key
# KEYS[3] = run_ids_key    Redis SET — idempotency record of seen run_ids
#
# ARGV[1] = amount_cents   int string
# ARGV[2] = limit_cents    int string
# ARGV[3] = run_id         string
#
# Returns 4-element array:
#   [allowed(0/1), new_spent_cents(str), status(str), idempotent(0/1)]
#
#   allowed=1  idempotent=0  → first debit, money taken
#   allowed=1  idempotent=1  → duplicate run_id, safe replay, no money taken
#   allowed=0  idempotent=0  → denied: frozen, exhausted, or over limit
#
_DEBIT_SCRIPT = """
local spent_key   = KEYS[1]
local status_key  = KEYS[2]
local run_ids_key = KEYS[3]
local amount      = tonumber(ARGV[1])
local limit       = tonumber(ARGV[2])
local run_id      = ARGV[3]

-- idempotency guard: if this run_id was already processed, return current
-- state without touching any counters (safe for at-least-once delivery)
if redis.call('SISMEMBER', run_ids_key, run_id) == 1 then
    local current = tonumber(redis.call('GET', spent_key) or '0')
    local status  = redis.call('GET', status_key) or 'active'
    return {1, tostring(current), status, 1}
end

-- deny immediately if frozen / exhausted (no write needed)
local status = redis.call('GET', status_key) or 'active'
if status == 'frozen' or status == 'exhausted' then
    local current = tonumber(redis.call('GET', spent_key) or '0')
    return {0, tostring(current), status, 0}
end

-- deny if headroom is insufficient
local current = tonumber(redis.call('GET', spent_key) or '0')
if (current + amount) > limit then
    return {0, tostring(current), status, 0}
end

-- atomic debit + record run_id in idempotency set
local new_spent = current + amount
redis.call('SET',  spent_key, tostring(new_spent))
redis.call('SADD', run_ids_key, run_id)

local new_status = 'active'
if new_spent >= limit then
    new_status = 'exhausted'
    redis.call('SET', status_key, 'exhausted')
else
    -- defensive re-read: concurrent freeze may have arrived between our
    -- initial status read and this write
    local existing = redis.call('GET', status_key)
    if existing == 'frozen' then
        new_status = 'frozen'
    else
        redis.call('SET', status_key, 'active')
    end
end

return {1, tostring(new_spent), new_status, 0}
"""

# FREEZE_SCRIPT — KEYS[1]=status_key
_FREEZE_SCRIPT = """
redis.call('SET', KEYS[1], 'frozen')
return 1
"""

# RESET_SCRIPT — clears spent, status, AND idempotency run_ids set
# After reset the same run_ids can be re-processed.
# KEYS[1]=spent_key  KEYS[2]=status_key  KEYS[3]=run_ids_key
_RESET_SCRIPT = """
redis.call('SET', KEYS[1], '0')
redis.call('SET', KEYS[2], 'active')
redis.call('DEL', KEYS[3])
return 1
"""


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

class BudgetStatus(str, Enum):
    ACTIVE    = "active"
    EXHAUSTED = "exhausted"
    FROZEN    = "frozen"


@dataclass(frozen=True)
class BudgetState:
    """
    Current snapshot of a run's budget.
    All ``_cents`` fields are integer cents; ``_usd`` properties are
    convenience views for logging only — never use for arithmetic.
    """
    tenant_id:       str
    run_id:          str
    spent_cents:     int
    limit_cents:     int
    status:          BudgetStatus
    idempotent:      bool = False     # True when debit was a safe replay
    remaining_cents: int  = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "remaining_cents", max(0, self.limit_cents - self.spent_cents)
        )

    @property
    def spent_usd(self) -> float:
        return cents_to_usd(self.spent_cents)

    @property
    def limit_usd(self) -> float:
        return cents_to_usd(self.limit_cents)

    @property
    def remaining_usd(self) -> float:
        return cents_to_usd(self.remaining_cents)


class BudgetExhaustedError(Exception):
    """Raised when a debit is attempted on an exhausted or frozen budget."""

    def __init__(
        self,
        tenant_id: str,
        run_id: str,
        attempted_cents: int,
        remaining_cents: int,
    ) -> None:
        self.tenant_id       = tenant_id
        self.run_id          = run_id
        self.attempted_cents = attempted_cents
        self.remaining_cents = remaining_cents
        super().__init__(
            f"Budget exhausted for {tenant_id}/{run_id}: "
            f"attempted={attempted_cents}¢ remaining={remaining_cents}¢"
        )


class BudgetGuardError(Exception):
    """Raised on infrastructure failure (Redis unavailable, etc.)."""


# ---------------------------------------------------------------------------
# BudgetGuard
# ---------------------------------------------------------------------------

class BudgetGuard:
    """
    Atomic, idempotent, Redis-backed budget enforcement.
    All amounts are integer CENTS.

    Usage
    -----
        guard = BudgetGuard(redis_client)
        await guard.initialise()

        await guard.set_budget("acme", "run-acme-abc", limit_cents=500)
        s1 = await guard.debit("acme", "run-acme-abc", amount_cents=10)
        assert not s1.idempotent          # first call

        s2 = await guard.debit("acme", "run-acme-abc", amount_cents=10)
        assert s2.idempotent              # safe replay, not double-charged

    Args:
        redis:      aioredis.Redis (or compatible async client).
        key_prefix: Redis key namespace (default "budget").
        fail_open:  When True, allow spend on Redis failure.
                    Default False → fail-CLOSED (deny on outage).
    """

    def __init__(
        self,
        redis,
        key_prefix: str = "budget",
        fail_open: bool = False,
    ) -> None:
        self._redis     = redis
        self._prefix    = key_prefix
        self._fail_open = fail_open
        self._sha_debit:  Optional[str] = None
        self._sha_freeze: Optional[str] = None
        self._sha_reset:  Optional[str] = None
        logger.info("BudgetGuard created (fail_open=%s)", fail_open)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialise(self) -> None:
        """
        Load Lua scripts into Redis and cache their SHAs.
        Must be called once before any debit/check.

        Raises:
            BudgetGuardError: If Redis is unreachable.
        """
        try:
            self._sha_debit  = await self._redis.script_load(_DEBIT_SCRIPT)
            self._sha_freeze = await self._redis.script_load(_FREEZE_SCRIPT)
            self._sha_reset  = await self._redis.script_load(_RESET_SCRIPT)
            logger.info(
                "BudgetGuard Lua scripts loaded: sha_debit=%s…", self._sha_debit[:8]
            )
        except Exception as exc:
            logger.critical("BudgetGuard.initialise failed: %s", exc, exc_info=True)
            raise BudgetGuardError("Failed to load Lua scripts") from exc

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def set_budget(
        self,
        tenant_id: str,
        run_id: str,
        limit_cents: int,
        reset_spent: bool = False,
    ) -> None:
        """
        Create or update the budget limit for a run.

        Args:
            tenant_id:   Tenant identifier.
            run_id:      Run identifier.
            limit_cents: Maximum allowed spend in INTEGER CENTS (e.g. 500 = $5.00).
            reset_spent: If True, zero out spent counter and clear idempotency set.

        Raises:
            TypeError:        If limit_cents is not int.
            ValueError:       If limit_cents <= 0.
            BudgetGuardError: On Redis failure.
        """
        if not isinstance(limit_cents, int):
            raise TypeError(
                f"limit_cents must be int, got {type(limit_cents).__name__}. "
                "Use usd_to_cents() to convert."
            )
        if limit_cents <= 0:
            raise ValueError(f"limit_cents must be positive, got {limit_cents}")

        limit_key, spent_key, status_key, run_ids_key = self._keys(tenant_id, run_id)

        try:
            pipe = self._redis.pipeline()
            # ✅ Option A: _maybe_await wraps every pipeline queue call
            await _maybe_await(pipe.set(limit_key, str(limit_cents)))
            if reset_spent:
                await _maybe_await(pipe.set(spent_key, "0"))
                await _maybe_await(pipe.set(status_key, "active"))
                await _maybe_await(pipe.delete(run_ids_key))
            else:
                await _maybe_await(pipe.setnx(spent_key, "0"))
                await _maybe_await(pipe.setnx(status_key, "active"))
            await pipe.execute()
            logger.info(
                "Budget set: tenant=%s run=%s limit=%d¢ ($%.2f) reset=%s",
                tenant_id, run_id, limit_cents, cents_to_usd(limit_cents), reset_spent,
            )
        except Exception as exc:
            logger.error("set_budget failed: %s", exc, exc_info=True)
            raise BudgetGuardError("set_budget Redis failure") from exc

    async def debit(
        self,
        tenant_id: str,
        run_id: str,
        amount_cents: int,
    ) -> BudgetState:
        """
        Atomically debit amount_cents from the run's budget.

        Idempotent: calling debit() twice with the same run_id is safe —
        the second call returns allowed=1, state.idempotent=True and does
        NOT subtract from the budget again.

        Args:
            tenant_id:    Tenant identifier.
            run_id:       Run identifier (also used as idempotency key).
            amount_cents: INTEGER CENTS to debit (>= 1).
                          Use usd_to_cents(float) to convert from USD.

        Returns:
            BudgetState after the debit.

        Raises:
            TypeError:            If amount_cents is not int.
            ValueError:           If amount_cents < 1.
            BudgetExhaustedError: If budget is exhausted or frozen.
            BudgetGuardError:     On Redis failure (fail-closed by default).
        """
        if not isinstance(amount_cents, int):
            raise TypeError(
                f"amount_cents must be int, got {type(amount_cents).__name__}. "
                "Use usd_to_cents() to convert."
            )
        if amount_cents < 1:
            raise ValueError(f"amount_cents must be >= 1, got {amount_cents}")

        self._assert_initialised()

        limit_key, spent_key, status_key, run_ids_key = self._keys(tenant_id, run_id)

        try:
            limit_cents = await self._get_limit_cents(limit_key, tenant_id, run_id)

            # ✅ Option B: single Lua round-trip — check + idempotency + debit
            # KEYS: spent_key, status_key, run_ids_key (3 keys)
            # ARGV: amount_cents, limit_cents, run_id
            # Returns: [allowed, new_spent, status, idempotent]
            result = await self._redis.evalsha(
                self._sha_debit,
                3,              # ✅ IRONCLAD: num_keys as unpacked int, not list
                spent_key,
                status_key,
                run_ids_key,
                str(amount_cents),
                str(limit_cents),
                run_id,
            )

            allowed         = int(result[0])
            new_spent_cents = int(result[1])
            status          = BudgetStatus(result[2])
            idempotent      = bool(int(result[3]))

            if not allowed:
                raise BudgetExhaustedError(
                    tenant_id,
                    run_id,
                    amount_cents,
                    max(0, limit_cents - new_spent_cents),
                )

            state = BudgetState(
                tenant_id=tenant_id,
                run_id=run_id,
                spent_cents=new_spent_cents,
                limit_cents=limit_cents,
                status=status,
                idempotent=idempotent,
            )
            logger.info(
                "Debit %s: tenant=%s run=%s amount=%dc spent=%dc/%dc status=%s",
                "REPLAY" if idempotent else "OK",
                tenant_id, run_id,
                amount_cents, new_spent_cents, limit_cents,
                status.value,
            )
            return state

        except BudgetExhaustedError:
            raise
        except Exception as exc:
            logger.error("debit failed: %s", exc, exc_info=True)
            if self._fail_open:
                logger.warning("fail_open=True — permitting spend despite Redis error")
                return BudgetState(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    spent_cents=0,
                    limit_cents=2_147_483_647,  # INT_MAX sentinel
                    status=BudgetStatus.ACTIVE,
                )
            raise BudgetGuardError("debit Redis failure") from exc

    async def get_state(self, tenant_id: str, run_id: str) -> BudgetState:
        """
        Read current budget state without modifying it.

        Raises:
            BudgetGuardError: On Redis failure or if budget not initialised.
        """
        limit_key, spent_key, status_key, _ = self._keys(tenant_id, run_id)
        try:
            pipe = self._redis.pipeline()
            # ✅ Option A: _maybe_await on every pipeline queue call
            await _maybe_await(pipe.get(limit_key))
            await _maybe_await(pipe.get(spent_key))
            await _maybe_await(pipe.get(status_key))
            limit_raw, spent_raw, status_raw = await pipe.execute()

            if limit_raw is None:
                raise BudgetGuardError(
                    f"Budget not initialised for {tenant_id}/{run_id}"
                )

            return BudgetState(
                tenant_id=tenant_id,
                run_id=run_id,
                spent_cents=int(spent_raw or 0),
                limit_cents=int(limit_raw),
                status=BudgetStatus(status_raw or "active"),
            )
        except BudgetGuardError:
            raise
        except Exception as exc:
            logger.error("get_state failed: %s", exc, exc_info=True)
            raise BudgetGuardError("get_state Redis failure") from exc

    async def freeze(self, tenant_id: str, run_id: str) -> None:
        """Operator emergency stop — freeze the run's budget."""
        self._assert_initialised()
        _, _, status_key, _ = self._keys(tenant_id, run_id)
        try:
            await self._redis.evalsha(
                self._sha_freeze, 1, status_key  # ✅ unpacked
            )
            logger.warning("Budget FROZEN: tenant=%s run=%s", tenant_id, run_id)
        except Exception as exc:
            logger.error("freeze failed: %s", exc, exc_info=True)
            raise BudgetGuardError("freeze Redis failure") from exc

    async def reset(self, tenant_id: str, run_id: str) -> None:
        """
        Reset spent counter, unfreeze budget, and clear the idempotency set.
        Use after human review.  After reset, same run_ids can be re-debited.
        """
        self._assert_initialised()
        _, spent_key, status_key, run_ids_key = self._keys(tenant_id, run_id)
        try:
            await self._redis.evalsha(
                self._sha_reset, 3, spent_key, status_key, run_ids_key  # ✅ unpacked
            )
            logger.info("Budget RESET: tenant=%s run=%s", tenant_id, run_id)
        except Exception as exc:
            logger.error("reset failed: %s", exc, exc_info=True)
            raise BudgetGuardError("reset Redis failure") from exc

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    async def recover_run(
        self,
        tenant_id: str,
        run_id: str,
        limit_cents: int,
        spent_cents: int,
    ) -> None:
        """
        Crash-recovery: restore known-good state after node restart.
        Replays event-log values; does NOT zero out spent (by design).

        Note: The idempotency run_ids set is preserved — replayed run_ids
        are still recognised.  Call reset() first for a full clean slate.

        Args:
            tenant_id:   Tenant identifier.
            run_id:      Run identifier.
            limit_cents: Budget cap (integer cents).
            spent_cents: Known spent amount from event replay (integer cents).

        Raises:
            TypeError:        If arguments are not int.
            BudgetGuardError: On Redis failure.
        """
        for name, val in (("limit_cents", limit_cents), ("spent_cents", spent_cents)):
            if not isinstance(val, int):
                raise TypeError(f"{name} must be int, got {type(val).__name__}")

        limit_key, spent_key, status_key, _ = self._keys(tenant_id, run_id)
        try:
            status = "exhausted" if spent_cents >= limit_cents else "active"
            pipe = self._redis.pipeline()
            # ✅ Option A: _maybe_await on every pipeline queue call
            await _maybe_await(pipe.set(limit_key,  str(limit_cents)))
            await _maybe_await(pipe.set(spent_key,  str(spent_cents)))
            await _maybe_await(pipe.set(status_key, status))
            await pipe.execute()
            logger.info(
                "Budget RECOVERED: tenant=%s run=%s spent=%dc/%dc status=%s",
                tenant_id, run_id, spent_cents, limit_cents, status,
            )
        except Exception as exc:
            logger.error("recover_run failed: %s", exc, exc_info=True)
            raise BudgetGuardError("recover_run Redis failure") from exc

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _keys(self, tenant_id: str, run_id: str) -> tuple[str, str, str, str]:
        """Return (limit_key, spent_key, status_key, run_ids_key)."""
        base = f"{self._prefix}:{tenant_id}:{run_id}"
        return (
            f"{base}:limit_cents",
            f"{base}:spent_cents",
            f"{base}:status",
            f"{base}:run_ids",     # idempotency set — Option B
        )

    async def _get_limit_cents(
        self, limit_key: str, tenant_id: str, run_id: str
    ) -> int:
        raw = await self._redis.get(limit_key)
        if raw is None:
            raise BudgetGuardError(
                f"Budget not initialised for {tenant_id}/{run_id}"
            )
        return int(raw)

    def _assert_initialised(self) -> None:
        if self._sha_debit is None:
            raise BudgetGuardError(
                "BudgetGuard not initialised — call await guard.initialise() first"
            )
