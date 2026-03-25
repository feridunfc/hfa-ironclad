
"""
hfa-core/src/hfa/state/__init__.py

Single authoritative state transition gateway.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from weakref import WeakKeyDictionary

logger = logging.getLogger(__name__)

VALID_TRANSITIONS: dict[str, frozenset[str]] = {
    "admitted": frozenset({"queued", "scheduled", "rejected", "failed"}),
    "queued": frozenset({"scheduled", "failed", "rejected"}),
    "scheduled": frozenset({"running", "failed"}),
    "running": frozenset({"done", "failed", "dead_lettered", "rescheduled"}),
    "rescheduled": frozenset({"admitted", "failed"}),
    "done": frozenset(),
    "failed": frozenset(),
    "rejected": frozenset(),
    "dead_lettered": frozenset(),
}
TERMINAL_STATES: frozenset[str] = frozenset({"done", "failed", "rejected", "dead_lettered"})


class InvalidStateTransition(Exception):
    """Raised when a transition is rejected by the state machine."""


@dataclass(frozen=True)
class TransitionResult:
    ok: bool
    reason: str
    from_state: Optional[str]
    to_state: str
    run_id: str

    COMMITTED = "committed"
    CAS_MISS = "cas_miss"
    INITIAL_WRITE_BLOCKED = "initial_write_blocked"
    ILLEGAL_TRANSITION = "illegal_transition"
    TERMINAL_BLOCKED = "terminal_blocked"
    UNKNOWN_STATE = "unknown_state_fail_closed"
    LUA_UNAVAILABLE = "lua_unavailable"

    def __bool__(self) -> bool:
        return self.ok


def _strict_cas_mode() -> bool:
    env_flag = os.getenv("HFA_STRICT_CAS_MODE", "").strip().lower()
    if env_flag in {"1", "true", "yes", "on"}:
        return True
    app_env = (os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "development").strip().lower()
    return app_env in {"production", "staging"}


def validate_transition(
    run_id: str,
    from_state: Optional[str],
    to_state: str,
    *,
    raise_on_invalid: bool = True,
) -> bool:
    if from_state is None:
        if to_state in {"admitted", "queued"}:
            return True
        msg = f"run={run_id}: illegal initial state {to_state!r}"
        if raise_on_invalid:
            raise InvalidStateTransition(msg)
        return False

    allowed = VALID_TRANSITIONS.get(from_state)
    if allowed is None:
        msg = f"run={run_id}: unknown state {from_state!r}"
        logger.error("StateTransition fail-closed: %s", msg)
        if raise_on_invalid:
            raise InvalidStateTransition(msg)
        return False
    if to_state in allowed:
        return True
    msg = f"run={run_id}: illegal transition {from_state!r} -> {to_state!r} (allowed: {sorted(allowed)})"
    logger.error("StateTransition ILLEGAL: %s", msg)
    if raise_on_invalid:
        raise InvalidStateTransition(msg)
    return False


def is_terminal(state: Optional[str]) -> bool:
    return state in TERMINAL_STATES


_loader_cache: "WeakKeyDictionary[object, object]" = WeakKeyDictionary()


def _state_script_path() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        for rel in ("lua/state_transition.lua", "hfa/lua/state_transition.lua", "hfa-core/src/hfa/lua/state_transition.lua"):
            p = parent / rel
            if p.exists():
                return p
    raise FileNotFoundError("state_transition.lua not found")


async def _get_lua_loader(redis):
    from hfa.lua.loader import LuaScriptLoader

    loader = _loader_cache.get(redis)
    if loader is not None:
        return loader

    script_path = _state_script_path()
    loader = LuaScriptLoader(redis, script_path)
    try:
        await loader.load()
    except Exception as exc:
        if _strict_cas_mode():
            raise RuntimeError(f"StateMachine strict CAS mode: failed to load {script_path.name}: {exc}") from exc
        logger.warning("StateMachine Lua loader unavailable (%s) — Python fallback", exc)
        return None

    _loader_cache[redis] = loader
    logger.info("StateMachine: Lua CAS loader initialised")
    return loader


async def get_run_state(redis, run_id: str, *, state_key: Optional[str] = None) -> Optional[str]:
    raw = await redis.get(state_key or run_id)
    return raw.decode() if isinstance(raw, bytes) else raw


async def transition_state(
    redis,
    run_id: str,
    to_state: str,
    *,
    state_key: str,
    state_ttl: int = 86400,
    expected_state: Optional[str] = None,
    raise_on_invalid: bool = False,
):
    mode = "initial" if expected_state is None else "cas"

    loader = await _get_lua_loader(redis)
    if loader is not None:
        try:
            runner = getattr(loader, "run", None)
            if runner is None:
                runner = loader.execute
                result = await runner(
                    script_name="state_transition",
                    keys=[state_key],
                    args=[run_id, to_state, expected_state or "", str(state_ttl), mode],
                )
            else:
                result = await runner(
                    num_keys=1,
                    keys=[state_key],
                    args=[run_id, to_state, expected_state or "", str(state_ttl), mode],
                )

            if isinstance(result, (list, tuple)) and len(result) >= 2:
                code = int(result[0])
                prior_raw = result[1]
                prior = prior_raw.decode() if isinstance(prior_raw, bytes) else (prior_raw or None)
            else:
                code = int(result) if result is not None else -99
                prior = expected_state

            if code == 1:
                return TransitionResult(True, TransitionResult.COMMITTED, prior, to_state, run_id)
            if code == 0:
                reason = TransitionResult.INITIAL_WRITE_BLOCKED if mode == "initial" else TransitionResult.CAS_MISS
                return TransitionResult(False, reason, prior, to_state, run_id)
            if code == -1:
                return TransitionResult(False, TransitionResult.INITIAL_WRITE_BLOCKED, prior, to_state, run_id)
            if code == -2:
                return TransitionResult(False, TransitionResult.ILLEGAL_TRANSITION, prior, to_state, run_id)
            if code == -3:
                return TransitionResult(False, TransitionResult.TERMINAL_BLOCKED, prior, to_state, run_id)
            if code == -4:
                return TransitionResult(False, TransitionResult.UNKNOWN_STATE, prior, to_state, run_id)
            logger.warning("StateMachine Lua returned unexpected code=%r for run=%s", code, run_id)
        except Exception as exc:
            if _strict_cas_mode():
                raise RuntimeError(f"StateMachine strict CAS mode: Lua CAS failed for run={run_id}: {exc}") from exc
            logger.warning("StateMachine Lua CAS failed (%s) — Python fallback", exc)

    return await _python_transition(
        redis,
        run_id,
        to_state,
        state_key=state_key,
        state_ttl=state_ttl,
        expected_state=expected_state,
        mode=mode,
        raise_on_invalid=raise_on_invalid,
    )


async def _python_transition(
    redis,
    run_id: str,
    to_state: str,
    *,
    state_key: str,
    state_ttl: int,
    expected_state: Optional[str],
    mode: str,
    raise_on_invalid: bool,
) -> TransitionResult:
    current = await get_run_state(redis, run_id, state_key=state_key)

    if mode == "initial":
        if current is not None:
            return TransitionResult(False, TransitionResult.INITIAL_WRITE_BLOCKED, current, to_state, run_id)
        await redis.set(state_key, to_state, ex=state_ttl)
        return TransitionResult(True, TransitionResult.COMMITTED, None, to_state, run_id)

    if current != expected_state:
        return TransitionResult(False, TransitionResult.CAS_MISS, current, to_state, run_id)

    if is_terminal(current):
        return TransitionResult(False, TransitionResult.TERMINAL_BLOCKED, current, to_state, run_id)

    if current is not None:
        allowed = VALID_TRANSITIONS.get(current)
        if allowed is None:
            if raise_on_invalid:
                raise InvalidStateTransition(f"run={run_id}: unknown state {current!r}")
            return TransitionResult(False, TransitionResult.UNKNOWN_STATE, current, to_state, run_id)
        if to_state not in allowed:
            if raise_on_invalid:
                raise InvalidStateTransition(f"run={run_id}: illegal {current!r} -> {to_state!r}")
            return TransitionResult(False, TransitionResult.ILLEGAL_TRANSITION, current, to_state, run_id)

    await redis.set(state_key, to_state, ex=state_ttl)
    return TransitionResult(True, TransitionResult.COMMITTED, current, to_state, run_id)
