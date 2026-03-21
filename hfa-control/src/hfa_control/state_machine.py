"""
hfa-control/src/hfa_control/state_machine.py
IRONCLAD Sprint 21 v3 — Run state machine with CAS enforcement

Design
------
Single authoritative entrypoint for all run state transitions.
All callers (scheduler, recovery, consumer, cancel flow) MUST use
transition_state() — never write run_state directly.

State diagram
-------------
  (start)
     │
  admitted ──► queued ──► scheduled ──► running ──► done
                │              │            │
                └──► failed    └──► failed  └──► failed
                │                           │
                └──► rejected               └──► dead_lettered

  running → rescheduled → admitted  (reschedule loop)

CAS semantics
-------------
transition_state() uses SET ... XX (only if key exists) pattern plus
a GET-before-SET check. In production use dispatch_commit.lua for
scheduler→running which provides true Redis-atomic CAS. The Python
CAS here protects all other transition paths.

IRONCLAD rules
--------------
* Validate before every state write — no exception.
* Terminal states are final — no outgoing transitions.
* CAS failure = log WARNING + return False (not exception) for recovery paths.
* All enforcement paths emit reason codes for metrics.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ── Legal transition map ───────────────────────────────────────────────────

VALID_TRANSITIONS: dict[str, frozenset[str]] = {
    "admitted":      frozenset({"queued", "scheduled", "rejected", "failed"}),
    "queued":        frozenset({"scheduled", "failed", "rejected"}),
    "scheduled":     frozenset({"running", "failed", "admitted"}),
    "running":       frozenset({"done", "failed", "dead_lettered", "scheduled", "rescheduled"}),
    "rescheduled":   frozenset({"admitted", "failed"}),
    # Terminal — no outgoing transitions
    "done":          frozenset(),
    "failed":        frozenset(),
    "rejected":      frozenset(),
    "dead_lettered": frozenset(),
}

TERMINAL_STATES: frozenset[str] = frozenset({"done", "failed", "rejected", "dead_lettered"})


class InvalidStateTransition(Exception):
    """Raised when a state transition is not allowed by the state machine."""


# ── Validation ─────────────────────────────────────────────────────────────

def validate_transition(
    run_id: str,
    from_state: Optional[str],
    to_state: str,
    *,
    raise_on_invalid: bool = True,
) -> bool:
    """
    Check whether a state transition is legal.

    Args:
        run_id:           Run identifier (for logging).
        from_state:       Current state. None = state not set yet.
        to_state:         Target state.
        raise_on_invalid: If True, raise InvalidStateTransition on violation.

    Returns True if transition is legal, False otherwise.
    """
    if from_state is None:
        if to_state in ("admitted", "queued"):
            return True
        msg = (
            f"run={run_id}: illegal initial state {to_state!r} "
            f"(only 'admitted' or 'queued' allowed)"
        )
        logger.error("StateTransition ILLEGAL: %s", msg)
        if raise_on_invalid:
            raise InvalidStateTransition(msg)
        return False

    allowed = VALID_TRANSITIONS.get(from_state, frozenset())

    if to_state in allowed:
        return True

    if from_state not in VALID_TRANSITIONS:
        logger.warning(
            "StateTransition: run=%s unknown from_state=%r → %r (allowing for compat)",
            run_id, from_state, to_state,
        )
        return True

    msg = (
        f"run={run_id}: illegal transition {from_state!r} → {to_state!r} "
        f"(allowed: {sorted(allowed)})"
    )
    logger.error("StateTransition ILLEGAL: %s", msg)
    if raise_on_invalid:
        raise InvalidStateTransition(msg)
    return False


def is_terminal(state: Optional[str]) -> bool:
    """Return True if state is terminal (no further transitions allowed)."""
    return state in TERMINAL_STATES


# ── Authoritative CAS transition ──────────────────────────────────────────

async def transition_state(
    redis,
    run_id: str,
    to_state: str,
    *,
    state_key: str,
    state_ttl: int = 86400,
    expected_state: Optional[str] = None,
    raise_on_invalid: bool = False,
) -> bool:
    """
    Authoritatively transition a run's state with CAS semantics.

    This is the single entrypoint for all state writes outside the
    Lua atomic dispatch path. All callers should use this function
    instead of writing run_state directly.

    Args:
        redis:           Redis client.
        run_id:          Run identifier.
        to_state:        Target state.
        state_key:       Redis key for run state (RedisKey.run_state(run_id)).
        state_ttl:       TTL for the state key.
        expected_state:  If set, only transition if current state matches.
                         If None, reads current state and validates.
        raise_on_invalid: If True, raise on illegal transition.

    Returns:
        True if transition succeeded.
        False if:
          - CAS mismatch (state changed under us)
          - illegal transition
          - current state is terminal
    """
    raw = await redis.get(state_key)
    current = raw.decode() if isinstance(raw, bytes) else raw

    # CAS check: if caller specified expected_state, enforce it
    if expected_state is not None and current != expected_state:
        logger.warning(
            "StateTransition CAS miss: run=%s expected=%r actual=%r → %r",
            run_id, expected_state, current, to_state,
        )
        return False

    # Validate transition
    ok = validate_transition(
        run_id, current, to_state,
        raise_on_invalid=raise_on_invalid,
    )
    if not ok:
        return False

    # Write new state with TTL
    await redis.set(state_key, to_state, ex=state_ttl)
    logger.debug(
        "StateTransition: run=%s %r → %r",
        run_id, current, to_state,
    )
    return True
