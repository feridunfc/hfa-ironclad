"""
hfa-control/src/hfa_control/scheduler_types.py
IRONCLAD Sprint 22 (PR-2) — Deterministic Tie-Break Models
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SchedulingDecisionScore:
    """
    Strict hierarchy for deterministic worker selection.
    Lower tuple value wins.
    """

    # 1. Existing scheduling score (fairness / aging / priority)
    primary_score: int

    # 2. Lower saturation is better
    load_factor: float

    # 3. Lower inflight is better
    inflight: int

    # 4. Higher capacity is better
    capacity: int

    # 5-7. Stable lexical topology fallback
    worker_group: str
    region: str
    worker_id: str

    def as_sort_key(self) -> tuple:
        return (
            self.primary_score,
            self.load_factor,
            self.inflight,
            -self.capacity,
            self.worker_group,
            self.region,
            self.worker_id,
        )