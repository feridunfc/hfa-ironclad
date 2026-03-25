
from __future__ import annotations


class Scheduler:
    """Single-authority worker picker facade.

    Responsibilities:
    - consume admitted events
    - enqueue into tenant queues
    - build capacity snapshots
    - invoke scoring
    - enforce pacing
    - commit scheduling through Lua/state transition
    """

    def _pick_best_worker(self, candidates):
        usable = []
        for candidate in candidates:
            if not getattr(candidate, "schedulable", True):
                continue
            if not getattr(candidate, "worker_id", None):
                continue
            if getattr(candidate, "decision_score", None) is None:
                continue
            usable.append(candidate)
        if not usable:
            return None
        usable.sort(key=lambda c: c.decision_score.as_sort_key())
        return usable[0]
