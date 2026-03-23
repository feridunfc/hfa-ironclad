
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.reasons import TASK_OWNERSHIP_FENCED


@dataclass(frozen=True)
class OwnershipCheckResult:
    ok: bool
    status: str


class TaskOwnershipFence:
    """
    Simple ownership fencing layer for worker/task recovery paths.

    Rules:
    - empty owner => claimable
    - same owner => allowed
    - different owner => fenced
    """

    @staticmethod
    def check(current_owner: str | None, candidate_owner: str) -> OwnershipCheckResult:
        if current_owner in (None, "", candidate_owner):
            return OwnershipCheckResult(True, "OK")
        return OwnershipCheckResult(False, TASK_OWNERSHIP_FENCED)
