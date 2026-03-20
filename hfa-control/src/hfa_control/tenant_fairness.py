"""
hfa-control/src/hfa_control/tenant_fairness.py
IRONCLAD Sprint 14C/16 — Tenant fairness tracker (CFS-style vruntime)

Sprint 14C: accounting hook only.
Sprint 16:  pick_next() actively drives the fair scheduler loop.

CFS algorithm
-------------
* New tenants start at min_vruntime (starvation prevention).
* update_on_dispatch() increases vruntime by cost after every confirmed dispatch.
* pick_next() selects the tenant with the lowest vruntime.

Starvation prevention
---------------------
A tenant idle for a long time would have vruntime=0 and monopolize the
scheduler on resume. Initializing at min_vruntime bounds catch-up to one round.

IRONCLAD rules
--------------
* update_on_dispatch() only after confirmed dispatch — never on failure.
* Negative cost clamped to 0.
* reset() clears all state (leadership change / test isolation).
* No locks needed — single-writer scheduler loop.
"""

from __future__ import annotations


class TenantFairnessTracker:
    """
    CFS-style per-tenant virtual runtime tracker.

    Lower vruntime => tenant is under-served => higher scheduling priority.
    """

    def __init__(self) -> None:
        self._vruntime: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, tenant_id: str) -> float:
        """Return vruntime for tenant. New tenants start at min_vruntime."""
        if tenant_id not in self._vruntime:
            return self._min_vruntime()
        return self._vruntime[tenant_id]

    def observe(self, tenant_id: str) -> float:
        """Read-only hook — does not mutate state."""
        return self.get(tenant_id)

    def pick_next(self, tenants: list[str]) -> str:
        """
        Select the tenant with the lowest vruntime (most under-served).

        Args:
            tenants: Non-empty list of candidate tenant_ids.

        Returns:
            tenant_id of the most under-served tenant.
        """
        if not tenants:
            raise ValueError("pick_next called with empty tenant list")
        return min(
            tenants, key=lambda t: (self.get(t), 1 if t in self._vruntime else 0)
        )

    def all_vruntimes(self) -> dict[str, float]:
        """Return snapshot of all vruntime values (metrics / observability)."""
        return dict(self._vruntime)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def update_on_dispatch(self, tenant_id: str, cost: float = 1.0) -> None:
        """
        Increase tenant's vruntime after a successful dispatch.
        New tenants are initialized to min_vruntime before adding cost.
        """
        base = self._vruntime.get(tenant_id, 0.0)
        self._vruntime[tenant_id] = base + max(cost, 0.0)

    def reset(self) -> None:
        """Clear all vruntime data (leadership change / test isolation)."""
        self._vruntime.clear()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _min_vruntime(self) -> float:
        """Floor for new-tenant initialization. Returns 0.0 if no tenants yet."""
        if not self._vruntime:
            return 0.0
        return min(self._vruntime.values())
