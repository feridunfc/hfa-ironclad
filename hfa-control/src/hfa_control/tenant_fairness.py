"""
hfa-control/src/hfa_control/tenant_fairness.py
IRONCLAD Sprint 14C --- In-memory tenant fairness tracker
"""

from __future__ import annotations


class TenantFairnessTracker:
    """
    Tracks virtual runtime per tenant.

    Lower vruntime => higher scheduling priority
    """

    def __init__(self):
        self._vruntime: dict[str, float] = {}

    def get(self, tenant_id: str) -> float:
        """Get current vruntime for tenant (default 0.0)."""
        return self._vruntime.get(tenant_id, 0.0)

    def update_on_dispatch(self, tenant_id: str, cost: float = 1.0) -> None:
        """Increase vruntime after scheduling a run."""
        self._vruntime[tenant_id] = self.get(tenant_id) + cost

    def pick_next(self, tenants: list[str]) -> str:
        """Select tenant with lowest vruntime."""
        return min(tenants, key=lambda t: self.get(t))