"""
hfa-control/src/hfa_control/tenant_fairness.py
IRONCLAD Sprint 14C --- In-memory tenant fairness tracker

Design
------
Tracks per-tenant virtual runtime (vruntime) for CFS-style fairness accounting.
This is an instrumentation/accounting hook ONLY.

Current state:
  - Fairness data is collected after each successful scheduling dispatch.
  - Scheduler placement logic remains load-factor and policy driven.
  - Full active fair-queue scheduling is planned for a future sprint.

Key invariants:
  - update_on_dispatch() is only called on SUCCESSFUL placement.
  - No update on PlacementError or unexpected exceptions.
  - In-memory only — not persisted to Redis.
  - Thread/async safe for single-writer scheduler use (no locks needed).
"""

from __future__ import annotations


class TenantFairnessTracker:
    """
    Tracks per-tenant virtual runtime for fairness accounting.

    Lower vruntime => tenant has been under-served relative to others.
    This data is used for observability and future fair-queue scheduling.

    IRONCLAD rules
    --------------
    * update_on_dispatch() must only be called after confirmed dispatch.
    * cost is bounded to [0, ∞) — negative costs are silently clamped to 0.
    * observe() is a read-only hook safe to call at any time.
    * reset() clears all state (e.g. for testing or CP leadership change).
    """

    def __init__(self) -> None:
        self._vruntime: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, tenant_id: str) -> float:
        """Return current vruntime for tenant (default 0.0)."""
        return self._vruntime.get(tenant_id, 0.0)

    def observe(self, tenant_id: str) -> float:
        """
        Read-only observation hook. Returns vruntime without mutating state.
        Safe to call in _select_worker_group before placement decision.
        """
        return self.get(tenant_id)

    def pick_next(self, tenants: list[str]) -> str:
        """
        Select tenant with lowest vruntime (least-served).
        Used for future fair-queue ordering.
        """
        return min(tenants, key=lambda t: self.get(t))

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def update_on_dispatch(self, tenant_id: str, cost: float = 1.0) -> None:
        """
        Increase vruntime after a successful scheduling dispatch.

        Args:
            tenant_id: Tenant that was just dispatched.
            cost:      Scheduling cost (clamped to >= 0). Defaults to 1.0.
        """
        self._vruntime[tenant_id] = self.get(tenant_id) + max(cost, 0.0)

    def reset(self) -> None:
        """Clear all tenant vruntime data (testing / CP leadership change)."""
        self._vruntime.clear()