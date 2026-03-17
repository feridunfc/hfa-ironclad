"""
tests/core/test_sprint14c_fairness.py
IRONCLAD Sprint 14C --- Tenant fairness tests
"""

from hfa_control.tenant_fairness import TenantFairnessTracker


def test_fairness_balances_two_tenants():
    """Two tenants with equal load should get roughly equal picks."""
    tracker = TenantFairnessTracker()

    tenants = ["a", "b"]
    order = []

    for _ in range(10):
        t = tracker.pick_next(tenants)
        order.append(t)
        tracker.update_on_dispatch(t)

    # Distribution should be nearly equal
    assert abs(order.count("a") - order.count("b")) <= 1


def test_heavy_tenant_gets_deprioritized():
    """Tenant with higher vruntime should be deprioritized."""
    tracker = TenantFairnessTracker()

    # Heavy tenant gets 10 cost
    tracker.update_on_dispatch("heavy", cost=10)

    next_tenant = tracker.pick_next(["heavy", "light"])

    assert next_tenant == "light"


def test_new_tenant_starts_at_zero():
    """New tenant should start with vruntime 0."""
    tracker = TenantFairnessTracker()

    assert tracker.get("new") == 0.0


def test_vruntime_accumulates_correctly():
    """Multiple dispatches should accumulate vruntime."""
    tracker = TenantFairnessTracker()

    tracker.update_on_dispatch("a")
    tracker.update_on_dispatch("a")

    assert tracker.get("a") == 2.0


def test_pick_next_with_single_tenant():
    """Single tenant should always be selected."""
    tracker = TenantFairnessTracker()

    tenant = tracker.pick_next(["only"])

    assert tenant == "only"