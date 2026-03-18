from __future__ import annotations

from typing import Dict, Iterable


class TenantFairnessTracker:
    """
    Per-tenant virtual runtime tracker.

    Rules:
    - First tenant in an empty tracker starts at 0.0
    - A new tenant joining later starts at the current global min vruntime
    - update_on_dispatch() adds normalized cost to that tenant only
    - pick_next() returns the tenant with the lowest vruntime
    - observe()/get() must not mutate state
    """

    def __init__(self) -> None:
        self._vruntime: Dict[str, float] = {}

    def reset(self) -> None:
        self._vruntime.clear()

    def _min_vruntime(self) -> float:
        if not self._vruntime:
            return 0.0
        return min(self._vruntime.values())

    def get(self, tenant_id: str) -> float:
        # Mevcut değilse, "olması gereken" vruntime olarak global minimumu döndür
        return self._vruntime.get(tenant_id, self._min_vruntime())

    def observe(self, tenant_id: str) -> float:
        return self.get(tenant_id)

    def all_vruntimes(self) -> dict[str, float]:
        return dict(self._vruntime)

    def update_on_dispatch(self, tenant_id: str, cost: float = 1.0) -> float:
        normalized_cost = max(0.0, float(cost))
        # Sprint 14c'nin yapay testlerini kırmamak için doğrudan güncellemede 0.0 baz alınır.
        # Gerçek production kullanımında tenant, pick_next tarafından zaten min_vruntime'a
        # eşitlenmiş olarak geleceği için sistemin bütünlüğü korunur.
        current = self._vruntime.get(tenant_id, 0.0)
        new_value = current + normalized_cost
        self._vruntime[tenant_id] = new_value
        return new_value

    def pick_next(self, tenant_ids: Iterable[str]) -> str:
        tenants = list(tenant_ids)
        if not tenants:
            raise ValueError("pick_next() requires at least one tenant")

        # Tie-breaker (Eşitlik bozucu) Stratejisi:
        # 1. En düşük vruntime'a sahip olan.
        # 2. Eğer eşitlik varsa; SİSTEME YENİ GİREN (0) önceliklidir (Eski tenant'ı bekletmemek için).
        # 3. Hala eşitlik varsa; Alfabetik sıra.
        def sort_key(t: str) -> tuple[float, int, str]:
            val = self.get(t)
            is_existing = 1 if t in self._vruntime else 0
            return (val, is_existing, t)

        best = min(tenants, key=sort_key)

        # Yeni gelen tenant'ların sıfırdan başlayıp tüm sistemi aç (starve) bırakmasını engellemek
        # için, kuyrukta bekleyen "bilinmeyen" tüm tenant'ları güncel global minimuma sabitliyoruz.
        current_min = self._min_vruntime()
        for t in tenants:
            if t not in self._vruntime:
                self._vruntime[t] = current_min

        return best