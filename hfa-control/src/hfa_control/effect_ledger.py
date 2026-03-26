from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Optional

@dataclass
class EffectReceipt:
    accepted: bool
    duplicate: bool
    effect_type: str
    token: str
    run_id: str
    owner_id: Optional[str] = None
    committed_state: Optional[str] = None

    @classmethod
    def from_json(cls, data: str | bytes) -> "EffectReceipt":
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return cls(**json.loads(data))

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)

class EffectLedger:
    """Exactly-once effect admission layer using Atomic SETNX."""

    def __init__(self, redis_client):
        self._redis = redis_client

    @staticmethod
    def effect_key(effect_type: str, run_id: str, token: str) -> str:
        return f"hfa:effect:{effect_type}:{run_id}:{token}"

    async def acquire_effect(
        self,
        run_id: str,
        token: str,
        effect_type: str,
        owner_id: str,
        ttl_seconds: int = 86400,
    ) -> EffectReceipt:
        key = self.effect_key(effect_type, run_id, token)

        proposed = EffectReceipt(
            accepted=True,
            duplicate=False,
            effect_type=effect_type,
            token=token,
            run_id=run_id,
            owner_id=owner_id,
            committed_state="pending",
        )

        # KOMUTANIN REVİZYONU: Recursion yerine Bounded-Loop (Sınırlı Döngü)
        for _ in range(2):
            success = await self._redis.set(key, proposed.to_json(), nx=True, ex=ttl_seconds)
            if success:
                return proposed

            existing = await self._redis.get(key)
            if existing:
                prior = EffectReceipt.from_json(existing)
                prior.accepted = False
                prior.duplicate = True
                return prior

        # Son Fallback (Eğer her iki denemede de anahtar tam o an silindiyse)
        prior = await self.get_receipt(run_id=run_id, token=token, effect_type=effect_type)
        if prior is not None:
            prior.accepted = False
            prior.duplicate = True
            return prior

        # En kötü senaryo: Reddet ve sistemi koru
        return EffectReceipt(
            accepted=False,
            duplicate=True,
            effect_type=effect_type,
            token=token,
            run_id=run_id,
            owner_id=owner_id,
            committed_state="unknown",
        )

    async def get_receipt(self, *, run_id: str, token: str, effect_type: str) -> EffectReceipt | None:
        raw = await self._redis.get(self.effect_key(effect_type, run_id, token))
        if raw is None:
            return None
        return EffectReceipt.from_json(raw)