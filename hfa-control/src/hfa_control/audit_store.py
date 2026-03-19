"""
hfa-control/src/hfa_control/audit_store.py
IRONCLAD Sprint 17.2 --- Redis-backed audit storage (append-only log)

NOT: This is not a signed ledger.
It is an append-only Redis LIST based audit log.
"""

from __future__ import annotations

import json
import logging
from typing import List

from hfa_control.audit import AuditEvent

logger = logging.getLogger(__name__)

AUDIT_KEY = "hfa:audit:log"
AUDIT_TTL = 2_592_000  # 30 days


class RedisLedgerStore:
    def __init__(self, redis) -> None:
        self._redis = redis

    async def append(self, event: AuditEvent) -> None:
        data = json.dumps({
            "event_type": event.event_type,
            "timestamp": event.timestamp,
            "tenant_id": event.tenant_id,
            "run_id": event.run_id,
            "worker_id": event.worker_id,
            "metadata": event.metadata or {},
        })
        await self._redis.rpush(AUDIT_KEY, data)
        await self._redis.expire(AUDIT_KEY, AUDIT_TTL)

    async def read_recent(self, limit: int = 100) -> List[AuditEvent]:
        try:
            raw = await self._redis.lrange(AUDIT_KEY, -limit, -1)
        except Exception as exc:  # pragma: no cover
            logger.error("Failed to read audit events: %s", exc, exc_info=True)
            return []

        events: List[AuditEvent] = []
        for item in raw:
            if isinstance(item, bytes):
                item = item.decode("utf-8")
            data = json.loads(item)
            events.append(AuditEvent(**data))
        return events
