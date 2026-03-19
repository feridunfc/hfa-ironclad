import asyncio

import fakeredis.aioredis as faredis
import pytest

from hfa_control.audit import AuditEvent, AuditLogger
from hfa_control.audit_store import RedisLedgerStore


@pytest.mark.asyncio
class TestSprint17Audit:
    async def test_audit_logger_enqueues_events(self):
        redis = faredis.FakeRedis()
        store = RedisLedgerStore(redis)
        logger = AuditLogger(store)
        await logger.start()
        logger.run_admitted("run-123", "tenant-a", {"agent": "test"})
        logger.run_rejected("run-456", "tenant-b", "rate_limit")
        await asyncio.sleep(0.1)
        await logger.close()
        events = await store.read_recent(10)
        assert len(events) >= 2
        assert events[0].event_type == "run.admitted"
        assert events[0].run_id == "run-123"
        assert events[0].tenant_id == "tenant-a"

    async def test_audit_store_persists_to_redis(self):
        redis = faredis.FakeRedis()
        store = RedisLedgerStore(redis)
        event = AuditEvent(
            event_type="test.event",
            timestamp=12345.0,
            tenant_id="tenant-a",
            run_id="run-123",
            metadata={"key": "value"},
        )
        await store.append(event)
        events = await store.read_recent(10)
        assert len(events) == 1
        assert events[0].event_type == "test.event"
        assert events[0].tenant_id == "tenant-a"
        assert events[0].metadata["key"] == "value"
