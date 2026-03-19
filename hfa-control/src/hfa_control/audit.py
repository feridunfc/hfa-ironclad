"""
hfa-control/src/hfa_control/audit.py
IRONCLAD Sprint 17.2 --- Audit logging for control plane
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Protocol

logger = logging.getLogger(__name__)


@dataclass
class AuditEvent:
    """Base audit event structure."""

    event_type: str
    timestamp: float
    tenant_id: Optional[str] = None
    run_id: Optional[str] = None
    worker_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class AuditStore(Protocol):
    async def append(self, event: AuditEvent) -> None: ...


class AuditLogger:
    """
    Central audit logger for control plane operations.
    Fail-silent: audit must never block critical paths.
    """

    def __init__(self, store: AuditStore, component: str = "control-plane") -> None:
        self._store = store
        self._component = component
        self._queue: asyncio.Queue[AuditEvent] = asyncio.Queue(maxsize=1000)
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        if self._worker_task is not None:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop(), name="audit.worker")
        logger.info("AuditLogger started: component=%s", self._component)

    async def close(self) -> None:
        self._running = False
        if self._worker_task and not self._worker_task.done():
            try:
                await asyncio.wait_for(self._worker_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass
        await self._flush_queue()
        self._worker_task = None
        logger.info("AuditLogger closed")

    async def _worker_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                await self._write_event(event)
            except Exception as exc:  # pragma: no cover
                logger.error("Audit worker error: %s", exc, exc_info=True)

    async def _flush_queue(self) -> None:
        while not self._queue.empty():
            try:
                event = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                await self._write_event(event)
            except Exception as exc:  # pragma: no cover
                logger.error("Audit flush error: %s", exc, exc_info=True)

    async def _write_event(self, event: AuditEvent) -> None:
        try:
            await self._store.append(event)
        except Exception as exc:
            logger.error("Failed to write audit event: %s", exc, exc_info=True)

    def _enqueue(self, event: AuditEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Audit queue full, dropping event: %s", event.event_type)

    def run_admitted(self, run_id: str, tenant_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        self._enqueue(AuditEvent("run.admitted", time.time(), tenant_id=tenant_id, run_id=run_id, metadata=metadata or {}))

    def run_rejected(self, run_id: str, tenant_id: str, reason: str) -> None:
        self._enqueue(AuditEvent("run.rejected", time.time(), tenant_id=tenant_id, run_id=run_id, metadata={"reason": reason}))

    def run_scheduled(self, run_id: str, tenant_id: str, worker_group: str, shard: int) -> None:
        self._enqueue(AuditEvent("run.scheduled", time.time(), tenant_id=tenant_id, run_id=run_id, metadata={"worker_group": worker_group, "shard": shard}))

    def drain_started(self, worker_id: str, worker_group: str, reason: str) -> None:
        self._enqueue(AuditEvent("worker.drain_started", time.time(), worker_id=worker_id, metadata={"worker_group": worker_group, "reason": reason}))

    def dlq_replay(self, run_id: str, tenant_id: str) -> None:
        self._enqueue(AuditEvent("dlq.replay", time.time(), tenant_id=tenant_id, run_id=run_id))

    def dlq_deleted(self, run_id: str, tenant_id: str) -> None:
        self._enqueue(AuditEvent("dlq.deleted", time.time(), tenant_id=tenant_id, run_id=run_id))

    def run_rescheduled(self, run_id: str, tenant_id: str, reason: str) -> None:
        self._enqueue(AuditEvent("run.rescheduled", time.time(), tenant_id=tenant_id, run_id=run_id, metadata={"reason": reason}))
