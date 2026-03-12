"""
hfa-worker/src/hfa_worker/heartbeat.py
IRONCLAD Sprint 10 — Worker heartbeat + draining publisher

Publishes WorkerHeartbeatEvent to hfa:stream:heartbeat every INTERVAL.
On graceful shutdown, publishes WorkerDrainingEvent before stopping.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — asyncio.get_running_loop().
* close() always safe (idempotent).
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional

from hfa.events.schema import WorkerHeartbeatEvent, WorkerDrainingEvent
from hfa.events.codec  import serialize_event

logger = logging.getLogger(__name__)

HEARTBEAT_STREAM   = "hfa:stream:heartbeat"
HEARTBEAT_INTERVAL = float(os.environ.get("WORKER_HEARTBEAT_INTERVAL", "10"))


class WorkerHeartbeatPublisher:

    def __init__(
        self,
        redis,
        worker_id:    str,
        worker_group: str,
        region:       str,
        shards:       List[int],
        capacity:     int,
        inflight_fn:  Callable[[], int],
        version:      str             = "",
        capabilities: Optional[List[str]] = None,
    ) -> None:
        self._redis       = redis
        self._worker_id   = worker_id
        self._group       = worker_group
        self._region      = region
        self._shards      = shards
        self._capacity    = capacity
        self._inflight_fn = inflight_fn
        self._version     = version
        self._caps        = capabilities or []
        self._running     = False
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(
            self._loop(), name=f"heartbeat.{self._worker_id}"
        )
        logger.info(
            "HeartbeatPublisher started: worker=%s group=%s region=%s shards=%s",
            self._worker_id, self._group, self._region, self._shards,
        )

    async def close(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("HeartbeatPublisher closed: worker=%s", self._worker_id)

    # ------------------------------------------------------------------
    # Draining
    # ------------------------------------------------------------------

    async def publish_draining(self, reason: str = "SIGTERM") -> None:
        """
        Publish WorkerDrainingEvent immediately.
        drain_deadline = now + 120 seconds.
        Called before graceful shutdown begins so the Control Plane
        stops routing new runs to this worker.
        """
        deadline = (
            datetime.now(timezone.utc) + timedelta(seconds=120)
        ).isoformat()

        evt = WorkerDrainingEvent(
            worker_id=self._worker_id,
            worker_group=self._group,
            region=self._region,
            drain_deadline_utc=deadline,
            reason=reason,
        )
        try:
            await self._redis.xadd(
                HEARTBEAT_STREAM,
                serialize_event(evt),
                maxlen=10_000,
                approximate=True,
            )
            logger.info(
                "WorkerDrainingEvent published: worker=%s deadline=%s reason=%s",
                self._worker_id, deadline, reason,
            )
        except Exception as exc:
            logger.error("publish_draining error: worker=%s %s", self._worker_id, exc)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._publish_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "HeartbeatPublisher._loop error: worker=%s %s",
                    self._worker_id, exc,
                )
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _publish_once(self) -> None:
        inflight = self._inflight_fn()
        evt = WorkerHeartbeatEvent(
            worker_id=self._worker_id,
            worker_group=self._group,
            region=self._region,
            shards=self._shards,
            capacity=self._capacity,
            inflight=inflight,
            version=self._version,
            capabilities=self._caps,
        )
        await self._redis.xadd(
            HEARTBEAT_STREAM,
            serialize_event(evt),
            maxlen=10_000,
            approximate=True,
        )
        logger.debug(
            "Heartbeat published: worker=%s inflight=%d/%d",
            self._worker_id, inflight, self._capacity,
        )
