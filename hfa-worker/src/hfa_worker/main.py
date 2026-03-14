"""IRONCLAD Sprint 9 â€” worker lifecycle wiring."""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Dict, List, Set

from hfa.runtime.shard import ShardClaimer
from hfa.runtime.state_store import StateStore
from hfa_worker.consumer import ShardConsumer
from hfa_worker.heartbeat import HeartbeatPublisher

logger = logging.getLogger("hfa.worker.main")


class WorkerService:
    """Standalone worker service for Sprint 9 distributed runtime."""

    def __init__(self, redis: object, config: Dict[str, object]) -> None:
        self._redis = redis
        self._worker_id = str(config.get("worker_id") or f"worker-{uuid.uuid4().hex[:8]}")
        self._worker_group = str(config.get("worker_group") or "default-group")
        self._region = str(config.get("region") or "eu-west")
        self._version = str(config.get("version") or "1.0.0")
        self._capabilities = list(config.get("capabilities") or ["base"])
        self._configured_shards = list(config.get("shards") or [0])
        self._capacity = int(config.get("capacity") or 10)

        self._inflight_tracker: Set[str] = set()
        self._semaphore = asyncio.Semaphore(self._capacity)
        self._state_store = StateStore(redis)
        self._claimer = ShardClaimer(redis, self._worker_id)

        self._claimed_shards: List[int] = []
        self._consumers: List[ShardConsumer] = []
        self._renew_task: asyncio.Task[None] | None = None
        self._running = False

        self._heartbeat = HeartbeatPublisher(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            region=self._region,
            version=self._version,
            capabilities=self._capabilities,
            shards_fn=lambda: list(self._claimed_shards),
            capacity=self._capacity,
            inflight_fn=lambda: len(self._inflight_tracker),
        )

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def start(self) -> None:
        self._running = True

        for shard in self._configured_shards:
            if await self._claimer.claim(shard):
                self._claimed_shards.append(shard)
            else:
                logger.warning("Could not claim shard=%s worker_id=%s", shard, self._worker_id)

        for shard in self._claimed_shards:
            consumer = ShardConsumer(
                redis=self._redis,
                shard=shard,
                worker_id=self._worker_id,
                state_store=self._state_store,
                semaphore=self._semaphore,
                inflight_tracker=self._inflight_tracker,
            )
            self._consumers.append(consumer)
            await consumer.start()

        if self._claimed_shards:
            self._renew_task = asyncio.create_task(self._renew_loop(), name=f"renew.{self._worker_id}")

        await self._heartbeat.start()
        logger.info(
            "Worker started worker_id=%s claimed_shards=%s capacity=%s",
            self._worker_id,
            self._claimed_shards,
            self._capacity,
        )

    async def graceful_shutdown(self, timeout_seconds: int = 60) -> None:
        logger.info("Initiating graceful shutdown worker_id=%s", self._worker_id)
        self._running = False

        # 1. stop renew loop
        if self._renew_task is not None:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
            self._renew_task = None

        # 2. stop consumer pull loops
        for consumer in self._consumers:
            await consumer.stop()

        # 3. wait for inflight runs to complete
        waited = 0
        while self._inflight_tracker and waited < timeout_seconds:
            logger.info("Waiting for inflight runs=%s worker_id=%s", len(self._inflight_tracker), self._worker_id)
            await asyncio.sleep(1)
            waited += 1

        # 4. publish draining and stop heartbeat
        await self._heartbeat.publish_draining("SIGTERM")
        await self._heartbeat.stop()

        # 5. release owned shards
        if self._claimed_shards:
            await self._claimer.release(self._claimed_shards)

        logger.info("Worker shutdown complete worker_id=%s", self._worker_id)

    async def _renew_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(20)
                await self._claimer.renew(self._claimed_shards)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Shard renew loop failed worker_id=%s error=%s", self._worker_id, exc, exc_info=True)
                await asyncio.sleep(1)