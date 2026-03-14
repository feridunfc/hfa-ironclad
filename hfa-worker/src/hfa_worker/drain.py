"""
hfa-worker/src/hfa_worker/drain.py
IRONCLAD Sprint 11 --- Graceful Drain Management with Event Emission
"""
from __future__ import annotations

import asyncio
import logging
import time

from hfa.events.codec import serialize_event
from hfa.events.schema import WorkerDrainingEvent

logger = logging.getLogger(__name__)
SHARD_OWNER_KEY = "hfa:cp:shard:owner:{}"


class DrainManager:
    def __init__(
        self,
        redis,
        worker_id: str,
        worker_group: str,
        shards: list[int],
        consumer,
        control_stream: str = "hfa:stream:control",
    ):
        self._redis = redis
        self._worker_id = worker_id
        self._worker_group = worker_group
        self._shards = shards
        self._consumer = consumer
        self._control_stream = control_stream
        self._draining = False

    async def start_drain(self, reason: str = "shutdown", timeout: float = 30.0) -> None:
        if self._draining:
            logger.warning("Already draining, ignoring second drain request")
            return

        self._draining = True
        deadline_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + timeout))

        event = WorkerDrainingEvent(
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            drain_deadline_utc=deadline_utc,
            reason=reason,
        )
        await self._redis.xadd(
            self._control_stream,
            serialize_event(event),
            maxlen=10000,
            approximate=True,
        )

        self._consumer.stop_pulling()

        deadline = time.time() + timeout
        while self._consumer.inflight_count > 0 and time.time() < deadline:
            await asyncio.sleep(1.0)

        await self._release_shards()

    async def _release_shards(self) -> None:
        for shard in self._shards:
            key = SHARD_OWNER_KEY.format(shard)
            try:
                owner = await self._redis.get(key)
                if owner:
                    owner_str = owner.decode() if isinstance(owner, bytes) else owner
                    if owner_str == self._worker_group:
                        await self._redis.delete(key)
            except Exception as exc:
                logger.error("Failed to release shard %d: %s", shard, exc)

    @property
    def is_draining(self) -> bool:
        return self._draining
