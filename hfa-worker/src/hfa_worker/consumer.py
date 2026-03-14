"""
hfa-worker/src/hfa_worker/consumer.py
IRONCLAD Sprint 11 --- Real Execution Path with Pending Resume & Failure Taxonomy (REVISED)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional, Set

from hfa.events.codec import deserialize_run_requested, serialize_event
from hfa.events.schema import RunCompletedEvent, RunFailedEvent
from hfa.runtime.state_store import StateStore
from hfa_worker.executor import BaseExecutor
from hfa_worker.idempotency import IdempotencyGuard
from hfa_worker.models import InfrastructureError, TerminalExecutionError
from hfa_worker.redis_utils import ack_message, ensure_consumer_group

logger = logging.getLogger(__name__)
CONSUMER_GROUP = "worker_consumers"


class WorkerConsumer:
    def __init__(
        self,
        redis,
        worker_id: str,
        worker_group: str,
        shards: list[int],
        executor: BaseExecutor,
        reclaim_idle_ms: int = 60000,
    ):
        self._redis = redis
        self._worker_id = worker_id
        self._worker_group = worker_group
        self._shards = shards
        self._executor = executor
        self._reclaim_idle_ms = reclaim_idle_ms

        self._state = StateStore(redis)
        self._guard = IdempotencyGuard(redis)
        self._inflight: Set[str] = set()

        self._consumer_name = worker_id
        self._streams = [f"hfa:stream:runs:{s}" for s in shards]

        self._running = False
        self._pulling = True
        self._task: Optional[asyncio.Task] = None
        self._renewer_task: Optional[asyncio.Task] = None

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    @property
    def is_draining(self) -> bool:
        return not self._pulling

    def stop_pulling(self) -> None:
        self._pulling = False
        logger.info("Pulling stopped: worker=%s", self._worker_id)

    async def start(self) -> None:
        for stream in self._streams:
            await ensure_consumer_group(
                self._redis,
                stream,
                CONSUMER_GROUP,
                start_id="0",
                mkstream=True,
            )

        self._running = True
        self._pulling = True

        loop = asyncio.get_running_loop()
        self._task = loop.create_task(
            self._main_lifecycle(),
            name=f"consumer.{self._worker_id}",
        )
        self._renewer_task = loop.create_task(
            self._claim_renewer(),
            name=f"renewer.{self._worker_id}",
        )

    async def close(self) -> None:
        self._running = False
        self._pulling = False

        for task in [self._task, self._renewer_task]:
            if task:
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

        self._task = None
        self._renewer_task = None

    async def _claim_renewer(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(300)
                for run_id in list(self._inflight):
                    await self._guard.renew_claim(run_id)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Claim renewer error: %s", exc)

    async def _main_lifecycle(self) -> None:
        await self._reclaim_pending_messages()
        await self._consume_loop()

    async def _reclaim_pending_messages(self) -> None:
        for stream in self._streams:
            try:
                pending = await self._redis.xpending_range(
                    stream,
                    CONSUMER_GROUP,
                    min="-",
                    max="+",
                    count=100,
                )

                if not pending:
                    continue

                to_claim = []
                for p in pending:
                    if isinstance(p, dict):
                        msg_id = p.get("message_id")
                        idle_ms = p.get("time_since_delivered", 0)
                    else:
                        msg_id = p[0]
                        idle_ms = p[2]

                    if msg_id is not None and idle_ms >= self._reclaim_idle_ms:
                        to_claim.append(msg_id)

                if not to_claim:
                    continue

                claimed = await self._redis.xclaim(
                    stream,
                    CONSUMER_GROUP,
                    self._consumer_name,
                    self._reclaim_idle_ms,
                    to_claim,
                )

                for msg_id, data in claimed:
                    msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                    shard = int(stream.split(":")[-1])
                    logger.info(
                        "Reclaimed pending message: worker=%s stream=%s msg_id=%s",
                        self._worker_id,
                        stream,
                        msg_id_str,
                    )
                    await self._process_message(msg_id_str, data, stream, shard)

            except Exception as exc:
                logger.error("Error reclaiming pending messages: %s", exc)

    async def _consume_loop(self) -> None:
        streams_dict = {s: ">" for s in self._streams}

        while self._running and self._pulling:
            try:
                msgs = await self._redis.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=self._consumer_name,
                    streams=streams_dict,
                    count=10,
                    block=100,  # shorter block so shutdown/tests do not hang
                )

                if not msgs:
                    continue

                for stream_name, entries in msgs:
                    s_name = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                    shard = int(s_name.split(":")[-1])

                    for msg_id, data in entries:
                        msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        await self._process_message(msg_id_str, data, s_name, shard)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Consume loop error: %s", exc)
                await asyncio.sleep(0.1)

    async def _process_message(self, msg_id: str, data: dict, stream: str, shard: int) -> None:
        try:
            event = deserialize_run_requested(data)
            if event is None:
                logger.warning("Failed to deserialize message %s", msg_id)
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
                return

            if not await self._guard.should_execute(event.run_id):
                logger.info("Skipping terminal run=%s", event.run_id)
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
                return

            started = await self._guard.try_claim_and_mark_running(
                event.run_id,
                self._worker_id,
                self._worker_group,
                shard,
            )
            if not started:
                logger.info("Run already claimed elsewhere: run=%s", event.run_id)
                return

            self._inflight.add(event.run_id)

            try:
                result = await self._executor.execute(event)

                if result.status == "done":
                    await self._state.store_result(
                        event.run_id,
                        event.tenant_id,
                        "done",
                        result.payload,
                        result.cost_cents,
                        result.tokens_used,
                    )
                    await self._state.transition_state(event.run_id, "done")

                    evt = RunCompletedEvent(
                        run_id=event.run_id,
                        tenant_id=event.tenant_id,
                        worker_id=self._worker_id,
                        payload=result.payload,
                        cost_cents=result.cost_cents,
                        tokens_used=result.tokens_used,
                    )
                else:
                    await self._state.store_result(
                        event.run_id,
                        event.tenant_id,
                        "failed",
                        result.payload,
                        result.cost_cents,
                        result.tokens_used,
                        error=result.error,
                    )
                    await self._state.transition_state(event.run_id, "failed")

                    evt = RunFailedEvent(
                        run_id=event.run_id,
                        tenant_id=event.tenant_id,
                        worker_id=self._worker_id,
                        error=result.error or "Terminal error",
                        cost_cents=result.cost_cents,
                        tokens_used=result.tokens_used,
                        payload=result.payload,
                    )

                await self._redis.xadd("hfa:stream:results", serialize_event(evt))
                await self._state.mark_completed(event.run_id)
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)

            except TerminalExecutionError as exc:
                logger.warning("Terminal failure run=%s error=%s", event.run_id, exc)

                await self._state.store_result(
                    event.run_id,
                    event.tenant_id,
                    "failed",
                    {},
                    exc.cost_cents,
                    exc.tokens_used,
                    error=str(exc),
                )
                await self._state.transition_state(event.run_id, "failed")

                evt = RunFailedEvent(
                    run_id=event.run_id,
                    tenant_id=event.tenant_id,
                    worker_id=self._worker_id,
                    error=str(exc),
                    cost_cents=exc.cost_cents,
                    tokens_used=exc.tokens_used,
                )
                await self._redis.xadd("hfa:stream:results", serialize_event(evt))
                await self._state.mark_completed(event.run_id)
                await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)

            except InfrastructureError as exc:
                logger.warning("Infrastructure crash run=%s error=%s", event.run_id, exc)
                await self._state.release_claim(event.run_id)
                # no ACK, no terminal state

            except Exception as exc:
                logger.error("Unexpected infra crash run=%s error=%s", event.run_id, exc, exc_info=True)
                await self._state.release_claim(event.run_id)
                # no ACK, no terminal state

            finally:
                self._inflight.discard(event.run_id)

        except Exception as exc:
            logger.error("Message process error: %s", exc, exc_info=True)