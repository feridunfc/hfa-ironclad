"""IRONCLAD Sprint 9 — shard consumer with pull-side backpressure."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Set, Tuple

from hfa.events.codec import deserialize_run_requested, serialize_event
from hfa.events.schema import RunCompletedEvent, RunFailedEvent
from hfa.runtime.state_store import StateStore

logger = logging.getLogger("hfa.worker.consumer")

ResultTuple = Tuple[Any, int, int]
ExecutorCallable = Callable[[Dict[str, Any]], Awaitable[ResultTuple]]


class TerminalExecutionError(Exception):
    """Raised when a run fails permanently and should be ACKed as failed."""


class ShardConsumer:
    RESULTS_STREAM = "hfa:stream:results"

    def __init__(
        self,
        redis: object,
        shard: int,
        worker_id: str,
        state_store: StateStore,
        semaphore: asyncio.Semaphore,
        inflight_tracker: Set[str],
        executor: Optional[ExecutorCallable] = None,
    ) -> None:
        self._redis = redis
        self._shard = shard
        self._worker_id = worker_id
        self._state_store = state_store
        self._semaphore = semaphore
        self._inflight = inflight_tracker
        self._executor = executor or self._default_executor
        self._stream = f"hfa:stream:runs:{shard}"
        self._group = f"cg_shard_{shard}"
        self._task: Optional[asyncio.Task[None]] = None
        self._running = False

    async def start(self) -> None:
        try:
            await self._redis.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._running = True
        self._task = asyncio.create_task(self._loop(), name=f"consumer.shard.{self._shard}")

    async def stop(self) -> None:
        self._running = False
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _loop(self) -> None:
        while self._running:
            permit_acquired = False
            try:
                # Pull-side backpressure: reserve global capacity BEFORE pulling.
                try:
                    await asyncio.wait_for(self._semaphore.acquire(), timeout=1.0)
                    permit_acquired = True
                except asyncio.TimeoutError:
                    continue

                messages = await self._redis.xreadgroup(
                    groupname=self._group,
                    consumername=self._worker_id,
                    streams={self._stream: ">"},
                    count=1,
                    block=2000,
                )
                if not messages:
                    self._semaphore.release()
                    permit_acquired = False
                    continue

                for _, entries in messages:
                    for msg_id, data in entries:
                        asyncio.create_task(
                            self._process_message_with_reserved_slot(msg_id, data),
                            name=f"run.{self._shard}.{msg_id.decode() if isinstance(msg_id, bytes) else msg_id}",
                        )
                        permit_acquired = False

            except asyncio.CancelledError:
                if permit_acquired:
                    self._semaphore.release()
                break
            except Exception as exc:
                if permit_acquired:
                    self._semaphore.release()
                logger.error("Consumer loop error shard=%s: %s", self._shard, exc, exc_info=True)
                await asyncio.sleep(1.0)

    async def _process_message_with_reserved_slot(self, msg_id: bytes, data: dict) -> None:
        try:
            await self._process_message(msg_id, data)
        finally:
            self._semaphore.release()

    async def _process_message(self, msg_id: bytes, data: dict) -> None:
        event_type = (data.get(b"event_type") or b"").decode()
        if event_type != "RunRequested":
            logger.warning("Ignoring unexpected event_type=%s shard=%s", event_type, self._shard)
            await self._redis.xack(self._stream, self._group, msg_id)
            return

        event = deserialize_run_requested(data)
        run_id = event.run_id
        self._inflight.add(run_id)

        try:
            started_at = time.time()
            await self._state_store.patch_run_meta(
                run_id,
                {
                    "worker_id": self._worker_id,
                    "shard": str(self._shard),
                    "started_at": str(started_at),
                },
            )
            await self._state_store.transition_state(run_id, "running")

            payload, cost_cents, tokens_used = await self._executor(event.payload)
            completed_at = time.time()

            await self._state_store.store_result(
                run_id=run_id,
                tenant_id=event.tenant_id,
                status="done",
                payload=payload,
                cost_cents=cost_cents,
                tokens_used=tokens_used,
                error=None,
                completed_at=completed_at,
            )
            await self._state_store.transition_state(run_id, "done")
            completed_event = RunCompletedEvent(
                run_id=run_id,
                tenant_id=event.tenant_id,
                status="done",
                cost_cents=int(cost_cents),
                tokens_used=int(tokens_used),
                completed_at=completed_at,
            )
            await self._redis.xadd(
                self.RESULTS_STREAM,
                serialize_event(completed_event),
                maxlen=100_000,
                approximate=True,
            )
            await self._redis.xack(self._stream, self._group, msg_id)

        except TerminalExecutionError as exc:
            completed_at = time.time()
            logger.error("Terminal execution failure run_id=%s error=%s", run_id, exc)
            await self._state_store.store_result(
                run_id=run_id,
                tenant_id=event.tenant_id,
                status="failed",
                payload={},
                cost_cents=0,
                tokens_used=0,
                error=str(exc),
                completed_at=completed_at,
            )
            await self._state_store.transition_state(run_id, "failed")
            failed_event = RunFailedEvent(
                run_id=run_id,
                tenant_id=event.tenant_id,
                status="failed",
                error=str(exc),
                cost_cents=0,
                tokens_used=0,
                completed_at=completed_at,
            )
            await self._redis.xadd(
                self.RESULTS_STREAM,
                serialize_event(failed_event),
                maxlen=100_000,
                approximate=True,
            )
            await self._redis.xack(self._stream, self._group, msg_id)

        except Exception as exc:
            logger.critical("Internal platform crash run_id=%s error=%s", run_id, exc, exc_info=True)
            # No XACK here by design. Message stays in PEL.

        finally:
            self._inflight.discard(run_id)

    async def _default_executor(self, payload: Dict[str, Any]) -> ResultTuple:
        # Placeholder for real sandbox execution.
        return {"result": "success", "input": payload}, 10, 100
