
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from hfa.config.keys import RedisTTL
from hfa.dag.heartbeat import HeartbeatPolicy
from hfa.dag.reasons import (
    TASK_HEARTBEAT_OWNER_MISMATCH,
    TASK_HEARTBEAT_RECORDED,
    TASK_REQUEUED,
)
from hfa.dag.schema import DagRedisKey
from hfa.lua.loader import LuaScriptLoader

logger = logging.getLogger(__name__)

_LUA_DIR = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "hfa-core"
    / "src"
    / "hfa"
    / "lua"
)
_LUA_DIR_INSTALLED = Path(__file__).resolve().parent.parent.parent / "hfa" / "lua"


def _lua_path(filename: str) -> Path:
    for base in (_LUA_DIR, _LUA_DIR_INSTALLED):
        p = base / filename
        if p.exists():
            return p
    here = Path(__file__).resolve()
    for parent in here.parents:
        for subdir in ("hfa-core/src/hfa/lua", "hfa/lua"):
            p = parent / subdir / filename
            if p.exists():
                return p
    raise FileNotFoundError(f"{filename} not found in known lua directories")


@dataclass(frozen=True)
class TaskHeartbeatResult:
    ok: bool
    status: str


@dataclass(frozen=True)
class TaskRequeueResult:
    ok: bool
    status: str
    requeue_count: int = 0


class TaskHeartbeatManager:
    def __init__(self, redis, policy: HeartbeatPolicy | None = None) -> None:
        self._redis = redis
        self._policy = policy or HeartbeatPolicy()

    async def record_heartbeat(self, *, task_id: str, tenant_id: str, worker_id: str, now_ms: int | None = None) -> TaskHeartbeatResult:
        now_ms = now_ms or int(time.time() * 1000)
        state_key = DagRedisKey.task_state(task_id)
        meta_key = DagRedisKey.task_meta(task_id)

        state = await self._redis.get(state_key)
        if state != "running":
            return TaskHeartbeatResult(False, "TASK_STATE_CONFLICT")

        owner = await self._redis.hget(meta_key, "heartbeat_owner")
        if owner not in (None, "", worker_id):
            return TaskHeartbeatResult(False, TASK_HEARTBEAT_OWNER_MISMATCH)

        await self._redis.hset(meta_key, mapping={
            "heartbeat_owner": worker_id,
            "heartbeat_at_ms": str(now_ms),
            "tenant_id": tenant_id,
        })
        return TaskHeartbeatResult(True, TASK_HEARTBEAT_RECORDED)


class TaskRecoveryManager:
    def __init__(self, redis, policy: HeartbeatPolicy | None = None) -> None:
        self._redis = redis
        self._policy = policy or HeartbeatPolicy()
        self._requeue_loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        path = _lua_path("task_requeue.lua")
        self._requeue_loader = LuaScriptLoader(self._redis, path)
        await self._requeue_loader.load()

    async def find_stale_tasks(self, *, tenant_id: str, now_ms: int | None = None) -> list[str]:
        now_ms = now_ms or int(time.time() * 1000)
        running_key = DagRedisKey.task_running_zset(tenant_id)
        task_ids = await self._redis.zrange(running_key, 0, -1)
        stale: list[str] = []
        for task_id in task_ids:
            heartbeat = await self._redis.hget(DagRedisKey.task_meta(task_id), "heartbeat_at_ms")
            try:
                last = int(heartbeat or "0")
            except ValueError:
                last = 0
            if last <= 0 or (now_ms - last) > self._policy.stale_after_ms:
                stale.append(task_id)
        return stale

    async def requeue_stale_task(self, *, task_id: str, tenant_id: str, expected_state: str = "running", now_ms: int | None = None, ready_score: int | None = None, reason_code: str = "TASK_STALE_DETECTED") -> TaskRequeueResult:
        if self._requeue_loader is None:
            await self.initialise()

        assert self._requeue_loader is not None
        now_ms = now_ms or int(time.time() * 1000)
        ready_score = ready_score if ready_score is not None else now_ms

        result = await self._requeue_loader.run(
            num_keys=5,
            keys=[
                DagRedisKey.task_state(task_id),
                DagRedisKey.task_meta(task_id),
                DagRedisKey.tenant_ready_queue(tenant_id),
                DagRedisKey.task_running_zset(tenant_id),
                DagRedisKey.completion_stream(tenant_id),
            ],
            args=[
                task_id,
                tenant_id,
                expected_state,
                str(now_ms),
                str(ready_score),
                str(self._policy.max_requeue_count),
                reason_code,
                str(getattr(RedisTTL, "STREAM_MAXLEN", 10000)),
            ],
        )

        status = result[0].decode() if isinstance(result[0], bytes) else result[0]
        value = result[1].decode() if len(result) > 1 and isinstance(result[1], bytes) else (result[1] if len(result) > 1 else "0")
        try:
            retries = int(value)
        except Exception:
            retries = 0
        return TaskRequeueResult(status == TASK_REQUEUED, status, retries)
