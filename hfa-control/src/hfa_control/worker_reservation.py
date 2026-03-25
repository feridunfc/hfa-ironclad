
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from hfa.dag.schema import DagRedisKey
from hfa.lua.loader import LuaScriptLoader

_LUA_DIR = (
    Path(__file__).resolve().parent.parent.parent.parent / "hfa-core" / "src" / "hfa" / "lua"
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
class WorkerReservationResult:
    ok: bool
    status: str
    scheduler_epoch: str = ""

class WorkerReservationManager:
    def __init__(self, redis, reservation_ttl_seconds: int = 30, scheduler_id: str = "scheduler-1") -> None:
        self._redis = redis
        self._reservation_ttl_seconds = reservation_ttl_seconds
        self._scheduler_id = scheduler_id
        self._loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        path = _lua_path("reserve_worker.lua")
        self._loader = LuaScriptLoader(self._redis, path)
        await self._loader.load()

    async def reserve(self, *, worker_id: str, task_id: str, scheduler_epoch: str, reserved_at_ms: int) -> WorkerReservationResult:
        if self._loader is None:
            await self.initialise()
        assert self._loader is not None
        result = await self._loader.run(
            num_keys=1,
            keys=[DagRedisKey.worker_reservation(worker_id)],
            args=[
                worker_id,
                task_id,
                scheduler_epoch,
                str(reserved_at_ms),
                str(self._reservation_ttl_seconds),
                self._scheduler_id,
            ],
        )
        status = result[0].decode() if isinstance(result[0], bytes) else result[0]
        epoch = result[1].decode() if len(result) > 1 and isinstance(result[1], bytes) else (result[1] if len(result) > 1 else "")
        return WorkerReservationResult(ok=(status == "reservation_created"), status=status, scheduler_epoch=epoch)

    async def get(self, worker_id: str) -> dict[str, str]:
        return await self._redis.hgetall(DagRedisKey.worker_reservation(worker_id))

    async def release(self, worker_id: str) -> None:
        await self._redis.delete(DagRedisKey.worker_reservation(worker_id))

    async def renew(self, worker_id: str, ttl_seconds: int | None = None) -> bool:
        ttl_seconds = ttl_seconds or self._reservation_ttl_seconds
        key = DagRedisKey.worker_reservation(worker_id)
        exists = await self._redis.exists(key)
        if not exists:
            return False
        return bool(await self._redis.expire(key, ttl_seconds))
