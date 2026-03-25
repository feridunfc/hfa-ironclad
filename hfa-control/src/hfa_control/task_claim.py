
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from hfa.config.keys import RedisTTL
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
class TaskClaimResult:
    ok: bool
    status: str
    state: str = ""

class TaskClaimManager:
    def __init__(self, redis) -> None:
        self._redis = redis
        self._claim_loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        path = _lua_path("task_claim_start.lua")
        self._claim_loader = LuaScriptLoader(self._redis, path)
        await self._claim_loader.load()

    async def claim_start(self, *, task_id: str, tenant_id: str, worker_instance_id: str, claimed_at_ms: int, scheduler_epoch: str = "", heartbeat_score: int | None = None) -> TaskClaimResult:
        if self._claim_loader is None:
            await self.initialise()
        assert self._claim_loader is not None
        heartbeat_score = heartbeat_score if heartbeat_score is not None else claimed_at_ms
        result = await self._claim_loader.run(
            num_keys=4,
            keys=[
                DagRedisKey.task_state(task_id),
                DagRedisKey.task_meta(task_id),
                DagRedisKey.task_running_zset(tenant_id),
                DagRedisKey.worker_reservation(worker_instance_id),
            ],
            args=[
                task_id,
                worker_instance_id,
                str(claimed_at_ms),
                str(RedisTTL.RUN_STATE),
                str(RedisTTL.RUN_META),
                str(heartbeat_score),
                scheduler_epoch,
            ],
        )
        status = result[0].decode() if isinstance(result[0], bytes) else result[0]
        state = result[1].decode() if len(result) > 1 and isinstance(result[1], bytes) else (result[1] if len(result) > 1 else "")
        return TaskClaimResult(ok=(status == "task_claimed"), status=status, state=state)
