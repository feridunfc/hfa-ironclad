"""
hfa-control/src/hfa_control/api/models.py
IRONCLAD Sprint 10 — Control Plane API response models
"""
from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel


class WorkerResponse(BaseModel):
    worker_id:    str
    worker_group: str
    region:       str
    shards:       List[int]
    capacity:     int
    inflight:     int
    load_factor:  float
    status:       str        # "healthy" | "degraded" | "draining" | "dead"
    last_seen:    float
    version:      str
    capabilities: List[str]


class ShardResponse(BaseModel):
    shard:        int
    worker_group: str
    stream_len:   int
    owner_alive:  bool


class PlacementResponse(BaseModel):
    run_id:           str
    tenant_id:        str
    state:            str
    worker_group:     str
    shard:            int
    reschedule_count: int
    admitted_at:      float


class DLQEntryResponse(BaseModel):
    run_id:           str
    tenant_id:        str
    reason:           str
    delivery_count:   int
    dead_lettered_at: float
    original_error:   str
    cost_cents:       int      # integer cents — no float USD


class HealthResponse(BaseModel):
    is_leader:       bool
    instance_id:     str
    region:          str
    registry_size:   int
    healthy_workers: int
    scheduler_lag:   int
    dlq_depth:       int
