"""
hfa-control/src/hfa_control/backpressure.py
IRONCLAD Sprint 21 v3 — Backpressure guard with hysteresis

Design
------
Prevents the scheduler from over-dispatching when the system is under load.
Uses hysteresis bands to prevent oscillation (thrashing between stop/resume).

Hysteresis model
----------------
                  hard_cap ──────────── STOP (hard)
                  soft_high ─────────── SOFT THROTTLE (halve)
                  soft_low ──────────── RESUME (exit throttle)
                  (below soft_low)───── NORMAL

This prevents the common failure mode of:
  inflight=99 → resume → dispatch=100 → stop → ... (thrashing)

Capacity-weighted evaluation
-----------------------------
Workers have different capacities. A fleet of 1 large worker + 100 tiny
workers should not be treated the same as 101 medium workers.
total_effective_capacity = sum(worker.capacity) not len(workers).

IRONCLAD rules
--------------
* Hard limits always stop dispatch immediately (no hysteresis).
* Soft limits use hysteresis: enter at soft_high, exit at soft_low.
* Reason codes are always emitted for observability.
* Empty worker pool is not a backpressure condition (separate error).
* All thresholds default to 0 = disabled (opt-in, backward compatible).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BackpressureSignal:
    active: bool
    reason: Optional[str]
    allowed_dispatches: int
    severity: str                # "none" | "soft" | "hard"

    @property
    def is_hard(self) -> bool:
        return self.severity == "hard"

    @property
    def is_soft(self) -> bool:
        return self.severity == "soft"


class BackpressureGuard:
    """
    Evaluates system state and returns a dispatch limit per cycle.

    Maintains internal hysteresis state to avoid oscillation.
    """

    def __init__(self, config) -> None:
        self._config = config
        self._in_soft_throttle = False  # hysteresis state

    def evaluate(
        self,
        *,
        global_inflight: int,
        total_capacity: int,           # capacity-weighted sum
        queue_depth: int,
        worker_load_factors: list[float],
        worker_capacities: Optional[list[int]] = None,  # per-worker capacity
        max_dispatches_requested: int,
    ) -> BackpressureSignal:
        """
        Evaluate backpressure conditions and return allowed dispatch count.

        Args:
            global_inflight:         Running runs across all workers.
            total_capacity:          Sum of all schedulable worker capacities.
            queue_depth:             Total queued runs (not yet dispatched).
            worker_load_factors:     Per-worker load factor (inflight/capacity).
            worker_capacities:       Per-worker capacity (for weighted saturation).
            max_dispatches_requested: How many dispatches the loop wants.

        Returns:
            BackpressureSignal with allowed_dispatches and severity.
        """
        hard_cap         = int(getattr(self._config, "backpressure_hard_inflight_cap", 0) or 0)
        soft_high        = int(getattr(self._config, "backpressure_soft_inflight_cap", 0) or 0)
        soft_low_ratio   = float(getattr(self._config, "backpressure_hysteresis_ratio", 0.8) or 0.8)
        max_queue_ratio  = float(getattr(self._config, "backpressure_max_queue_ratio", 10.0) or 10.0)
        sat_threshold    = float(getattr(self._config, "backpressure_worker_saturation", 0.95) or 0.95)

        soft_low = int(soft_high * soft_low_ratio) if soft_high > 0 else 0

        # ── Hard: global inflight cap ────────────────────────────────────
        if hard_cap > 0 and global_inflight >= hard_cap:
            logger.warning(
                "Backpressure HARD: global_inflight=%d >= hard_cap=%d",
                global_inflight, hard_cap,
            )
            return BackpressureSignal(
                active=True,
                reason="global_inflight_hard_cap",
                allowed_dispatches=0,
                severity="hard",
            )

        # ── Hard: capacity-weighted saturation ───────────────────────────
        if worker_load_factors:
            if worker_capacities and len(worker_capacities) == len(worker_load_factors):
                # Weighted saturation: large workers count more
                total_w = sum(worker_capacities)
                if total_w > 0:
                    weighted_load = sum(
                        lf * cap for lf, cap in zip(worker_load_factors, worker_capacities)
                    ) / total_w
                    if weighted_load >= sat_threshold:
                        logger.warning(
                            "Backpressure HARD: weighted_load=%.2f >= sat_threshold=%.2f",
                            weighted_load, sat_threshold,
                        )
                        return BackpressureSignal(
                            active=True,
                            reason="fleet_capacity_saturated",
                            allowed_dispatches=0,
                            severity="hard",
                        )
            else:
                # Fallback: all workers must be saturated (conservative)
                if all(lf >= sat_threshold for lf in worker_load_factors):
                    logger.warning(
                        "Backpressure HARD: all %d workers saturated (>= %.2f)",
                        len(worker_load_factors), sat_threshold,
                    )
                    return BackpressureSignal(
                        active=True,
                        reason="all_workers_saturated",
                        allowed_dispatches=0,
                        severity="hard",
                    )

        # ── Soft: hysteresis band on inflight ────────────────────────────
        if soft_high > 0:
            if not self._in_soft_throttle and global_inflight >= soft_high:
                self._in_soft_throttle = True
                logger.debug(
                    "Backpressure: entering SOFT throttle (inflight=%d >= soft_high=%d)",
                    global_inflight, soft_high,
                )
            elif self._in_soft_throttle and global_inflight < soft_low:
                self._in_soft_throttle = False
                logger.debug(
                    "Backpressure: exiting SOFT throttle (inflight=%d < soft_low=%d)",
                    global_inflight, soft_low,
                )

            if self._in_soft_throttle:
                allowed = max(1, max_dispatches_requested // 2)
                return BackpressureSignal(
                    active=True,
                    reason="global_inflight_soft_cap",
                    allowed_dispatches=allowed,
                    severity="soft",
                )

        # ── Soft: queue depth pressure ───────────────────────────────────
        if total_capacity > 0 and queue_depth > max_queue_ratio * total_capacity:
            allowed = max(1, max_dispatches_requested // 2)
            logger.debug(
                "Backpressure SOFT: queue_depth=%d > %.1f * capacity=%d → %d",
                queue_depth, max_queue_ratio, total_capacity, allowed,
            )
            return BackpressureSignal(
                active=True,
                reason="queue_depth_pressure",
                allowed_dispatches=allowed,
                severity="soft",
            )

        # ── No backpressure ───────────────────────────────────────────────
        return BackpressureSignal(
            active=False,
            reason=None,
            allowed_dispatches=max_dispatches_requested,
            severity="none",
        )
