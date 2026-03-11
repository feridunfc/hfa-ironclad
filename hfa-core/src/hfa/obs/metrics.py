"""
hfa-core/src/hfa/obs/metrics.py
IRONCLAD Sprint 7 Mini3 — Telemetry Baseline

Goals
-----
- Centralized metric helper API
- No-op safe when OpenTelemetry is unavailable
- Fire-and-forget instrumentation
- Minimal, stable contract for Sprint 7 Mini3

Notes
-----
- This module does NOT bootstrap exporters/providers.
- Export/bootstrap belongs to a later hardening step.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from opentelemetry import metrics as _otel_metrics
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    logger.info("OpenTelemetry not installed; HFAMetrics running in no-op mode")


class _Instruments:
    """Lazy singleton container for OTel instruments."""
    _instance: Optional["_Instruments"] = None

    def __init__(self) -> None:
        self._ready = False

        if not _OTEL_AVAILABLE:
            return

        try:
            meter = _otel_metrics.get_meter("hfa.core")

            self.runs_total = meter.create_counter(
                name="hfa_runs_total",
                description="Total number of orchestration runs completed",
                unit="1",
            )
            self.run_latency = meter.create_histogram(
                name="hfa_run_latency_ms",
                description="Run duration in milliseconds",
                unit="ms",
            )
            self.queue_depth = meter.create_up_down_counter(
                name="hfa_queue_depth",
                description="Current orchestrator queue depth",
                unit="1",
            )
            self.sandbox_slots = meter.create_up_down_counter(
                name="hfa_sandbox_slots_active",
                description="Current number of active sandbox slots",
                unit="1",
            )

            self._ready = True
        except Exception as exc:
            logger.warning("HFAMetrics instrument init failed: %s", exc)

    @classmethod
    def get(cls) -> "_Instruments":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        cls._instance = None


class HFAMetrics:
    """
    Fire-and-forget metric helpers.

    Design constraints:
    - must never break business logic
    - must be safe when OTel is absent
    - must keep label cardinality low
    """

    @staticmethod
    def inc_runs_total(tenant_id: str, status: str) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.runs_total.add(1, {"tenant_id": tenant_id, "status": status})
        except Exception as exc:
            logger.debug("HFAMetrics.inc_runs_total failed: %s", exc)

    @staticmethod
    def record_run_latency(tenant_id: str, duration_ms: float) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.run_latency.record(duration_ms, {"tenant_id": tenant_id})
        except Exception as exc:
            logger.debug("HFAMetrics.record_run_latency failed: %s", exc)

    @staticmethod
    def inc_queue_depth(tenant_id: str) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.queue_depth.add(1, {"tenant_id": tenant_id})
        except Exception as exc:
            logger.debug("HFAMetrics.inc_queue_depth failed: %s", exc)

    @staticmethod
    def dec_queue_depth(tenant_id: str) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.queue_depth.add(-1, {"tenant_id": tenant_id})
        except Exception as exc:
            logger.debug("HFAMetrics.dec_queue_depth failed: %s", exc)

    @staticmethod
    def inc_sandbox_slots(node_id: str) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.sandbox_slots.add(1, {"node_id": node_id})
        except Exception as exc:
            logger.debug("HFAMetrics.inc_sandbox_slots failed: %s", exc)

    @staticmethod
    def dec_sandbox_slots(node_id: str) -> None:
        try:
            inst = _Instruments.get()
            if inst._ready:
                inst.sandbox_slots.add(-1, {"node_id": node_id})
        except Exception as exc:
            logger.debug("HFAMetrics.dec_sandbox_slots failed: %s", exc)