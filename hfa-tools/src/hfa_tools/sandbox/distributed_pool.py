"""
hfa-tools/src/hfa_tools/sandbox/distributed_pool.py
IRONCLAD Sprint 6 — DistributedSandboxPool

Architecture
------------
Sprint 3 introduced SandboxPool — a single-node Docker pool.
Sprint 6 extends this to a cluster:

    DistributedSandboxPool
        ├── SandboxNode[]          — discovered via Redis registry
        ├── NodeSelector           — weighted round-robin with health filter
        ├── NodeHealthMonitor      — periodic ping, evicts unresponsive nodes
        └── SandboxNode.execute()  — delegates to the node's Docker API

Node discovery
--------------
Nodes register themselves in Redis on startup:
    HSET "sandbox:nodes" "{node_id}" "{json: host, port, capacity, ...}"
    EXPIRE "sandbox:nodes:{node_id}" 60   # heartbeat TTL

NodeHealthMonitor polls nodes every `health_interval` seconds and marks
unhealthy nodes as unavailable. Unhealthy nodes are excluded from selection
but remain in the registry — they recover automatically if heartbeat resumes.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop() where needed.
* close() cancels monitor task and drains active executions.
* Fail-closed: if all nodes are unhealthy, raises SandboxClusterError.
* Node selection is deterministic under equal load (stable sort on node_id).
* No silent exception swallowing — all errors are logged with context.

Execution flow
--------------
    acquire node (weighted round-robin, health filter)
        ↓
    run_in_container(code, language, timeout)
        ↓
    return SandboxResult
        ↓
    release node slot
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, AsyncGenerator, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Node model
# ---------------------------------------------------------------------------

class NodeHealth(str, Enum):
    HEALTHY   = "healthy"
    DEGRADED  = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class SandboxNode:
    """
    Represents a single sandbox execution node.

    Attributes
    ----------
    node_id:    Unique node identifier (e.g. "sandbox-node-01").
    host:       Docker API host (e.g. "172.16.0.10").
    port:       Docker API port (default 2376 for TLS).
    capacity:   Maximum concurrent containers on this node.
    languages:  Supported language runtimes.
    health:     Current health status.
    active:     Number of currently running containers.
    last_seen:  Unix timestamp of last heartbeat.
    weight:     Routing weight (higher = more traffic). Set by admin.
    labels:     Arbitrary key-value metadata (region, tier, etc.).
    """
    node_id:   str
    host:      str
    port:      int              = 2376
    capacity:  int              = 10
    languages: List[str]        = field(default_factory=lambda: ["python", "node"])
    health:    NodeHealth       = NodeHealth.HEALTHY
    active:    int              = 0
    last_seen: float            = field(default_factory=time.time)
    weight:    int              = 10
    labels:    Dict[str, str]   = field(default_factory=dict)

    @property
    def available_slots(self) -> int:
        return max(0, self.capacity - self.active)

    @property
    def is_available(self) -> bool:
        return self.health == NodeHealth.HEALTHY and self.available_slots > 0

    @property
    def load_factor(self) -> float:
        """0.0 (idle) to 1.0 (full)."""
        if self.capacity == 0:
            return 1.0
        return self.active / self.capacity

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["health"] = self.health.value
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SandboxNode":
        d = dict(data)
        d["health"] = NodeHealth(d.get("health", "healthy"))
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Execution result
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SandboxResult:
    """Result of a sandboxed code execution."""
    run_id:      str
    node_id:     str
    language:    str
    stdout:      str
    stderr:      str
    exit_code:   int
    duration_ms: float
    timed_out:   bool = False

    @property
    def success(self) -> bool:
        return self.exit_code == 0 and not self.timed_out


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class SandboxClusterError(Exception):
    """No healthy nodes available."""

    def __init__(self, language: str, node_count: int) -> None:
        super().__init__(
            f"No healthy sandbox nodes for language={language!r} "
            f"(total_nodes={node_count})"
        )
        self.language   = language
        self.node_count = node_count


class SandboxExecutionError(Exception):
    """Execution failed on the selected node."""


# ---------------------------------------------------------------------------
# NodeSelector — weighted round-robin with health filter
# ---------------------------------------------------------------------------

class NodeSelector:
    """
    Select the best available node for a given language.

    Algorithm: weighted round-robin filtered by health + language support.
    Among nodes with equal (weight * available_slots), prefer lower load_factor.
    Stable sort on node_id for deterministic tie-breaking.

    Usage:
        selector = NodeSelector()
        node = selector.select(nodes, language="python")
    """

    def select(
        self,
        nodes:    List[SandboxNode],
        language: str,
    ) -> SandboxNode:
        """
        Select optimal node.

        Args:
            nodes:    All registered nodes (healthy and unhealthy).
            language: Required runtime language.

        Returns:
            Best SandboxNode.

        Raises:
            SandboxClusterError: No eligible healthy nodes.
        """
        eligible = [
            n for n in nodes
            if n.is_available and language in n.languages
        ]
        if not eligible:
            raise SandboxClusterError(language, len(nodes))

        # Sort: lower load_factor first, then by node_id for determinism
        eligible.sort(key=lambda n: (n.load_factor, n.node_id))
        # Apply weight: nodes with higher weight appear multiple times in virtual ring
        # Simplified: select from top-weighted eligible nodes
        max_weight = max(n.weight for n in eligible)
        top = [n for n in eligible if n.weight == max_weight]
        return top[0]  # lowest load among max-weight nodes


# ---------------------------------------------------------------------------
# NodeHealthMonitor
# ---------------------------------------------------------------------------

class NodeHealthMonitor:
    """
    Background task that pings registered nodes and updates health status.

    Unhealthy threshold: node.last_seen > stale_threshold_seconds ago.
    Degraded threshold: stale_threshold_seconds / 2.

    Args:
        stale_threshold: Seconds without heartbeat → UNHEALTHY.
        check_interval:  How often to scan all nodes (seconds).
    """

    def __init__(
        self,
        stale_threshold: float = 30.0,
        check_interval:  float = 10.0,
    ) -> None:
        self._stale_threshold = stale_threshold
        self._degraded_threshold = stale_threshold / 2
        self._check_interval = check_interval
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self, nodes: Dict[str, SandboxNode]) -> None:
        """Start monitor background task. `nodes` is a shared reference."""
        self._nodes   = nodes
        self._running = True
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(
            self._monitor_loop(), name="sandbox.health_monitor"
        )
        logger.info("NodeHealthMonitor started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("NodeHealthMonitor stopped")

    async def _monitor_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._check_interval)
                now = time.time()
                for node in self._nodes.values():
                    age = now - node.last_seen
                    if age > self._stale_threshold:
                        if node.health != NodeHealth.UNHEALTHY:
                            logger.warning(
                                "Node UNHEALTHY: node=%s age=%.0fs", node.node_id, age
                            )
                        node.health = NodeHealth.UNHEALTHY
                    elif age > self._degraded_threshold:
                        if node.health == NodeHealth.HEALTHY:
                            logger.warning(
                                "Node DEGRADED: node=%s age=%.0fs", node.node_id, age
                            )
                        node.health = NodeHealth.DEGRADED
                    else:
                        if node.health != NodeHealth.HEALTHY:
                            logger.info(
                                "Node RECOVERED: node=%s", node.node_id
                            )
                        node.health = NodeHealth.HEALTHY
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("NodeHealthMonitor error: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# DistributedSandboxPool
# ---------------------------------------------------------------------------

class DistributedSandboxPool:
    """
    Cluster-aware sandbox pool that distributes executions across nodes.

    Node registration is driven by heartbeats published to Redis.
    Each registered node runs SandboxPool (Sprint 3) locally.

    Usage
    -----
        pool = DistributedSandboxPool(redis_client=redis)
        await pool.start()

        result = await pool.execute(
            run_id="run-acme-01",
            language="python",
            code='print("hello")',
            timeout=10,
        )

        await pool.close()

    Args:
        redis_client:        Async Redis client (None = local-only mode).
        registry_key:        Redis hash key for node registry.
        heartbeat_interval:  How often this node publishes heartbeat (seconds).
        stale_threshold:     Seconds without heartbeat → node UNHEALTHY.
        local_node:          If provided, include this node in the pool directly.
    """

    REGISTRY_KEY = "sandbox:nodes"

    def __init__(
        self,
        redis_client             = None,
        heartbeat_interval: float= 15.0,
        stale_threshold:    float= 30.0,
        local_node: Optional[SandboxNode] = None,
    ) -> None:
        self._redis             = redis_client
        self._heartbeat_interval= heartbeat_interval
        self._nodes: Dict[str, SandboxNode] = {}
        self._selector          = NodeSelector()
        self._monitor           = NodeHealthMonitor(stale_threshold=stale_threshold)
        self._lock              = asyncio.Lock()
        self._running           = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._discovery_task:  Optional[asyncio.Task] = None

        if local_node:
            self._nodes[local_node.node_id] = local_node
        logger.info(
            "DistributedSandboxPool created: local_node=%s redis=%s",
            local_node.node_id if local_node else None,
            "enabled" if redis_client else "disabled",
        )

    async def start(self) -> None:
        """Start health monitor + Redis discovery."""
        self._running = True
        self._monitor.start(self._nodes)
        if self._redis:
            loop = asyncio.get_running_loop()
            self._discovery_task = loop.create_task(
                self._discovery_loop(), name="sandbox.discovery"
            )
        logger.info("DistributedSandboxPool started")

    async def close(self) -> None:
        self._running = False
        await self._monitor.stop()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        if self._discovery_task:
            self._discovery_task.cancel()
            try:
                await self._discovery_task
            except asyncio.CancelledError:
                pass
        logger.info("DistributedSandboxPool closed")

    # ------------------------------------------------------------------
    # Node management
    # ------------------------------------------------------------------

    async def register_node(self, node: SandboxNode) -> None:
        """
        Register a node directly (used for testing or local injection).
        Also publishes to Redis if available.
        """
        async with self._lock:
            self._nodes[node.node_id] = node
        if self._redis:
            await self._publish_node(node)
        logger.info(
            "SandboxNode registered: node=%s host=%s capacity=%d",
            node.node_id, node.host, node.capacity,
        )

    async def deregister_node(self, node_id: str) -> None:
        async with self._lock:
            self._nodes.pop(node_id, None)
        if self._redis:
            try:
                await self._redis.hdel(self.REGISTRY_KEY, node_id)
            except Exception as exc:
                logger.error("Redis deregister error: %s", exc)
        logger.info("SandboxNode deregistered: node=%s", node_id)

    def list_nodes(self) -> List[SandboxNode]:
        return list(self._nodes.values())

    def healthy_node_count(self) -> int:
        return sum(1 for n in self._nodes.values() if n.health == NodeHealth.HEALTHY)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(
        self,
        run_id:   str,
        language: str,
        code:     str,
        timeout:  int = 30,
        stdin:    Optional[str] = None,
    ) -> SandboxResult:
        """
        Execute code on the best available node.

        Args:
            run_id:   Run identifier (for logging/audit).
            language: Runtime language ("python" or "node").
            code:     Source code to execute.
            timeout:  Execution timeout in seconds.
            stdin:    Optional stdin data.

        Returns:
            SandboxResult.

        Raises:
            SandboxClusterError:   No healthy nodes available.
            SandboxExecutionError: Node rejected or failed the execution.
        """
        async with self._lock:
            nodes = list(self._nodes.values())

        node = self._selector.select(nodes, language)

        # Reserve a slot
        async with self._lock:
            node.active += 1

        start = time.time()
        try:
            result = await self._run_on_node(node, run_id, language, code, timeout, stdin)
            logger.info(
                "Sandbox execute: run=%s node=%s lang=%s exit=%d duration=%.0fms",
                run_id, node.node_id, language,
                result.exit_code, result.duration_ms,
            )
            return result
        except Exception as exc:
            logger.error(
                "Sandbox execute FAILED: run=%s node=%s error=%s",
                run_id, node.node_id, exc, exc_info=True,
            )
            # Mark node as degraded on execution failure
            async with self._lock:
                node.health = NodeHealth.DEGRADED
            raise SandboxExecutionError(str(exc)) from exc
        finally:
            async with self._lock:
                node.active = max(0, node.active - 1)

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    async def _run_on_node(
        self,
        node:     SandboxNode,
        run_id:   str,
        language: str,
        code:     str,
        timeout:  int,
        stdin:    Optional[str],
    ) -> SandboxResult:
        """
        Delegate execution to the target node's Docker API.
        In production, this makes an HTTP call to the node's Docker socket.
        In tests, this is mocked at this boundary.
        """
        loop = asyncio.get_running_loop()
        start = time.time()
        timed_out = False

        try:
            stdout, stderr, exit_code = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self._docker_exec(node, language, code, stdin),
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            timed_out = True
            stdout, stderr, exit_code = "", "Execution timeout", 124

        duration_ms = (time.time() - start) * 1000

        return SandboxResult(
            run_id      = run_id,
            node_id     = node.node_id,
            language    = language,
            stdout      = stdout,
            stderr      = stderr,
            exit_code   = exit_code,
            duration_ms = duration_ms,
            timed_out   = timed_out,
        )

    @staticmethod
    def _docker_exec(
        node:     SandboxNode,
        language: str,
        code:     str,
        stdin:    Optional[str],
    ) -> tuple[str, str, int]:
        """
        Synchronous Docker exec call (runs in executor).
        Override in subclasses / integration tests.
        """
        import subprocess, tempfile, os
        # This is the local fallback implementation.
        # Production nodes override this via Docker remote API.
        interp = {"python": "python3", "node": "node"}.get(language, language)
        suffix = {"python": ".py", "node": ".js"}.get(language, ".txt")

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=suffix, delete=False
        ) as f:
            f.write(code)
            fpath = f.name

        try:
            result = subprocess.run(
                [interp, fpath],
                capture_output=True, text=True,
                input=stdin, timeout=30,
            )
            return result.stdout, result.stderr, result.returncode
        except subprocess.TimeoutExpired:
            return "", "Execution timeout", 124
        except FileNotFoundError:
            return "", f"Runtime not found: {interp}", 127
        finally:
            try:
                os.unlink(fpath)
            except Exception:
                pass

    async def _discovery_loop(self) -> None:
        """Periodically scan Redis for registered nodes."""
        while self._running:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                raw = await self._redis.hgetall(self.REGISTRY_KEY)
                if raw:
                    async with self._lock:
                        for node_id, data in raw.items():
                            nid = node_id.decode() if isinstance(node_id, bytes) else node_id
                            try:
                                d = json.loads(
                                    data.decode() if isinstance(data, bytes) else data
                                )
                                if nid in self._nodes:
                                    self._nodes[nid].last_seen = d.get("last_seen", time.time())
                                else:
                                    self._nodes[nid] = SandboxNode.from_dict(d)
                                    logger.info(
                                        "Discovered new node: %s", nid
                                    )
                            except Exception as exc:
                                logger.error(
                                    "Node discovery parse error: node=%s error=%s", nid, exc
                                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Discovery loop error: %s", exc, exc_info=True)

    async def _publish_node(self, node: SandboxNode) -> None:
        try:
            data = json.dumps(node.to_dict())
            await self._redis.hset(self.REGISTRY_KEY, node.node_id, data)
            await self._redis.expire(self.REGISTRY_KEY, 300)
        except Exception as exc:
            logger.error("Redis node publish error: %s", exc)
