"""
tests/integration/conftest.py
IRONCLAD Sprint 15 — Real Redis fixtures via Testcontainers

Fixtures
--------
redis_container  (session-scoped)  — single Redis container per test run
real_redis       (function-scoped) — flushed client per test

Usage
-----
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_something(real_redis):
        await real_redis.set("key", "value")
        ...

Running
-------
    # Integration only
    pytest tests/integration -q -m integration

    # With full suite
    pytest tests/ -q

Requirements
------------
    pip install testcontainers redis

CI notes
--------
GitHub Actions provides Docker by default. The redis:7-alpine image is
pulled on first run and cached. Integration tests add ~10–15s per suite run.

Fakeredis vs real Redis
-----------------------
Unit tests in tests/core use fakeredis for speed. Integration tests here
use a real Redis container to verify:
  - Lua eval (EVALSHA) behavior for atomic rate limiting
  - MULTI/EXEC transaction semantics
  - TTL and EXPIRE precision
  - XREADGROUP / XAUTOCLAIM stream semantics
These cannot be reliably reproduced by fakeredis.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from redis.asyncio import Redis


# ---------------------------------------------------------------------------
# Testcontainers Redis — session-scoped (one container for all integration tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def redis_container():
    """
    Start a Redis 7 Alpine container for the test session.
    Requires: pip install testcontainers
    """
    try:
        from testcontainers.redis import RedisContainer
    except ImportError:
        pytest.skip(
            "testcontainers not installed. "
            "Run: pip install testcontainers  to enable integration tests."
        )

    with RedisContainer("redis:7-alpine") as container:
        yield container


# ---------------------------------------------------------------------------
# Per-test Redis client — flushed between tests
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def real_redis(redis_container):
    """
    Async Redis client connected to the test container.
    Database is flushed after each test to ensure isolation.
    """
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )

    try:
        await client.flushdb()
        yield client
    finally:
        await client.flushdb()
        await client.aclose()
