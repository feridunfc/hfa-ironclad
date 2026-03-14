"""
hfa-worker/src/hfa_worker/redis_utils.py
Minimal Redis stream helpers for worker runtime.
"""
from __future__ import annotations


async def ensure_consumer_group(
    redis,
    stream: str,
    group: str,
    start_id: str = "0",
    mkstream: bool = True,
) -> None:
    """
    Ensure Redis consumer group exists.
    Ignores BUSYGROUP if the group already exists.
    """
    try:
        await redis.xgroup_create(
            name=stream,
            groupname=group,
            id=start_id,
            mkstream=mkstream,
        )
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def ack_message(
    redis,
    stream: str,
    group: str,
    msg_id,
) -> int:
    """
    ACK a message in the given consumer group.
    """
    return await redis.xack(stream, group, msg_id)