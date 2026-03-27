
from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from hfa.runtime.payload_config import (
    get_payload_bucket,
    get_payload_prefix,
    get_payload_store_base_backoff_seconds,
    get_payload_store_max_retries,
    get_payload_store_timeout_seconds,
)
from hfa.runtime.payload_metrics import inc_errors


@dataclass(frozen=True)
class PayloadEnvelope:
    payload_ref: str
    payload_size: int
    checksum: str
    payload_type: str = "binary"


class PayloadIntegrityError(RuntimeError):
    pass


class PayloadStore:
    async def put(self, data: bytes) -> PayloadEnvelope:
        raise NotImplementedError

    async def get(self, ref: str, *, expected_checksum: Optional[str] = None) -> bytes:
        raise NotImplementedError


class LocalFilePayloadStore(PayloadStore):
    def __init__(self, root_dir: str | None = None) -> None:
        self._root = Path(root_dir or os.getenv("PAYLOAD_LOCAL_DIR") or tempfile.gettempdir() + "/ironclad_payloads")
        self._root.mkdir(parents=True, exist_ok=True)

    async def put(self, data: bytes) -> PayloadEnvelope:
        checksum = hashlib.sha256(data).hexdigest()
        key = f"{uuid.uuid4().hex}.bin"
        path = self._root / key
        path.write_bytes(data)
        return PayloadEnvelope(
            payload_ref=f"file://{path.as_posix()}",
            payload_size=len(data),
            checksum=checksum,
        )

    async def get(self, ref: str, *, expected_checksum: Optional[str] = None) -> bytes:
        if not ref.startswith("file://"):
            raise ValueError(f"Unsupported local payload ref: {ref}")
        path = Path(ref[len('file://'):])
        data = path.read_bytes()
        if expected_checksum is not None:
            checksum = hashlib.sha256(data).hexdigest()
            if checksum != expected_checksum:
                raise PayloadIntegrityError(f"Checksum mismatch for {ref}")
        return data


class S3PayloadStore(PayloadStore):
    def __init__(self, bucket: str | None = None, prefix: str | None = None) -> None:
        self._bucket = bucket or get_payload_bucket()
        self._prefix = prefix or get_payload_prefix()

    async def _with_retries(self, op):
        retries = get_payload_store_max_retries()
        base_backoff = get_payload_store_base_backoff_seconds()
        timeout = get_payload_store_timeout_seconds()
        last_exc = None
        for attempt in range(retries):
            try:
                return await asyncio.wait_for(op(), timeout=timeout)
            except Exception as exc:
                last_exc = exc
                inc_errors()
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(base_backoff * (2 ** attempt))
        raise last_exc

    async def put(self, data: bytes) -> PayloadEnvelope:
        checksum = hashlib.sha256(data).hexdigest()
        key = f"{self._prefix}/{uuid.uuid4().hex}.bin"

        async def _op():
            import aioboto3
            session = aioboto3.Session()
            async with session.client("s3") as s3:
                await s3.put_object(Bucket=self._bucket, Key=key, Body=data)
            return None

        await self._with_retries(_op)
        return PayloadEnvelope(
            payload_ref=f"s3://{self._bucket}/{key}",
            payload_size=len(data),
            checksum=checksum,
        )

    async def get(self, ref: str, *, expected_checksum: Optional[str] = None) -> bytes:
        if not ref.startswith("s3://"):
            raise ValueError(f"Unsupported s3 payload ref: {ref}")
        bucket_and_key = ref[len("s3://"):]
        bucket, key = bucket_and_key.split("/", 1)

        async def _op():
            import aioboto3
            session = aioboto3.Session()
            async with session.client("s3") as s3:
                response = await s3.get_object(Bucket=bucket, Key=key)
                return await response["Body"].read()

        data = await self._with_retries(_op)
        if expected_checksum is not None:
            checksum = hashlib.sha256(data).hexdigest()
            if checksum != expected_checksum:
                raise PayloadIntegrityError(f"Checksum mismatch for {ref}")
        return data
