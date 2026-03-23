
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from hfa.dag.schema import DagRedisKey


_PLACEHOLDER_RE = re.compile(r"\$\{([A-Za-z0-9_\-]+)\.output\}")


class MissingParentOutputError(RuntimeError):
    pass


@dataclass(frozen=True)
class ResolvedInput:
    hydrated: Any
    referenced_task_ids: list[str]
    lineage_record: dict[str, Any]


class InputResolver:
    def __init__(self, redis) -> None:
        self._redis = redis

    async def resolve(self, template: Any) -> ResolvedInput:
        task_ids = sorted(self._collect_task_ids(template))
        outputs = await self._fetch_outputs(task_ids)
        hydrated = self._hydrate(template, outputs)
        lineage = self._build_lineage_record(task_ids, outputs)
        return ResolvedInput(
            hydrated=hydrated,
            referenced_task_ids=task_ids,
            lineage_record=lineage,
        )

    async def persist_lineage(self, *, task_id: str, lineage_record: dict[str, Any], ttl_seconds: int = 86400) -> None:
        key = DagRedisKey.task_lineage(task_id)
        payload = json.dumps(lineage_record, sort_keys=True, ensure_ascii=False)
        await self._redis.set(key, payload, ex=ttl_seconds)

    def _collect_task_ids(self, value: Any) -> set[str]:
        found: set[str] = set()

        def walk(node: Any) -> None:
            if isinstance(node, str):
                for match in _PLACEHOLDER_RE.finditer(node):
                    found.add(match.group(1))
            elif isinstance(node, dict):
                if "__merge__" in node:
                    merge_spec = node["__merge__"]
                    if isinstance(merge_spec, list):
                        for item in merge_spec:
                            if isinstance(item, str):
                                found.add(item)
                    elif isinstance(merge_spec, dict):
                        for task_id in merge_spec.values():
                            if isinstance(task_id, str):
                                found.add(task_id)
                    return
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(value)
        return found

    async def _fetch_outputs(self, task_ids: list[str]) -> dict[str, str]:
        if not task_ids:
            return {}

        keys = [DagRedisKey.task_output(task_id) for task_id in task_ids]
        values = await self._redis.mget(keys)

        outputs: dict[str, str] = {}
        missing: list[str] = []

        for task_id, value in zip(task_ids, values):
            if value is None:
                missing.append(task_id)
            else:
                outputs[task_id] = value

        if missing:
            raise MissingParentOutputError(
                f"Missing parent output for task ids: {', '.join(sorted(missing))}"
            )

        return outputs

    def _decode_output(self, raw: str) -> Any:
        try:
            return json.loads(raw)
        except Exception:
            return raw

    def _hydrate(self, value: Any, outputs: dict[str, str]) -> Any:
        if isinstance(value, str):
            matches = list(_PLACEHOLDER_RE.finditer(value))
            if not matches:
                return value

            if len(matches) == 1 and matches[0].span() == (0, len(value)):
                task_id = matches[0].group(1)
                return self._decode_output(outputs[task_id])

            def repl(match: re.Match[str]) -> str:
                task_id = match.group(1)
                raw = outputs[task_id]
                try:
                    decoded = json.loads(raw)
                    if isinstance(decoded, (dict, list)):
                        return json.dumps(decoded, ensure_ascii=False, sort_keys=True)
                    return str(decoded)
                except Exception:
                    return raw

            return _PLACEHOLDER_RE.sub(repl, value)

        if isinstance(value, dict):
            if "__merge__" in value:
                merge_spec = value["__merge__"]
                merge_mode = value.get("mode", "list")

                if isinstance(merge_spec, list):
                    merged_items = [self._decode_output(outputs[task_id]) for task_id in merge_spec]
                    if merge_mode == "list":
                        return merged_items
                    raise ValueError(f"Unsupported merge mode '{merge_mode}' for list merge spec")

                if isinstance(merge_spec, dict):
                    if merge_mode != "dict":
                        raise ValueError(f"Unsupported merge mode '{merge_mode}' for dict merge spec")
                    return {
                        key: self._decode_output(outputs[task_id])
                        for key, task_id in merge_spec.items()
                    }

                raise ValueError("__merge__ must be list[str] or dict[str, str]")

            return {k: self._hydrate(v, outputs) for k, v in value.items()}

        if isinstance(value, list):
            return [self._hydrate(item, outputs) for item in value]

        return value

    def _build_lineage_record(self, task_ids: list[str], outputs: dict[str, str]) -> dict[str, Any]:
        parents: dict[str, Any] = {}
        for task_id in task_ids:
            raw = outputs[task_id]
            digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            parents[task_id] = {
                "raw": raw,
                "decoded": self._decode_output(raw),
                "sha256": digest,
            }

        overall = hashlib.sha256(
            json.dumps(
                {tid: parents[tid]["sha256"] for tid in sorted(parents)},
                sort_keys=True,
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()

        return {
            "parent_count": len(task_ids),
            "parents": parents,
            "combined_sha256": overall,
        }
