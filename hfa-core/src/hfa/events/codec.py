"""
hfa-core/src/hfa/events/codec.py
IRONCLAD Sprint 10 — Event codec (serialise / deserialise)

Redis Streams store values as byte-strings.
serialize_event()  -> dict[str,str]   ready for XADD
decode_field()     -> typed Python     called by HFAEvent.from_redis()

IRONCLAD rules
--------------
* No print() — logging only.
* Never raise from decode_field() — return safe defaults on parse error.
* cost_cents always int — no float USD.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, is_dataclass
from typing import Any, Dict

logger = logging.getLogger(__name__)


def serialize_event(event: Any) -> Dict[str, str]:
    """Convert a dataclass event to a flat dict[str, str] for XADD."""
    if not is_dataclass(event):
        raise TypeError(f"serialize_event expects dataclass, got {type(event)!r}")

    raw = asdict(event)
    out: Dict[str, str] = {}
    for k, v in raw.items():
        if v is None:
            if k in ("trace_parent", "trace_state"):
                continue        # omit None trace fields entirely
            out[k] = ""
        elif isinstance(v, (dict, list)):
            out[k] = json.dumps(v, separators=(",", ":"))
        elif isinstance(v, bool):
            out[k] = "true" if v else "false"
        else:
            out[k] = str(v)
    return out


def decode_field(field_name: str, raw: Any, type_hint: str) -> Any:
    """
    Decode a single Redis Streams byte-value to the annotated Python type.
    On any parse error: returns a safe zero/empty default and logs a warning.
    """
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if raw == "" or raw is None:
        return _default_for(type_hint)

    # Collections stored as JSON
    _json_hints = ("Dict[str, Any]", "List[int]", "List[str]",
                   "dict", "list")
    if type_hint in _json_hints or type_hint.startswith(("Dict", "List")):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("codec.decode_field: JSON error field=%s raw=%r",
                           field_name, raw[:80])
            return _default_for(type_hint)

    if type_hint == "int":
        try:
            return int(raw)
        except (ValueError, TypeError):
            logger.warning("codec.decode_field: int cast failed field=%s raw=%r",
                           field_name, raw)
            return 0

    if type_hint == "float":
        try:
            return float(raw)
        except (ValueError, TypeError):
            logger.warning("codec.decode_field: float cast failed field=%s raw=%r",
                           field_name, raw)
            return 0.0

    if type_hint == "bool":
        return raw.lower() in ("true", "1", "yes")

    return raw     # str / Optional[str]


def _default_for(type_hint: str) -> Any:
    if type_hint == "int":
        return 0
    if type_hint == "float":
        return 0.0
    if type_hint == "bool":
        return False
    if type_hint.startswith("List"):
        return []
    if type_hint.startswith("Dict"):
        return {}
    if type_hint.startswith("Optional"):
        return None
    return ""
