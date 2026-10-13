from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any


def build_event_key(stream: str, unique_fields: Mapping[str, Any]) -> str:
    """Return a stable SHA-256 key for a stream and its unique fields."""
    if not isinstance(stream, str) or not stream.strip():
        raise ValueError("stream must be a non-empty string")
    if not isinstance(unique_fields, Mapping) or not unique_fields:
        raise ValueError("unique_fields must be a non-empty mapping")
    payload = {
        "stream": stream,
        "unique_fields": _normalize_json_value(unique_fields),
    }
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _normalize_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("event-key fields cannot contain non-finite numbers")
        return value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("event-key mapping keys must be strings")
        return {key: _normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_json_value(item) for item in value]
    raise TypeError(f"unsupported event-key field type: {type(value).__name__}")
