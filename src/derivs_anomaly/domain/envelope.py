from __future__ import annotations

import json
import re
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from derivs_anomaly.domain.event_key import build_event_key


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    source: str
    stream: str
    symbol: str
    event_time: datetime
    received_time: datetime
    event_key: str
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        for field_name in ("source", "stream", "symbol"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if not re.fullmatch(r"[0-9a-f]{64}", self.event_key):
            raise ValueError("event_key must be a lowercase SHA-256 hex digest")
        object.__setattr__(self, "symbol", self.symbol.upper())
        object.__setattr__(self, "event_time", _as_utc(self.event_time, "event_time"))
        object.__setattr__(self, "received_time", _as_utc(self.received_time, "received_time"))
        if not isinstance(self.payload, Mapping):
            raise ValueError("payload must be a mapping")
        payload = deepcopy(dict(self.payload))
        try:
            json.dumps(payload, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("payload must contain only finite JSON-compatible values") from exc
        object.__setattr__(self, "payload", payload)

    @classmethod
    def wrap(
        cls,
        *,
        source: str,
        stream: str,
        symbol: str,
        event_time: datetime,
        received_time: datetime,
        unique_fields: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> EventEnvelope:
        return cls(
            source=source,
            stream=stream,
            symbol=symbol,
            event_time=event_time,
            received_time=received_time,
            event_key=build_event_key(stream, unique_fields),
            payload=payload,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "stream": self.stream,
            "symbol": self.symbol,
            "event_time": self.event_time.isoformat().replace("+00:00", "Z"),
            "received_time": self.received_time.isoformat().replace("+00:00", "Z"),
            "event_key": self.event_key,
            "payload": deepcopy(self.payload),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EventEnvelope:
        required = {
            "source",
            "stream",
            "symbol",
            "event_time",
            "received_time",
            "event_key",
            "payload",
        }
        if not isinstance(data, Mapping) or not required.issubset(data):
            raise ValueError("envelope is missing required fields")
        return cls(
            source=data["source"],
            stream=data["stream"],
            symbol=data["symbol"],
            event_time=_parse_datetime(data["event_time"]),
            received_time=_parse_datetime(data["received_time"]),
            event_key=data["event_key"],
            payload=data["payload"],
        )


def _as_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be a timezone-aware datetime")
    return value.astimezone(UTC)


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value, "datetime")
    if not isinstance(value, str):
        raise ValueError("datetime value must be an ISO-8601 string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        return _as_utc(datetime.fromisoformat(normalized), "datetime")
    except ValueError as exc:
        raise ValueError("datetime value must be a timezone-aware ISO-8601 string") from exc
