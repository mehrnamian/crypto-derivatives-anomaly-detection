from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pandera.pandas as pa

from derivs_anomaly.domain.envelope import EventEnvelope

BASE_SCHEMA = pa.DataFrameSchema(
    {
        "event_time": pa.Column("datetime64[ns, UTC]", nullable=False),
        "received_time": pa.Column("datetime64[ns, UTC]", nullable=False),
        "event_key": pa.Column(str, nullable=False),
        "source": pa.Column(str, nullable=False),
        "stream": pa.Column(str, nullable=False),
        "symbol": pa.Column(str, nullable=False),
        "mark_price": pa.Column(float, nullable=True),
        "estimated_funding": pa.Column(float, nullable=True),
        "open_interest": pa.Column(float, nullable=True),
        "liquidation_price": pa.Column(float, nullable=True),
        "liquidation_quantity": pa.Column(float, nullable=True),
        "liquidation_notional": pa.Column(float, nullable=True),
        "liquidation_side": pa.Column(str, nullable=True),
        "extra": pa.Column(str, nullable=False),
        "raw_payload": pa.Column(str, nullable=False),
    },
    strict=True,
    coerce=True,
)

MARK_FIELDS = {"e", "E", "s", "p", "i", "r", "T"}
OI_FIELDS = {"openInterest", "symbol", "time"}
LIQUIDATION_FIELDS = {"e", "E", "T", "s", "o"}
LIQUIDATION_ORDER_FIELDS = {"s", "S", "o", "f", "q", "p", "ap", "X", "T", "l", "z", "ps", "cp"}


def _finite_float(value: Any, field_name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


def normalize_envelope(envelope: EventEnvelope) -> dict[str, Any]:
    payload = envelope.payload
    event_time = (
        envelope.received_time
        if envelope.stream in {"mark", "open_interest"}
        else envelope.event_time
    )
    row: dict[str, Any] = {
        "event_time": pd.Timestamp(event_time).tz_convert("UTC"),
        "received_time": pd.Timestamp(envelope.received_time).tz_convert("UTC"),
        "event_key": envelope.event_key,
        "source": envelope.source,
        "stream": envelope.stream,
        "symbol": envelope.symbol,
        "mark_price": None,
        "estimated_funding": None,
        "open_interest": None,
        "liquidation_price": None,
        "liquidation_quantity": None,
        "liquidation_notional": None,
        "liquidation_side": None,
    }

    if envelope.stream == "mark":
        row["mark_price"] = _finite_float(payload.get("p"), "mark price")
        row["estimated_funding"] = (
            _finite_float(payload["r"], "estimated funding")
            if payload.get("r") is not None
            else None
        )
        extra = {key: value for key, value in payload.items() if key not in MARK_FIELDS}
        if row["mark_price"] <= 0:
            raise ValueError("mark price must be positive")
    elif envelope.stream == "open_interest":
        row["open_interest"] = _finite_float(payload.get("openInterest"), "open interest")
        extra = {key: value for key, value in payload.items() if key not in OI_FIELDS}
        if row["open_interest"] <= 0:
            raise ValueError("open interest must be positive")
    elif envelope.stream == "liquidation":
        order = payload.get("o")
        if not isinstance(order, dict):
            raise ValueError("liquidation event is missing its order object")
        row["liquidation_price"] = _finite_float(order.get("p"), "liquidation price")
        row["liquidation_quantity"] = _finite_float(order.get("q"), "liquidation quantity")
        row["liquidation_notional"] = row["liquidation_price"] * row["liquidation_quantity"]
        row["liquidation_side"] = str(order.get("S", "")).upper()
        extra = {
            "event": {
                key: value for key, value in payload.items() if key not in LIQUIDATION_FIELDS
            },
            "order": {
                key: value for key, value in order.items()
                if key not in LIQUIDATION_ORDER_FIELDS
            },
        }
        if row["liquidation_price"] <= 0 or row["liquidation_quantity"] <= 0:
            raise ValueError("liquidation price and quantity must be positive")
        if row["liquidation_side"] not in {"BUY", "SELL"}:
            raise ValueError("liquidation side must be BUY or SELL")
    else:
        raise ValueError(f"unsupported stream: {envelope.stream}")

    if event_time > datetime.now(UTC).replace(microsecond=0) + pd.Timedelta(minutes=1):
        raise ValueError("event_time is more than one minute in the future")
    row["extra"] = json.dumps(extra, sort_keys=True, separators=(",", ":"), allow_nan=False)
    row["raw_payload"] = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    validated = BASE_SCHEMA.validate(pd.DataFrame([row]))
    return validated.iloc[0].to_dict()
