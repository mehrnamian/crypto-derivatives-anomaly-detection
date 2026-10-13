from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class GoldMinute:
    minute: datetime
    symbol: str
    mark_price: float | None
    estimated_funding: float | None
    open_interest: float | None
    open_interest_change: float | None
    liquidation_count: int
    liquidation_notional: float
    coverage: float


@dataclass(frozen=True, slots=True)
class AnomalyScore:
    minute: datetime
    score: float
    is_alert: bool
    model_version: str
    features: dict[str, float]


@dataclass(frozen=True, slots=True)
class MergedAlert:
    started_at: datetime
    ended_at: datetime
    score: float
    model_version: str
    alert_count: int


@dataclass(frozen=True, slots=True)
class GapWindow:
    stream: str
    started_at: datetime
    ended_at: datetime | None


@dataclass(frozen=True, slots=True)
class QuarantineRecord:
    reason: str
    stream: str
    raw: dict[str, Any]
