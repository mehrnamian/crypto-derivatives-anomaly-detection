from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

LOGGER = structlog.get_logger(__name__)


def calculate_previous_utc_day(
    gold_path: Path,
    *,
    reference_time: datetime | None = None,
) -> dict[str, Any]:
    if reference_time is not None and (
        reference_time.tzinfo is None or reference_time.utcoffset() is None
    ):
        raise ValueError("reference_time must be timezone-aware")
    now = (reference_time or datetime.now(UTC)).astimezone(UTC)
    today = now.date()
    day_start = pd.Timestamp(today, tz="UTC") - pd.Timedelta(days=1)
    day_end = day_start + pd.Timedelta(days=1)
    if not gold_path.is_file():
        present = 0
    else:
        frame = pd.read_parquet(gold_path, columns=["minute", "coverage"])
        frame["minute"] = pd.to_datetime(frame["minute"], utc=True, errors="coerce")
        day_rows = frame.loc[(frame["minute"] >= day_start) & (frame["minute"] < day_end)]
        present = int(day_rows.loc[day_rows["coverage"] > 0, "minute"].nunique())
    expected = 1440
    completeness = present / expected * 100.0
    result = {
        "utc_day": str(today - timedelta(days=1)),
        "expected_minute_buckets": expected,
        "present_minute_buckets": present,
        "missing_minute_buckets": expected - present,
        "completeness_percent": round(completeness, 4),
        "target_percent": 99.5,
        "passed": completeness >= 99.5,
    }
    LOGGER.info("daily_data_quality", **result)
    return result
