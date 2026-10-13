from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pandas as pd

from derivs_anomaly.domain.envelope import EventEnvelope
from derivs_anomaly.model.features import build_features
from derivs_anomaly.pipeline.completeness import calculate_previous_utc_day
from derivs_anomaly.pipeline.process import process_bronze


def _envelope(stream: str, event_time: datetime, payload: dict, identity: dict) -> dict:
    return EventEnvelope.wrap(
        source="binance-usdm",
        stream=stream,
        symbol="BTCUSDT",
        event_time=event_time,
        received_time=event_time + timedelta(seconds=1),
        unique_fields=identity,
        payload=payload,
    ).to_dict()


def test_process_is_idempotent_deduplicates_and_quarantines(tmp_path) -> None:
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    first_time = int(now.timestamp() * 1000)
    second_minute = now + timedelta(minutes=1)
    second_time = int(second_minute.timestamp() * 1000)
    valid_payload = {
        "e": "markPriceUpdate",
        "E": first_time,
        "s": "BTCUSDT",
        "p": "62000.0",
        "r": "0.0001",
    }
    rows = [
        _envelope(
            "mark",
            now,
            valid_payload,
            {"event_id": 1},
        ),
        _envelope(
            "mark",
            now,
            valid_payload,
            {"event_id": 1},
        ),
        _envelope(
            "mark",
            second_minute,
            {
                "e": "markPriceUpdate",
                "E": second_time,
                "s": "BTCUSDT",
                "p": "0",
                "r": "0",
            },
            {"event_id": 2},
        ),
    ]
    source = bronze / "capture.jsonl"
    source.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    first = process_bronze(tmp_path)
    first_gold = pd.read_parquet(tmp_path / "gold" / "minutes.parquet")
    first_scores = pd.read_parquet(tmp_path / "gold" / "anomaly_scores.parquet")
    second = process_bronze(tmp_path)
    second_gold = pd.read_parquet(tmp_path / "gold" / "minutes.parquet")
    second_scores = pd.read_parquet(tmp_path / "gold" / "anomaly_scores.parquet")

    pd.testing.assert_frame_equal(first_gold, second_gold)
    pd.testing.assert_frame_equal(first_scores, second_scores)
    assert first["silver_rows"] == second["silver_rows"] == 1
    assert first["duplicate_rows"] == second["duplicate_rows"] == 1
    assert first["quarantined_rows"] == second["quarantined_rows"] == 2
    assert first["gold_minutes"] == second["gold_minutes"] == 1
    quarantined = list((tmp_path / "quarantine").glob("*.jsonl"))
    assert len(quarantined) == 1
    items = [
        json.loads(line)
        for line in quarantined[0].read_text(encoding="utf-8").splitlines()
    ]
    assert {item["reason"] for item in items} == {
        "duplicate event_key",
        "mark price must be positive",
    }
    assert pd.read_parquet(tmp_path / "gold" / "minutes.parquet").iloc[0]["mark_price"] == 62000.0


def test_features_are_past_only_and_sorted() -> None:
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    rows = []
    for i in range(63):
        rows.append({
            "minute": start + pd.Timedelta(minutes=i),
            "symbol": "BTCUSDT",
            "estimated_funding": 0.0001 + i * i * 0.00000001,
            "open_interest_change": float(i % 3),
            "liquidation_notional": float(i),
        })
    frame = pd.DataFrame(rows).iloc[::-1].reset_index(drop=True)
    features = build_features(frame)
    assert features["minute"].is_monotonic_increasing
    assert pd.isna(features.loc[0, "funding_change"])
    # First differences need one prior row; then a full 60-value baseline.
    assert pd.isna(features.loc[59, "funding_change_robust_z"])
    assert pd.notna(features.loc[61, "funding_change_robust_z"])


def test_future_dated_records_are_quarantined(tmp_path) -> None:
    future = datetime.now(UTC) + timedelta(minutes=5)
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    row = _envelope("mark", future, {"p": "62000", "r": "0.0001"}, {"id": 1})
    (bronze / "future.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")

    result = process_bronze(tmp_path)
    assert result["silver_rows"] == 0
    assert result["quarantined_rows"] == 1
    quarantine = next((tmp_path / "quarantine").glob("*.jsonl"))
    assert "more than one minute in the future" in quarantine.read_text(encoding="utf-8")


def test_daily_completeness_uses_all_1440_utc_buckets(tmp_path) -> None:
    gold_path = tmp_path / "minutes.parquet"
    day_start = pd.Timestamp("2026-01-01T00:00:00Z")
    minutes = pd.date_range(day_start, periods=1440, freq="min")
    pd.DataFrame({
        "minute": minutes,
        "coverage": [0.0 if index < 6 else 1.0 for index in range(1440)],
    }).to_parquet(gold_path, index=False)

    report = calculate_previous_utc_day(
        gold_path,
        reference_time=datetime(2026, 1, 2, 0, 10, tzinfo=UTC),
    )
    assert report["present_minute_buckets"] == 1434
    assert report["completeness_percent"] == 99.5833
    assert report["passed"] is True


def test_data_older_than_two_hours_remains_in_silver_only(tmp_path) -> None:
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    recent_time = now - timedelta(minutes=5)
    recent = _envelope(
        "mark",
        recent_time,
        {"p": "62000", "r": "0.0001"},
        {"id": "recent"},
    )
    (bronze / "initial.jsonl").write_text(json.dumps(recent) + "\n", encoding="utf-8")
    process_bronze(tmp_path)
    original_gold = pd.read_parquet(tmp_path / "gold" / "minutes.parquet")

    old_time = now - timedelta(hours=4)
    late = _envelope("mark", old_time, {"p": "61000", "r": "0.0001"}, {"id": "late"})
    (bronze / "late.jsonl").write_text(json.dumps(late) + "\n", encoding="utf-8")
    result = process_bronze(tmp_path)

    assert result["silver_rows"] == 2
    assert len(pd.read_parquet(tmp_path / "gold" / "minutes.parquet")) == len(original_gold)
    silver_files = list((tmp_path / "silver").rglob("*.parquet"))
    assert sum(len(pd.read_parquet(path)) for path in silver_files) == 2
