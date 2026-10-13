from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from derivs_anomaly.domain import (
    AnomalyScore,
    EventEnvelope,
    build_event_key,
    merge_nearby_alerts,
)
from derivs_anomaly.model.scoring import ROBUST_Z_COLUMNS, score_features


def test_event_key_is_stable_and_namespaced() -> None:
    assert build_event_key("mark", {"a": 1, "b": 2}) == build_event_key(
        "mark", {"b": 2, "a": 1}
    )
    assert build_event_key("mark", {"a": 1}) != build_event_key("liquidation", {"a": 1})
    assert build_event_key("s", {"stream": "x"}) != build_event_key("s", {"x": "stream"})


@pytest.mark.parametrize(
    ("stream", "fields", "error"),
    [
        ("", {"id": 1}, ValueError),
        ("stream", {}, ValueError),
        ("stream", {1: "id"}, ValueError),
        ("stream", {"id": float("nan")}, ValueError),
        ("stream", {"id": object()}, TypeError),
    ],
)
def test_event_key_rejects_ambiguous_values(stream, fields, error) -> None:
    with pytest.raises(error):
        build_event_key(stream, fields)


def test_envelope_normalizes_utc_and_round_trips() -> None:
    envelope = EventEnvelope.wrap(
        source="binance",
        stream="mark",
        symbol="btcusdt",
        event_time=datetime(2026, 1, 1, 2, tzinfo=UTC) + timedelta(hours=2),
        received_time=datetime(2026, 1, 1, 4, tzinfo=UTC),
        unique_fields={"event_id": 42},
        payload={"price": 100.0},
    )

    assert envelope.symbol == "BTCUSDT"
    assert envelope.event_time == datetime(2026, 1, 1, 4, tzinfo=UTC)
    assert EventEnvelope.from_dict(envelope.to_dict()) == envelope


def test_envelope_requires_aware_timestamps_and_json_payload() -> None:
    common = {
        "source": "binance",
        "stream": "mark",
        "symbol": "BTCUSDT",
        "received_time": datetime.now(UTC),
        "unique_fields": {"id": 1},
    }
    with pytest.raises(ValueError, match="timezone-aware"):
        EventEnvelope.wrap(
            **common,
            event_time=datetime(2026, 1, 1),
            payload={"price": 1},
        )
    with pytest.raises(ValueError, match="JSON-compatible"):
        EventEnvelope.wrap(
            **common,
            event_time=datetime.now(UTC),
            payload={"price": float("inf")},
        )


def test_envelope_serialization_does_not_share_payload_mutations() -> None:
    payload = {"nested": {"value": 1}}
    envelope = EventEnvelope.wrap(
        source="binance",
        stream="mark",
        symbol="BTCUSDT",
        event_time=datetime.now(UTC),
        received_time=datetime.now(UTC),
        unique_fields={"id": 1},
        payload=payload,
    )
    payload["nested"]["value"] = 2
    serialized = envelope.to_dict()
    serialized["payload"]["nested"]["value"] = 3
    assert envelope.payload["nested"]["value"] == 1


def test_merge_nearby_alerts_merges_only_within_window() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    scores = [
        AnomalyScore(start, 2.0, True, "v1", {}),
        AnomalyScore(start + timedelta(minutes=10), 4.0, True, "v1", {}),
        AnomalyScore(start + timedelta(minutes=26), 3.0, True, "v1", {}),
        AnomalyScore(start + timedelta(minutes=30), 100.0, False, "v1", {}),
        AnomalyScore(start + timedelta(minutes=27), 5.0, True, "v2", {}),
    ]

    merged = merge_nearby_alerts(scores)
    assert [(alert.alert_count, alert.model_version, alert.score) for alert in merged] == [
        (2, "v1", 4.0),
        (1, "v1", 3.0),
        (1, "v2", 5.0),
    ]


def test_merge_rejects_negative_window() -> None:
    with pytest.raises(ValueError, match="not be negative"):
        merge_nearby_alerts([], window=timedelta(seconds=-1))


def test_scoring_merges_alerts_and_caps_daily_episodes() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = []
    for minute in range(1440):
        strong_score = minute % 20 == 0 and minute // 20 < 10
        rows.append({
            "minute": start + timedelta(minutes=minute),
            "symbol": "BTCUSDT",
            **{column: 8.0 if strong_score else 0.0 for column in ROBUST_Z_COLUMNS},
        })

    scores = score_features(pd.DataFrame(rows), threshold=6.0)
    assert int(scores["is_alert"].sum()) == 5