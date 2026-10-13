from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from derivs_anomaly.domain.envelope import EventEnvelope
from derivs_anomaly.ingestion.binance import (
    parse_market_message,
    parse_open_interest,
    parse_rest_mark_price,
)
from derivs_anomaly.ingestion.gaps import GapJournal
from derivs_anomaly.pipeline.process import process_bronze
from derivs_anomaly.pipeline.quality import normalize_envelope


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[EventEnvelope] = []

    async def send(self, envelope: EventEnvelope) -> None:
        self.events.append(envelope)


def test_parse_binance_mark_and_liquidation_messages() -> None:
    received = datetime(2026, 1, 1, tzinfo=UTC)
    mark = parse_market_message(
        {
            "stream": "btcusdt@markPrice@1s",
            "data": {
                "e": "markPriceUpdate",
                "E": 1767225900000,
                "s": "BTCUSDT",
                "p": "62000.0",
                "r": "0.0001",
            },
        },
        received,
    )
    liquidation = parse_market_message(
        {
            "stream": "btcusdt@forceOrder",
            "data": {
                "e": "forceOrder",
                "E": 1767225600000,
                "o": {
                    "s": "BTCUSDT",
                    "S": "SELL",
                    "p": "62000.0",
                    "q": "0.5",
                    "T": 1767225600000,
                },
            },
        },
        received,
    )
    assert mark.stream == "mark"
    assert mark.event_time == received + timedelta(minutes=5)
    assert liquidation.stream == "liquidation"
    assert liquidation.payload["o"]["S"] == "SELL"
    assert normalize_envelope(mark)["event_time"] == received


def test_parse_open_interest_uses_received_time_without_exchange_timestamp() -> None:
    received = datetime(2026, 1, 1, tzinfo=UTC)
    envelope = parse_open_interest(
        {"symbol": "BTCUSDT", "openInterest": "123.45"},
        received,
    )
    assert envelope.stream == "open_interest"
    assert envelope.event_time == received
    assert envelope.payload["openInterest"] == "123.45"


def test_parse_open_interest_uses_observation_time_when_exchange_clock_differs() -> None:
    received = datetime(2026, 1, 1, tzinfo=UTC)
    envelope = parse_open_interest(
        {
            "symbol": "BTCUSDT",
            "openInterest": "123.45",
            "time": int((received + timedelta(minutes=5)).timestamp() * 1000),
        },
        received,
    )
    assert envelope.event_time == received
    assert normalize_envelope(envelope)["event_time"] == received


def test_parse_rest_mark_price_as_mark_stream_snapshot() -> None:
    received = datetime(2026, 1, 1, tzinfo=UTC)
    envelope = parse_rest_mark_price(
        {
            "symbol": "BTCUSDT",
            "markPrice": "62000.0",
            "lastFundingRate": "0.0001",
            "time": int((received + timedelta(minutes=5)).timestamp() * 1000),
        },
        received,
    )
    row = normalize_envelope(envelope)
    assert envelope.stream == "mark"
    assert envelope.event_time == received
    assert row["mark_price"] == 62000.0
    assert row["estimated_funding"] == 0.0001


@pytest.mark.asyncio
async def test_gap_journal_publishes_open_and_closed_controls(tmp_path) -> None:
    sink = RecordingSink()
    journal = GapJournal(tmp_path / "gaps.jsonl", event_sink=sink)
    started = datetime(2026, 1, 1, tzinfo=UTC)
    ended = started + timedelta(minutes=2)
    await journal.record(started, None)
    await journal.record(started, ended)

    assert [event.stream for event in sink.events] == ["gap", "gap"]
    assert [event.payload["state"] for event in sink.events] == ["open", "closed"]
    rows = (tmp_path / "gaps.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(rows) == 2
    assert journal.pending() == [(started, ended)]
    await journal.record(started, ended, state="recovered")
    assert journal.pending() == []


def test_gap_control_events_are_not_quarantined_as_market_rows(tmp_path) -> None:
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    start = datetime.now(UTC).replace(second=0, microsecond=0)
    open_event = EventEnvelope.wrap(
        source="derivs-anomaly-ingestion",
        stream="gap",
        symbol="BTCUSDT",
        event_time=start,
        received_time=start,
        unique_fields={"start": start.isoformat(), "state": "open"},
        payload={"started_at": start.isoformat(), "ended_at": None, "state": "open"},
    )
    close_event = EventEnvelope.wrap(
        source="derivs-anomaly-ingestion",
        stream="gap",
        symbol="BTCUSDT",
        event_time=start + timedelta(minutes=1),
        received_time=start + timedelta(minutes=1),
        unique_fields={"start": start.isoformat(), "state": "closed"},
        payload={
            "started_at": start.isoformat(),
            "ended_at": (start + timedelta(minutes=1)).isoformat(),
            "state": "closed",
        },
    )
    (bronze / "gaps.jsonl").write_text(
        "\n".join(json.dumps(event.to_dict()) for event in (open_event, close_event)) + "\n",
        encoding="utf-8",
    )

    result = process_bronze(tmp_path)
    assert result["silver_rows"] == 0
    assert result["quarantined_rows"] == 0
    rows = (tmp_path / "gold" / "gaps.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(rows) == 2
