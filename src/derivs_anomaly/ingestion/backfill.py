from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import structlog

from derivs_anomaly.domain.envelope import EventEnvelope
from derivs_anomaly.ingestion.binance import FUTURES_REST, EventSink
from derivs_anomaly.ingestion.gaps import GapJournal

LOGGER = structlog.get_logger(__name__)


class BinanceBackfill:
    """Best-effort REST backfill for mark prices and open interest history.

    Binance's public liquidation WebSocket is sampled and has no equivalent
    historical liquidation endpoint; that unrecoverable part of gaps is recorded.
    """

    def __init__(
        self,
        sink: EventSink,
        *,
        symbol: str = "BTCUSDT",
        client: httpx.AsyncClient | None = None,
        max_backfill_minutes: int = 7 * 24 * 60,
    ) -> None:
        self.sink = sink
        self.symbol = symbol.upper()
        self.client = client or httpx.AsyncClient(timeout=httpx.Timeout(15.0))
        self._owns_client = client is None
        self.max_backfill_minutes = max_backfill_minutes

    async def backfill(self, start: datetime, end: datetime) -> dict[str, int | str]:
        start = _utc(start)
        end = _utc(end)
        now = datetime.now(UTC)
        if end <= start:
            raise ValueError("backfill end must be after start")
        if start > now + timedelta(minutes=1) or end > now + timedelta(minutes=1):
            raise ValueError("backfill interval cannot be in the future")
        if end - start > timedelta(minutes=self.max_backfill_minutes):
            start = end - timedelta(minutes=self.max_backfill_minutes)
        mark_count = await self._backfill_mark_klines(start, end)
        oi_count = await self._backfill_open_interest(start, end)
        result = {
            "mark_bars": mark_count,
            "open_interest_snapshots": oi_count,
            "liquidations_recovered": 0,
            "limitation": (
                "Binance has no equivalent public historical endpoint for sampled "
                "liquidation stream events"
            ),
        }
        LOGGER.info(
            "backfill_complete",
            symbol=self.symbol,
            start=start.isoformat(),
            end=end.isoformat(),
            **result,
        )
        return result

    async def _get(self, endpoint: str, params: dict[str, Any]) -> Any:
        response = await self.client.get(f"{FUTURES_REST}{endpoint}", params=params)
        response.raise_for_status()
        return response.json()

    async def _backfill_mark_klines(self, start: datetime, end: datetime) -> int:
        cursor = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        count = 0
        while cursor < end_ms:
            records = await self._get(
                "/fapi/v1/markPriceKlines",
                {
                    "symbol": self.symbol,
                    "interval": "1m",
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1000,
                },
            )
            if not records:
                break
            for row in records:
                if not isinstance(row, list) or len(row) < 5:
                    continue
                event_ms = int(row[0])
                event_time = datetime.fromtimestamp(event_ms / 1000, UTC)
                received = datetime.now(UTC)
                envelope = EventEnvelope.wrap(
                    source="binance-usdm-rest-backfill",
                    stream="mark",
                    symbol=self.symbol,
                    event_time=event_time,
                    received_time=received,
                    unique_fields={"symbol": self.symbol, "interval": "1m", "open_time": event_ms},
                    payload={"p": str(row[4]), "r": None, "backfill_kind": "mark_price_kline"},
                )
                await self.sink.send(envelope)
                count += 1
            next_cursor = int(records[-1][0]) + 60_000
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            await asyncio.sleep(0.15)
        return count

    async def _backfill_open_interest(self, start: datetime, end: datetime) -> int:
        cursor = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        count = 0
        while cursor < end_ms:
            records = await self._get(
                "/futures/data/openInterestHist",
                {
                    "symbol": self.symbol,
                    "period": "5m",
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 500,
                },
            )
            if not records:
                break
            for record in records:
                event_ms = int(record["timestamp"])
                event_time = datetime.fromtimestamp(event_ms / 1000, UTC)
                received = datetime.now(UTC)
                payload = {
                    "symbol": self.symbol,
                    "time": event_ms,
                    "openInterest": str(record["sumOpenInterest"]),
                    "backfill_kind": "open_interest_history_5m",
                }
                envelope = EventEnvelope.wrap(
                    source="binance-usdm-rest-backfill",
                    stream="open_interest",
                    symbol=self.symbol,
                    event_time=event_time,
                    received_time=received,
                    unique_fields={"symbol": self.symbol, "timestamp": event_ms},
                    payload=payload,
                )
                await self.sink.send(envelope)
                count += 1
            next_cursor = int(records[-1]["timestamp"]) + 1
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            await asyncio.sleep(0.15)
        return count

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()
        close = getattr(self.sink, "close", None)
        if close is not None:
            await close()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("gap timestamps must be timezone-aware")
    return value.astimezone(UTC)


async def recover_open_gaps(
    journal_path: Path,
    sink: EventSink,
    *,
    symbol: str = "BTCUSDT",
) -> list[dict[str, int | str]]:
    backfiller = BinanceBackfill(sink, symbol=symbol)
    journal = GapJournal(journal_path, event_sink=sink, symbol=symbol)
    results = []
    try:
        for start, gap_end in journal.pending():
            result = await backfiller.backfill(start, gap_end)
            results.append(result)
            await journal.record(start, gap_end, state="recovered")
    finally:
        await backfiller.close()
    return results
