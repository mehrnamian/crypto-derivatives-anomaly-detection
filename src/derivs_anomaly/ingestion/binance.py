from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
import websockets
from websockets.exceptions import ConnectionClosed

from derivs_anomaly.domain.envelope import EventEnvelope

LOGGER = structlog.get_logger(__name__)
FUTURES_REST = "https://fapi.binance.com"
FUTURES_WS = "wss://fstream.binance.com/stream?streams={symbol}@markPrice@1s/{symbol}@forceOrder"


def _utc_millis(value: Any, field: str) -> datetime:
    try:
        millis = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid Binance millisecond timestamp in {field}") from exc
    return datetime.fromtimestamp(millis / 1000, tz=UTC)


def parse_market_message(message: Mapping[str, Any], received_time: datetime) -> EventEnvelope:
    """Parse one Binance combined mark-price or liquidation event."""
    data = message.get("data")
    if not isinstance(data, Mapping):
        raise ValueError("combined stream message must contain an object in 'data'")

    event_type = data.get("e")
    if event_type == "markPriceUpdate":
        stream = "mark"
        event_time = _utc_millis(data.get("E"), "E")
        identity = {"symbol": data.get("s"), "event_time": data.get("E")}
    elif event_type == "forceOrder":
        stream = "liquidation"
        order = data.get("o")
        if not isinstance(order, Mapping):
            raise ValueError("forceOrder event must contain an order object")
        event_time = _utc_millis(order.get("T", data.get("E")), "o.T")
        # Binance's public force-order feed is a sampled feed. Its fields do not
        # expose a unique trade id, so this key represents a published snapshot.
        identity = {
            "symbol": order.get("s"),
            "event_time": order.get("T", data.get("E")),
            "side": order.get("S"),
            "price": order.get("p"),
            "quantity": order.get("q"),
        }
    else:
        raise ValueError(f"unsupported Binance market event: {event_type!r}")

    symbol = str(data.get("s") or (data.get("o") or {}).get("s") or "")
    if not symbol:
        raise ValueError("Binance market event is missing its symbol")
    return EventEnvelope.wrap(
        source="binance-usdm",
        stream=stream,
        symbol=symbol,
        event_time=event_time,
        received_time=received_time,
        unique_fields=identity,
        payload=dict(data),
    )


def parse_open_interest(payload: Mapping[str, Any], received_time: datetime) -> EventEnvelope:
    symbol = str(payload.get("symbol") or "")
    if not symbol:
        raise ValueError("open-interest response is missing symbol")
    observed_at = received_time.astimezone(UTC)
    return EventEnvelope.wrap(
        source="binance-usdm-rest",
        stream="open_interest",
        symbol=symbol,
        event_time=observed_at,
        received_time=observed_at,
        unique_fields={"symbol": symbol, "observed_at": observed_at.isoformat()},
        payload=dict(payload),
    )


def parse_rest_mark_price(payload: Mapping[str, Any], received_time: datetime) -> EventEnvelope:
    symbol = str(payload.get("symbol") or "")
    if not symbol:
        raise ValueError("mark-price response is missing symbol")
    mark_price = payload.get("markPrice")
    funding_rate = payload.get("lastFundingRate")
    if mark_price is None or funding_rate is None:
        raise ValueError("mark-price response is missing markPrice or lastFundingRate")

    observed_at = received_time.astimezone(UTC)
    event_time_ms = int(observed_at.timestamp() * 1000)
    normalized_payload = {
        "e": "markPriceUpdate",
        "E": event_time_ms,
        "s": symbol,
        "p": str(mark_price),
        "r": str(funding_rate),
    }
    return EventEnvelope.wrap(
        source="binance-usdm-rest",
        stream="mark",
        symbol=symbol,
        event_time=observed_at,
        received_time=observed_at,
        unique_fields={"symbol": symbol, "observed_at": observed_at.isoformat()},
        payload=normalized_payload,
    )


class EventSink:
    async def send(self, envelope: EventEnvelope) -> None:
        raise NotImplementedError


class LocalJsonlSink(EventSink):
    """Append raw envelopes to a local bronze JSONL file (for development/tests)."""

    def __init__(self, bronze_dir: str) -> None:
        from pathlib import Path

        self.directory = Path(bronze_dir)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    async def send(self, envelope: EventEnvelope) -> None:
        now = datetime.now(UTC)
        path = self.directory / f"events-{now:%Y%m%dT%H%M}.jsonl"
        async with self._lock:
            line = json.dumps(envelope.to_dict(), separators=(",", ":"), allow_nan=False)
            await asyncio.to_thread(_append_line, path, line)


def _append_line(path: Any, line: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


class EventHubSink(EventSink):
    """Send events using Entra ID credentials; connection strings are not accepted."""

    def __init__(self, namespace: str, eventhub_name: str) -> None:
        if not namespace or not eventhub_name:
            raise ValueError("Event Hubs namespace and hub name are required")
        self.namespace = namespace
        self.eventhub_name = eventhub_name
        self._producer: Any = None
        self._credential: Any = None
        self._lock = asyncio.Lock()

    async def _get_producer(self) -> Any:
        if self._producer is None:
            from azure.eventhub.aio import EventHubProducerClient
            from azure.identity.aio import DefaultAzureCredential

            self._credential = DefaultAzureCredential()
            self._producer = EventHubProducerClient(
                fully_qualified_namespace=self.namespace,
                eventhub_name=self.eventhub_name,
                credential=self._credential,
            )
        return self._producer

    async def send(self, envelope: EventEnvelope) -> None:
        from azure.eventhub import EventData

        producer = await self._get_producer()
        body = json.dumps(envelope.to_dict(), separators=(",", ":"), allow_nan=False)
        async with self._lock:
            batch = await producer.create_batch()
            batch.add(EventData(body))
            await producer.send_batch(batch)

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.close()
        if self._credential is not None:
            await self._credential.close()


class BinanceIngestor:
    def __init__(
        self,
        sink: EventSink,
        *,
        symbol: str = "BTCUSDT",
        oi_poll_seconds: int = 15,
        watchdog_seconds: int = 30,
        max_connection_seconds: int = 23 * 60 * 60,
        max_backoff_seconds: float = 30.0,
        record_gap: Callable[[datetime, datetime | None], Awaitable[None]] | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.sink = sink
        self.symbol = symbol.upper()
        self.oi_poll_seconds = oi_poll_seconds
        self.watchdog_seconds = watchdog_seconds
        self.max_connection_seconds = max_connection_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.record_gap = record_gap or _ignore_gap
        self.client = client
        self._owns_client = client is None
        self._last_event_time: datetime | None = None
        self._message_count = 0
        self._minute_count = 0
        self._metric_window_started = datetime.now(UTC)
        self._reconnect_count = 0
        self._started_at = datetime.now(UTC)
        self._gap_started_at: datetime | None = None

    async def run(self) -> None:
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self._run_market_stream())
                tasks.create_task(self._poll_mark_price())
                tasks.create_task(self._poll_open_interest())
                tasks.create_task(self._metrics_heartbeat())
        finally:
            if self._owns_client and self.client is not None:
                await self.client.aclose()
            close = getattr(self.sink, "close", None)
            if close is not None:
                await close()

    async def _run_market_stream(self) -> None:
        backoff = 1.0
        while True:
            disconnect_at = datetime.now(UTC)
            try:
                async with websockets.connect(
                    FUTURES_WS.format(symbol=self.symbol.lower()),
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=2**20,
                ) as websocket:
                    connected_at = asyncio.get_running_loop().time()
                    last_seen = connected_at
                    LOGGER.info("websocket_connected", exchange="binance", symbol=self.symbol)
                    while True:
                        elapsed = asyncio.get_running_loop().time() - connected_at
                        if elapsed >= self.max_connection_seconds:
                            LOGGER.info("websocket_proactive_rotation", symbol=self.symbol)
                            break
                        timeout = min(self.watchdog_seconds, self.max_connection_seconds - elapsed)
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                        except TimeoutError:
                            if (
                                asyncio.get_running_loop().time() - last_seen
                                >= self.watchdog_seconds
                            ):
                                raise TimeoutError(
                                    "websocket inactivity watchdog expired"
                                ) from None
                            continue
                        last_seen = asyncio.get_running_loop().time()
                        received = datetime.now(UTC)
                        try:
                            payload = json.loads(raw)
                            envelope = parse_market_message(payload, received)
                        except (json.JSONDecodeError, ValueError, TypeError) as exc:
                            LOGGER.warning("invalid_exchange_message", error=str(exc))
                            continue
                        if self._gap_started_at is not None:
                            await self.record_gap(self._gap_started_at, envelope.event_time)
                            self._gap_started_at = None
                        self._last_event_time = max(
                            self._last_event_time or envelope.event_time,
                            envelope.event_time,
                        )
                        await self.sink.send(envelope)
                        self._message_count += 1
                        self._minute_count += 1
                    backoff = 1.0
            except (ConnectionClosed, OSError, TimeoutError, httpx.HTTPError) as exc:
                LOGGER.warning("websocket_disconnected", error=str(exc))
            if self._gap_started_at is None:
                self._gap_started_at = self._last_event_time or disconnect_at
                await self.record_gap(self._gap_started_at, None)
            self._reconnect_count += 1
            delay = min(self.max_backoff_seconds, backoff) * random.uniform(0.7, 1.3)
            LOGGER.info(
                "websocket_reconnect_wait",
                seconds=round(delay, 2),
                reconnect_count=self._reconnect_count,
            )
            await asyncio.sleep(delay)
            backoff = min(self.max_backoff_seconds, backoff * 2)

    async def _poll_mark_price(self) -> None:
        assert self.client is not None
        endpoint = f"{FUTURES_REST}/fapi/v1/premiumIndex"
        LOGGER.info("mark_price_rest_polling_started", symbol=self.symbol)
        while True:
            received = datetime.now(UTC)
            try:
                response = await self.client.get(endpoint, params={"symbol": self.symbol})
                response.raise_for_status()
                envelope = parse_rest_mark_price(response.json(), received)
                await self.sink.send(envelope)
                self._last_event_time = max(
                    self._last_event_time or envelope.event_time,
                    envelope.event_time,
                )
                self._message_count += 1
                self._minute_count += 1
                await asyncio.sleep(self.oi_poll_seconds)
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                LOGGER.warning("mark_price_rest_poll_failed", error=str(exc))
                await asyncio.sleep(min(self.oi_poll_seconds, 10))

    async def _poll_open_interest(self) -> None:
        assert self.client is not None
        endpoint = f"{FUTURES_REST}/fapi/v1/openInterest"
        while True:
            received = datetime.now(UTC)
            try:
                response = await self.client.get(endpoint, params={"symbol": self.symbol})
                response.raise_for_status()
                payload = response.json()
                envelope = parse_open_interest(payload, received)
                await self.sink.send(envelope)
                self._last_event_time = max(
                    self._last_event_time or envelope.event_time,
                    envelope.event_time,
                )
                self._message_count += 1
                self._minute_count += 1
                await asyncio.sleep(self.oi_poll_seconds)
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                LOGGER.warning("open_interest_poll_failed", error=str(exc))
                await asyncio.sleep(min(self.oi_poll_seconds, 10))

    async def _metrics_heartbeat(self) -> None:
        while True:
            await asyncio.sleep(60)
            now = datetime.now(UTC)
            elapsed = max(1.0, (now - self._metric_window_started).total_seconds())
            age = (
                max(0.0, (now - self._last_event_time).total_seconds())
                if self._last_event_time is not None
                else None
            )
            LOGGER.info(
                "ingestion_metrics",
                messages_per_minute=round(self._minute_count * 60 / elapsed, 2),
                reconnect_count=self._reconnect_count,
                latest_message_age_seconds=age,
                uptime_seconds=(now - self._started_at).total_seconds(),
            )
            self._minute_count = 0
            self._metric_window_started = now


async def _ignore_gap(_start: datetime, _end: datetime | None) -> None:
    return None
