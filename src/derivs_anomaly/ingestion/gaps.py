from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from derivs_anomaly.domain.envelope import EventEnvelope


class GapJournal:
    """Append-only local record of connection interruption and recovery times."""

    def __init__(
        self,
        path: Path,
        *,
        event_sink: Any = None,
        symbol: str = "BTCUSDT",
    ) -> None:
        self.path = path
        self.event_sink = event_sink
        self.symbol = symbol
        self._lock = asyncio.Lock()

    async def record(
        self,
        started_at: datetime,
        ended_at: datetime | None,
        *,
        state: str | None = None,
    ) -> None:
        status = state or ("open" if ended_at is None else "closed")
        if status not in {"open", "closed", "recovered"}:
            raise ValueError("gap state must be open, closed, or recovered")
        if status == "open" and ended_at is not None:
            raise ValueError("open gap must not have an end timestamp")
        if status != "open" and ended_at is None:
            raise ValueError("closed/recovered gap requires an end timestamp")
        row = {
            "stream": "combined",
            "started_at": started_at.astimezone(UTC).isoformat(),
            "ended_at": ended_at.astimezone(UTC).isoformat() if ended_at else None,
            "recorded_at": datetime.now(UTC).isoformat(),
            "state": status,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(row, separators=(",", ":"), allow_nan=False)
        async with self._lock:
            await asyncio.to_thread(self._append, line)
            if self.event_sink is not None:
                envelope = EventEnvelope.wrap(
                    source="derivs-anomaly-ingestion",
                    stream="gap",
                    symbol=self.symbol,
                    event_time=ended_at or started_at,
                    received_time=datetime.now(UTC),
                    unique_fields={
                        "started_at": row["started_at"],
                        "ended_at": row["ended_at"],
                        "state": row["state"],
                    },
                    payload={
                        "started_at": row["started_at"],
                        "ended_at": row["ended_at"],
                        "state": row["state"],
                    },
                )
                await self.event_sink.send(envelope)
            self._state_cache = getattr(self, "_state_cache", {})
            self._state_cache[row["started_at"]] = row

    def _append(self, line: str) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def pending(self) -> list[tuple[datetime, datetime]]:
        """Return closed but not yet recovered intervals from the journal."""
        states: dict[str, dict[str, object]] = {}
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        row = json.loads(line)
                        states[str(row["started_at"])] = row
        pending = []
        for row in states.values():
            if row.get("state") != "closed" or not row.get("ended_at"):
                continue
            started = datetime.fromisoformat(str(row["started_at"]).replace("Z", "+00:00"))
            ended = datetime.fromisoformat(str(row["ended_at"]).replace("Z", "+00:00"))
            pending.append((started, ended))
        return sorted(pending)