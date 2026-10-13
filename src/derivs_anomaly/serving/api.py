from __future__ import annotations

import hmac
import json
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

from derivs_anomaly.config import Settings


class GoldCache:
    def __init__(
        self,
        gold_dir: Path,
        *,
        storage_account_url: str | None = None,
        ttl_seconds: int = 30,
    ) -> None:
        self.gold_dir = gold_dir
        self.storage_account_url = storage_account_url
        self.ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        self._loaded_at = 0.0
        self._gold = pd.DataFrame()
        self._scores = pd.DataFrame()
        self._storage_client: Any = None
        self._credential: Any = None

    def snapshot(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        with self._lock:
            if time.monotonic() - self._loaded_at >= self.ttl_seconds:
                if self.storage_account_url:
                    self._refresh_from_adls()
                self._gold = self._read(self.gold_dir / "minutes.parquet")
                self._scores = self._read(self.gold_dir / "anomaly_scores.parquet")
                cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=24)
                if not self._gold.empty and "minute" in self._gold:
                    self._gold = self._gold.loc[self._gold["minute"] >= cutoff].copy()
                if not self._scores.empty and "minute" in self._scores:
                    self._scores = self._scores.loc[self._scores["minute"] >= cutoff].copy()
                self._loaded_at = time.monotonic()
            return self._gold.copy(), self._scores.copy()

    def _refresh_from_adls(self) -> None:
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        if self._storage_client is None:
            self._credential = DefaultAzureCredential()
            self._storage_client = DataLakeServiceClient(
                account_url=self.storage_account_url,
                credential=self._credential,
            )
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        filesystem = self._storage_client.get_file_system_client("gold")
        for name in ("minutes.parquet", "anomaly_scores.parquet", "gaps.jsonl"):
            path = self.gold_dir / name
            try:
                downloader = filesystem.get_file_client(name).download_file()
                temporary = path.with_suffix(path.suffix + ".tmp")
                temporary.write_bytes(downloader.readall())
                temporary.replace(path)
            except Exception as exc:
                if getattr(exc, "status_code", None) != 404:
                    raise

    @staticmethod
    def _read(path: Path) -> pd.DataFrame:
        if not path.is_file():
            return pd.DataFrame()
        frame = pd.read_parquet(path)
        if "minute" in frame:
            frame["minute"] = pd.to_datetime(frame["minute"], utc=True, errors="coerce")
        return frame


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or Settings()
    cache = GoldCache(config.gold_dir, storage_account_url=config.storage_account_url)
    app = FastAPI(
        title="Crypto Market Data Anomalies API",
        version="0.1.0",
        description="Read-only market-data anomalies. Not trading signals or recommendations.",
        docs_url=None,
        redoc_url=None,
    )

    async def require_api_key(
        x_api_key: Annotated[str | None, Header()] = None,
    ) -> None:
        expected = config.api_key.get_secret_value() if config.api_key else None
        if expected is None:
            raise HTTPException(status_code=503, detail="API authentication is not configured")
        if x_api_key is None or not hmac.compare_digest(x_api_key, expected):
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

    @app.get("/health", dependencies=[Depends(require_api_key)])
    async def health() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/status", dependencies=[Depends(require_api_key)])
    async def status() -> dict[str, Any]:
        gold, scores = cache.snapshot()
        if gold.empty or "minute" not in gold:
            return {
                "latest_data_timestamp": None,
                "freshness_seconds": None,
                "model_version": config.model_version,
            }
        latest = pd.to_datetime(gold["minute"], utc=True).max().to_pydatetime()
        score_version = (
            str(scores.iloc[-1]["model_version"])
            if not scores.empty and "model_version" in scores
            else config.model_version
        )
        completeness = (
            float(gold["coverage"].tail(1440).gt(0).mean())
            if "coverage" in gold
            else None
        )
        latest_reconnect = None
        gaps_path = config.gold_dir / "gaps.jsonl"
        if not gaps_path.is_file():
            gaps_path = config.data_dir / "gaps.jsonl"
        if gaps_path.is_file():
            try:
                with gaps_path.open("r", encoding="utf-8") as handle:
                    closed = [json.loads(line) for line in handle if line.strip()]
                closed = [item for item in closed if item.get("state") == "closed"]
                if closed:
                    latest_reconnect = closed[-1].get("recorded_at")
            except (OSError, json.JSONDecodeError):
                latest_reconnect = None
        return {
            "latest_data_timestamp": latest.isoformat(),
            "freshness_seconds": max(0.0, (datetime.now(UTC) - latest).total_seconds()),
            "model_version": score_version,
            "completeness_24h": completeness,
            "latest_reconnect": latest_reconnect,
        }

    @app.get("/anomalies", dependencies=[Depends(require_api_key)])
    async def anomalies(
        from_time: Annotated[datetime | None, Query(alias="from")] = None,
        to_time: Annotated[datetime | None, Query(alias="to")] = None,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        end = to_time or now
        start = from_time or (end - timedelta(hours=24))
        if start.tzinfo is None or end.tzinfo is None:
            raise HTTPException(
                status_code=422,
                detail="from/to timestamps must include a timezone",
            )
        start = start.astimezone(UTC)
        end = end.astimezone(UTC)
        if start > end:
            raise HTTPException(status_code=422, detail="from must not be later than to")
        if end - start > timedelta(hours=24):
            raise HTTPException(status_code=422, detail="query range cannot exceed 24 hours")
        gold, scores = cache.snapshot()
        if scores.empty:
            return {"items": [], "count": 0}
        scores["minute"] = pd.to_datetime(scores["minute"], utc=True, errors="coerce")
        selected = scores.loc[
            (scores["minute"] >= pd.Timestamp(start))
            & (scores["minute"] <= pd.Timestamp(end))
        ].copy()
        if not gold.empty:
            selected = selected.merge(
                gold,
                on=["minute", "symbol"],
                how="left",
                suffixes=("", "_gold"),
            )
        selected = selected.sort_values("minute")
        items = []
        for row in selected.to_dict(orient="records"):
            item = {}
            for key, value in row.items():
                if pd.isna(value):
                    item[key] = None
                elif isinstance(value, (datetime, pd.Timestamp)):
                    item[key] = value.isoformat()
                elif hasattr(value, "item"):
                    item[key] = value.item()
                else:
                    item[key] = value
            items.append(item)
        return {"items": items, "count": len(items)}

    return app


app = create_app()
