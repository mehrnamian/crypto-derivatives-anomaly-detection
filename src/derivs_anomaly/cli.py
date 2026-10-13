from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Any

import structlog
import websockets

from derivs_anomaly.config import Settings
from derivs_anomaly.ingestion.backfill import recover_open_gaps
from derivs_anomaly.ingestion.binance import (
    FUTURES_WS,
    BinanceIngestor,
    EventHubSink,
    LocalJsonlSink,
)
from derivs_anomaly.ingestion.gaps import GapJournal
from derivs_anomaly.logging import configure_logging
from derivs_anomaly.pipeline.process import process_bronze

LOGGER = structlog.get_logger(__name__)


async def _probe(minutes: float, symbol: str) -> dict[str, Any]:
    if minutes <= 0 or minutes > 10:
        raise ValueError("probe duration must be greater than zero and no more than 10 minutes")
    received = Counter()
    errors: list[str] = []
    started = datetime.now(UTC)
    url = FUTURES_WS.format(symbol=symbol.lower())
    try:
        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            max_size=2**20,
        ) as socket:
            deadline = asyncio.get_running_loop().time() + minutes * 60
            while asyncio.get_running_loop().time() < deadline:
                remaining = deadline - asyncio.get_running_loop().time()
                try:
                    message = await asyncio.wait_for(socket.recv(), timeout=remaining)
                except TimeoutError:
                    break
                decoded = json.loads(message)
                event = decoded.get("data", {}).get("e")
                received[str(event or "unknown")] += 1
    except Exception as exc:
        errors.append(type(exc).__name__)
    ended = datetime.now(UTC)
    return {
        "source": "binance-usdm",
        "symbol": symbol.upper(),
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_seconds": round((ended - started).total_seconds(), 2),
        "message_counts_by_event": dict(received),
        "total_messages": sum(received.values()),
        "errors": errors,
    }


async def _ingest(settings: Settings) -> None:
    if settings.eventhub_namespace and settings.eventhub_name:
        sink = EventHubSink(settings.eventhub_namespace, settings.eventhub_name)
    else:
        sink = LocalJsonlSink(str(settings.bronze_dir))
        LOGGER.warning(
            "using_local_bronze_sink",
            message="Set Event Hubs namespace and name for dev Azure ingestion",
        )
    journal = GapJournal(
        settings.data_dir / "gaps.jsonl",
        event_sink=sink,
        symbol=settings.symbol,
    )
    ingestor = BinanceIngestor(
        sink,
        symbol=settings.symbol,
        oi_poll_seconds=settings.oi_poll_seconds,
        watchdog_seconds=settings.watchdog_seconds,
        max_connection_seconds=settings.ws_max_hours * 3600,
        record_gap=journal.record,
    )
    await ingestor.run()


def _add_common_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-dir", default=None, help="Override local data lake root")


def main() -> None:
    parser = argparse.ArgumentParser(prog="derivs-anomaly")
    subparsers = parser.add_subparsers(dest="command", required=True)
    ingest_parser = subparsers.add_parser("ingest", help="Ingest Binance futures data")
    _add_common_options(ingest_parser)
    process_parser = subparsers.add_parser("process", help="Run bronze-to-gold processing")
    _add_common_options(process_parser)
    probe_parser = subparsers.add_parser(
        "probe", help="Count source WebSocket messages for Phase 0"
    )
    probe_parser.add_argument("--minutes", type=float, default=5.0)
    probe_parser.add_argument("--symbol", default=None)
    api_parser = subparsers.add_parser("api", help="Run the authenticated read-only API")
    api_parser.add_argument("--host", default=None)
    api_parser.add_argument("--port", type=int, default=None)
    dashboard_parser = subparsers.add_parser("dashboard", help="Run the Streamlit dashboard")
    dashboard_parser.add_argument("--port", type=int, default=8501)
    recovery_parser = subparsers.add_parser("recover", help="Backfill open WebSocket gaps")
    recovery_parser.add_argument("--data-dir", default=None, help="Override local data lake root")
    training_parser = subparsers.add_parser(
        "train", help="Evaluate chronological models and register the winner"
    )
    training_parser.add_argument("--gold", default=None, help="Path to gold minutes Parquet")
    training_parser.add_argument("--events", default="data/reference/events.csv")
    training_parser.add_argument(
        "--tracking-uri", default=None, help="Azure ML MLflow tracking URI"
    )
    subparsers.add_parser("quality", help="Measure completeness for the last full UTC day")

    args = parser.parse_args()
    settings = Settings()
    if getattr(args, "data_dir", None):
        settings = settings.model_copy(update={"data_dir": Path(args.data_dir)})
        settings.model_post_init(None)
    configure_logging(settings.log_json)

    if args.command == "probe":
        report = asyncio.run(_probe(args.minutes, (args.symbol or settings.symbol)))
        print(json.dumps(report, indent=2))
        if report["errors"] or report["total_messages"] == 0:
            raise SystemExit(1)
    elif args.command == "ingest":
        settings.ensure_directories()
        asyncio.run(_ingest(settings))
    elif args.command == "process":
        if settings.storage_account_url:
            from derivs_anomaly.pipeline.azure_lake import process_azure_lake

            result = process_azure_lake(settings)
        else:
            settings.ensure_directories()
            result = process_bronze(
                settings.data_dir,
                model_version=settings.model_version,
                threshold=settings.zscore_threshold,
            )
        print(json.dumps(result, indent=2))
    elif args.command == "api":
        if settings.api_key is None:
            raise SystemExit(
                "DERIVS_API_KEY must come from a secret manager before starting the API"
            )
        import uvicorn

        uvicorn.run(
            "derivs_anomaly.serving.api:app",
            host=args.host or settings.api_host,
            port=args.port or settings.api_port,
            proxy_headers=False,
        )
    elif args.command == "dashboard":
        dashboard_file = str(files("derivs_anomaly.dashboard").joinpath("app.py"))
        environment = os.environ.copy()
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                dashboard_file,
                "--server.port",
                str(args.port),
            ],
            env=environment,
            check=False,
        )
        raise SystemExit(result.returncode)
    elif args.command == "recover":
        if settings.eventhub_namespace and settings.eventhub_name:
            sink = EventHubSink(settings.eventhub_namespace, settings.eventhub_name)
        else:
            settings.ensure_directories()
            sink = LocalJsonlSink(str(settings.bronze_dir))
        journal_path = settings.data_dir / "gaps.jsonl"
        if settings.storage_account_url:
            from derivs_anomaly.pipeline.azure_lake import AzureLakeRunner

            runner = AzureLakeRunner(settings)
            try:
                runner._download_existing_gold(settings.gold_dir)
                journal_path = settings.gold_dir / "gaps.jsonl"
            finally:
                runner.close()
        result = asyncio.run(
            recover_open_gaps(
                journal_path,
                sink,
                symbol=settings.symbol,
            )
        )
        print(json.dumps(result, indent=2))
    elif args.command == "train":
        from derivs_anomaly.model.training import train_and_evaluate

        tracking_uri = args.tracking_uri or settings.mlflow_tracking_uri
        if not tracking_uri:
            raise SystemExit("Set DERIVS_MLFLOW_TRACKING_URI to the Azure ML workspace MLflow URI")
        report = train_and_evaluate(
            gold_path=args.gold or str(settings.gold_dir / "minutes.parquet"),
            reference_events_path=args.events,
            tracking_uri=tracking_uri,
        )
        print(json.dumps(report, indent=2))
    elif args.command == "quality":
        from derivs_anomaly.pipeline.completeness import calculate_previous_utc_day

        if settings.storage_account_url:
            from derivs_anomaly.pipeline.azure_lake import AzureLakeRunner

            runner = AzureLakeRunner(settings)
            try:
                runner._download_existing_gold(settings.gold_dir)
                result = calculate_previous_utc_day(settings.gold_dir / "minutes.parquet")
            finally:
                runner.close()
        else:
            result = calculate_previous_utc_day(settings.gold_dir / "minutes.parquet")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
