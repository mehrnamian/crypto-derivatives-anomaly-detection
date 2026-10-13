from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import structlog

from derivs_anomaly.domain.envelope import EventEnvelope
from derivs_anomaly.model.features import build_features
from derivs_anomaly.model.scoring import score_features
from derivs_anomaly.pipeline.lake import (
    atomic_write_jsonl,
    atomic_write_parquet,
    discover_bronze_files,
    iter_bronze_records,
    safe_stem,
)
from derivs_anomaly.pipeline.quality import normalize_envelope

LOGGER = structlog.get_logger(__name__)


def _file_token(path: Path, root: Path) -> str:
    relative = path.relative_to(root).as_posix()
    digest = hashlib.sha256(relative.encode("utf-8")).hexdigest()[:12]
    return f"{safe_stem(path)}-{digest}"


def _quarantine_item(
    reason: str,
    stream: str,
    raw: Any,
    *,
    source_file: str,
    line: int,
) -> dict[str, Any]:
    return {
        "quarantined_at": datetime.now(UTC).isoformat(),
        "reason": reason[:500],
        "stream": stream or "unknown",
        "source_file": source_file,
        "source_line": line,
        "raw": raw,
    }


def _aggregate_gold(
    silver_files: list[Path],
    gold_dir: Path,
    *,
    recompute_from: datetime | None = None,
) -> pd.DataFrame:
    if not silver_files:
        return pd.DataFrame()
    connection = duckdb.connect(database=":memory:")
    try:
        connection.read_parquet([str(path) for path in silver_files]).create_view("silver_events")
        rollup_sql = """
            SELECT
                time_bucket(INTERVAL '1 minute', event_time) AS minute,
                symbol,
                arg_max(mark_price, event_time) FILTER (WHERE mark_price IS NOT NULL) AS mark_price,
                arg_max(estimated_funding, event_time)
                    FILTER (WHERE estimated_funding IS NOT NULL) AS estimated_funding,
                arg_max(open_interest, event_time)
                    FILTER (WHERE open_interest IS NOT NULL) AS open_interest,
                count(*) FILTER (WHERE stream = 'liquidation') AS liquidation_count,
                coalesce(sum(liquidation_notional)
                    FILTER (WHERE stream = 'liquidation'), 0.0) AS liquidation_notional,
                count(DISTINCT stream) AS streams_present
            FROM silver_events
            {where_clause}
            GROUP BY minute, symbol
            """
        if recompute_from is None:
            connection.execute(
                "CREATE TEMP TABLE minute_rollup AS "
                + rollup_sql.format(where_clause="")
            )
        else:
            query_start = pd.Timestamp(recompute_from - pd.Timedelta(minutes=1))
            connection.execute(
                "CREATE TEMP TABLE minute_rollup AS "
                + rollup_sql.format(where_clause="WHERE event_time >= ?"),
                [query_start.to_pydatetime()],
            )
        frame = connection.execute(
            """
            WITH bounds AS (
                SELECT symbol, min(minute) AS first_minute, max(minute) AS last_minute
                FROM minute_rollup GROUP BY symbol
            ),
            buckets AS (
                SELECT b.symbol, series.minute
                FROM bounds b,
                     LATERAL generate_series(b.first_minute, b.last_minute, INTERVAL 1 MINUTE)
                        AS series(minute)
            ),
            joined AS (
                SELECT
                    b.minute,
                    b.symbol,
                    r.mark_price,
                    r.estimated_funding,
                    r.open_interest,
                    coalesce(r.liquidation_count, 0) AS liquidation_count,
                    coalesce(r.liquidation_notional, 0.0) AS liquidation_notional,
                    coalesce(r.streams_present, 0)::DOUBLE / 3.0 AS coverage
                FROM buckets b
                LEFT JOIN minute_rollup r USING (minute, symbol)
            ),
            changes AS (
                SELECT *,
                    open_interest - lag(open_interest) OVER (PARTITION BY symbol ORDER BY minute)
                        AS open_interest_change
                FROM joined
            )
            SELECT * FROM changes ORDER BY minute, symbol
            """
        ).df()
    finally:
        connection.close()

    if frame.empty:
        return frame
    frame["minute"] = pd.to_datetime(frame["minute"], utc=True)
    if recompute_from is not None:
        frame = frame.loc[frame["minute"] >= pd.Timestamp(recompute_from)].copy()
    return frame


def process_bronze(
    data_dir: Path,
    *,
    model_version: str = "robust-zscore-v1",
    threshold: float = 6.0,
) -> dict[str, int | float]:
    """Validate bronze, deterministically rebuild silver/gold, and score minute data."""
    data_dir = Path(data_dir)
    bronze_dir = data_dir / "bronze"
    silver_dir = data_dir / "silver"
    gold_dir = data_dir / "gold"
    quarantine_dir = data_dir / "quarantine"
    silver_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    files = discover_bronze_files(bronze_dir)
    source_by_name = {path.relative_to(bronze_dir).as_posix(): path for path in files}
    rejected_by_file: dict[Path, list[dict[str, Any]]] = defaultdict(list)
    accepted: list[dict[str, Any]] = []
    gaps: dict[str, dict[str, Any]] = {}
    gap_controls: list[dict[str, Any]] = []
    invalid_count = 0

    for path in files:
        relative_name = path.relative_to(bronze_dir).as_posix()
        try:
            for line_number, raw in iter_bronze_records(path):
                stream = str(raw.get("stream", "")) if isinstance(raw, dict) else ""
                try:
                    if not isinstance(raw, dict):
                        raise ValueError("record must be a JSON object")
                    envelope = EventEnvelope.from_dict(raw)
                    if envelope.stream == "gap":
                        payload = envelope.payload
                        started_at = str(payload["started_at"])
                        state = str(payload["state"])
                        control = {
                            "started_at": started_at,
                            "ended_at": payload.get("ended_at"),
                            "recorded_at": envelope.received_time.isoformat(),
                            "state": state,
                        }
                        gap_controls.append(control)
                        gaps[started_at] = control
                        continue
                    row = normalize_envelope(envelope)
                    row["source_file"] = relative_name
                    row["source_line"] = line_number
                    accepted.append(row)
                except (ValueError, TypeError, KeyError) as exc:
                    invalid_count += 1
                    rejected_by_file[path].append(
                        _quarantine_item(
                            str(exc),
                            stream,
                            raw,
                            source_file=relative_name,
                            line=line_number,
                        )
                    )
        except Exception as exc:
            invalid_count += 1
            rejected_by_file[path].append(
                _quarantine_item(
                    f"cannot read bronze file: {type(exc).__name__}",
                    "unknown",
                    {"file_error": type(exc).__name__},
                    source_file=relative_name,
                    line=0,
                )
            )

    # Stable winner selection means input traversal order cannot change survivors.
    accepted.sort(
        key=lambda row: (
            row["event_time"],
            row["received_time"],
            row["source_file"],
            row["source_line"],
        )
    )
    unique_by_key: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    for row in accepted:
        if row["event_key"] in unique_by_key:
            duplicate_count += 1
            path = source_by_name[row["source_file"]]
            rejected_by_file[path].append(
                _quarantine_item(
                    "duplicate event_key",
                    row["stream"],
                    {"event_key": row["event_key"]},
                    source_file=row["source_file"],
                    line=row["source_line"],
                )
            )
        else:
            unique_by_key[row["event_key"]] = row

    # Derive output file names from bronze source paths: retries overwrite exact outputs.
    by_target: dict[Path, list[dict[str, Any]]] = defaultdict(list)
    for row in unique_by_key.values():
        source_path = source_by_name[row["source_file"]]
        day = row["event_time"].strftime("%Y-%m-%d")
        target = (
            silver_dir
            / f"date={day}"
            / f"stream={row['stream']}"
            / f"{_file_token(source_path, bronze_dir)}.parquet"
        )
        by_target[target].append(row)

    for target, rows in by_target.items():
        atomic_write_parquet(target, pd.DataFrame(rows))
    targets = set(by_target)
    silver_files = sorted(targets)
    for old_file in silver_dir.rglob("*.parquet"):
        if old_file not in targets:
            old_file.unlink(missing_ok=True)
    directories = (path for path in silver_dir.rglob("*") if path.is_dir())
    for directory in sorted(directories, reverse=True):
        if not any(directory.iterdir()):
            directory.rmdir()

    active_quarantine_targets: set[Path] = set()
    for path in files:
        target = quarantine_dir / f"{_file_token(path, bronze_dir)}.jsonl"
        active_quarantine_targets.add(target)
        atomic_write_jsonl(target, rejected_by_file[path])
    for old_file in quarantine_dir.glob("*.jsonl"):
        if old_file not in active_quarantine_targets:
            old_file.unlink(missing_ok=True)

    gap_rows = sorted(gap_controls, key=lambda item: item["recorded_at"])
    atomic_write_jsonl(gold_dir / "gaps.jsonl", gap_rows)

    previous_gold_path = gold_dir / "minutes.parquet"
    previous_scores_path = gold_dir / "anomaly_scores.parquet"
    previous_scores = (
        pd.read_parquet(previous_scores_path)
        if previous_scores_path.is_file()
        else pd.DataFrame()
    )
    if not previous_scores.empty:
        previous_scores["minute"] = pd.to_datetime(previous_scores["minute"], utc=True)
    previous_gold = (
        pd.read_parquet(previous_gold_path)
        if previous_gold_path.is_file()
        else pd.DataFrame()
    )
    if not previous_gold.empty:
        previous_gold["minute"] = pd.to_datetime(previous_gold["minute"], utc=True)

    recompute_from = datetime.now(UTC) - pd.Timedelta(hours=2)
    recomputed_gold = _aggregate_gold(
        silver_files,
        gold_dir,
        recompute_from=recompute_from if not previous_gold.empty else None,
    )
    if recomputed_gold.empty:
        gold = previous_gold
    elif previous_gold.empty:
        gold = recomputed_gold
    else:
        cutoff = pd.Timestamp(recompute_from)
        preserved = previous_gold.loc[previous_gold["minute"] < cutoff]
        recent = recomputed_gold.loc[recomputed_gold["minute"] >= cutoff]
        gold = pd.concat([preserved, recent], ignore_index=True)
        gold = gold.drop_duplicates(["minute", "symbol"], keep="last")
        gold = gold.sort_values(["symbol", "minute"], kind="stable").reset_index(drop=True)

    if gold.empty:
        (gold_dir / "minutes.parquet").unlink(missing_ok=True)
        (gold_dir / "anomaly_scores.parquet").unlink(missing_ok=True)
        scores = pd.DataFrame()
    else:
        atomic_write_parquet(gold_dir / "minutes.parquet", gold)
        scoring_cutoff = gold["minute"].max() - pd.Timedelta(hours=2)
        feature_warmup_start = scoring_cutoff - pd.Timedelta(minutes=1440)
        recent_features_input = gold.loc[gold["minute"] >= feature_warmup_start]
        features = build_features(recent_features_input)
        recent_scores = score_features(
            features.loc[features["minute"] >= scoring_cutoff],
            threshold=threshold,
            model_version=model_version,
        )
        if previous_scores.empty:
            scores = recent_scores
        else:
            preserved_scores = previous_scores.loc[previous_scores["minute"] < scoring_cutoff]
            scores = pd.concat([preserved_scores, recent_scores], ignore_index=True)
            scores = scores.drop_duplicates(["minute", "symbol"], keep="last")
            scores = scores.sort_values(["symbol", "minute"], kind="stable")
        atomic_write_parquet(gold_dir / "anomaly_scores.parquet", scores)

    now = datetime.now(UTC)
    previous_day_start = pd.Timestamp(now.date(), tz="UTC") - pd.Timedelta(days=1)
    current_day_start = previous_day_start + pd.Timedelta(days=1)
    previous_day = (
        gold.loc[
            (gold["minute"] >= previous_day_start)
            & (gold["minute"] < current_day_start)
        ]
        if not gold.empty
        else pd.DataFrame()
    )
    completeness = (
        float(previous_day["coverage"].gt(0).sum() / 1440 * 100)
        if not previous_day.empty
        else 0.0
    )
    result: dict[str, int | float] = {
        "bronze_files": len(files),
        "silver_rows": len(unique_by_key),
        "duplicate_rows": duplicate_count,
        "quarantined_rows": invalid_count + duplicate_count,
        "gold_minutes": len(gold),
        "anomaly_scores": len(scores),
        "previous_utc_day_completeness_percent": completeness,
    }
    LOGGER.info("pipeline_complete", **result)
    return result
