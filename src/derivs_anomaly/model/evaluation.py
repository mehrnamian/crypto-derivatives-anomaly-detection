from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from derivs_anomaly.domain.alerts import merge_nearby_alerts
from derivs_anomaly.domain.records import AnomalyScore
from derivs_anomaly.model.features import feature_matrix


def event_recall(
    scores: pd.DataFrame,
    events: pd.DataFrame,
    *,
    tolerance_minutes: int = 15,
) -> float:
    if events.empty:
        raise ValueError("reference event set is empty")
    alert_times = pd.to_datetime(
        scores.loc[scores["is_alert"], "minute"], utc=True, errors="raise"
    )
    starts = pd.to_datetime(events["start_utc"], utc=True, errors="raise")
    ends = pd.to_datetime(events["end_utc"], utc=True, errors="raise")
    if (ends < starts).any():
        raise ValueError("reference event end_utc must not precede start_utc")
    tolerance = pd.Timedelta(minutes=tolerance_minutes)
    detected = [
        bool(((alert_times >= start - tolerance) & (alert_times <= end + tolerance)).any())
        for start, end in zip(starts, ends, strict=True)
    ]
    return float(np.mean(detected))


def alerts_per_event_free_day(scores: pd.DataFrame, events: pd.DataFrame) -> float:
    if scores.empty:
        return 0.0
    times = pd.to_datetime(scores["minute"], utc=True, errors="raise")
    if events.empty:
        event_starts = pd.Series([], dtype="datetime64[ns, UTC]")
        event_ends = event_starts
    else:
        event_starts = pd.to_datetime(events["start_utc"], utc=True, errors="raise")
        event_ends = pd.to_datetime(events["end_utc"], utc=True, errors="raise")
    mask = pd.Series(True, index=scores.index)
    for start, end in zip(event_starts, event_ends, strict=True):
        within_event = (times >= start - pd.Timedelta(minutes=15)) & (
            times <= end + pd.Timedelta(minutes=15)
        )
        mask &= ~within_event
    alerts = scores.loc[mask & scores["is_alert"]].copy()
    if alerts.empty:
        return 0.0
    alert_records = [
        AnomalyScore(
            minute=row.minute.to_pydatetime(),
            score=float(row.score),
            is_alert=True,
            model_version=str(row.model_version),
            features={},
        )
        for row in alerts.itertuples(index=False)
    ]
    merged = merge_nearby_alerts(alert_records)
    event_free_minutes = int(mask.sum())
    return len(merged) / max(event_free_minutes / 1440.0, 1 / 1440.0)


def chronological_split(
    frame: pd.DataFrame,
    *,
    train_fraction: float = 0.7,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between zero and one")
    ordered = frame.sort_values("minute", kind="stable").reset_index(drop=True)
    split_at = int(len(ordered) * train_fraction)
    if split_at < 1 or split_at >= len(ordered):
        raise ValueError("not enough rows for a chronological train/test split")
    return ordered.iloc[:split_at].copy(), ordered.iloc[split_at:].copy()


def fit_isolation_forest(
    train_features: pd.DataFrame,
    *,
    random_state: int = 41,
) -> IsolationForest:
    matrix = feature_matrix(train_features).dropna()
    if len(matrix) < 10:
        raise ValueError("at least 10 complete training rows are required")
    model = IsolationForest(
        n_estimators=200,
        contamination="auto",
        random_state=random_state,
        n_jobs=1,
    )
    model.fit(matrix)
    return model


def isolation_forest_scores(model: IsolationForest, features: pd.DataFrame) -> pd.DataFrame:
    matrix = feature_matrix(features)
    complete = matrix.notna().all(axis=1)
    score = pd.Series(0.0, index=features.index)
    score.loc[complete] = -model.decision_function(matrix.loc[complete])
    result = features.loc[:, ["minute", "symbol"]].copy()
    result["score"] = score
    result["is_alert"] = False
    result["model_version"] = "isolation-forest-v1"
    return result


def tune_robust_threshold(
    scores: pd.DataFrame,
    events: pd.DataFrame,
    *,
    candidates: Iterable[float] = (2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0),
    max_alerts_per_day: float = 5.0,
) -> dict[str, float]:
    best: dict[str, float] | None = None
    best_within_cap: dict[str, float] | None = None
    for threshold in sorted(set(float(item) for item in candidates)):
        trial = scores.copy()
        trial["is_alert"] = trial["score"] >= threshold
        recall = event_recall(trial, events)
        alerts_per_day = alerts_per_event_free_day(trial, events)
        result = {
            "threshold": threshold,
            "event_recall": recall,
            "alerts_per_day": alerts_per_day,
        }
        if best is None or (recall, -alerts_per_day, -threshold) > (
            best["event_recall"], -best["alerts_per_day"], -best["threshold"]
        ):
            best = result
        if alerts_per_day <= max_alerts_per_day and (
            best_within_cap is None
            or (recall, -alerts_per_day, -threshold) > (
                best_within_cap["event_recall"],
                -best_within_cap["alerts_per_day"],
                -best_within_cap["threshold"],
            )
        ):
            best_within_cap = result
    if best_within_cap is not None:
        return best_within_cap
    if best is None:
        raise ValueError("no candidate thresholds were supplied")
    return best
