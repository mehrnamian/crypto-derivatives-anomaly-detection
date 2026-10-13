from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd

from derivs_anomaly.model.features import FEATURE_COLUMNS

ROBUST_Z_COLUMNS = tuple(f"{column}_robust_z" for column in FEATURE_COLUMNS)


def score_features(
    features: pd.DataFrame,
    *,
    threshold: float = 6.0,
    model_version: str = "robust-zscore-v1",
    alert_merge_minutes: int = 15,
    max_alerts_per_day: int = 5,
) -> pd.DataFrame:
    """Score each minute from past-only robust z-scores; never emits trade advice."""
    if threshold <= 0 or not np.isfinite(threshold):
        raise ValueError("threshold must be a finite positive number")
    missing = {"minute", "symbol", *ROBUST_Z_COLUMNS}.difference(features.columns)
    if missing:
        raise ValueError(f"feature frame is missing score fields: {', '.join(sorted(missing))}")
    robust = features.loc[:, ROBUST_Z_COLUMNS].abs()
    scores = robust.max(axis=1, skipna=True).fillna(0.0).astype(float)
    scores = scores.clip(upper=1_000_000.0)
    result = features.loc[:, ["minute", "symbol"]].copy()
    result["score"] = scores
    result["is_alert"] = scores >= threshold
    result["model_version"] = model_version
    if alert_merge_minutes < 0 or max_alerts_per_day < 1:
        raise ValueError("alert policy limits must be non-negative and max alerts must be positive")
    result["is_alert"] = _apply_alert_policy(
        result,
        merge_window=timedelta(minutes=alert_merge_minutes),
        max_per_day=max_alerts_per_day,
    )
    return result


def _apply_alert_policy(
    scores: pd.DataFrame,
    *,
    merge_window: timedelta,
    max_per_day: int,
) -> pd.Series:
    alerts = pd.Series(False, index=scores.index, dtype=bool)
    candidates = scores.loc[scores["is_alert"]].copy()
    if candidates.empty:
        return alerts
    candidates["minute"] = pd.to_datetime(candidates["minute"], utc=True, errors="raise")
    candidates["day"] = candidates["minute"].dt.strftime("%Y-%m-%d")
    for (_symbol, _day), daily in candidates.groupby(["symbol", "day"], sort=True):
        daily = daily.sort_values("minute", kind="stable")
        groups: list[list[int]] = []
        for index, row in daily.iterrows():
            if not groups:
                groups.append([index])
                continue
            previous_time = daily.loc[groups[-1][-1], "minute"]
            if row["minute"] - previous_time <= merge_window:
                groups[-1].append(index)
            else:
                groups.append([index])
        leaders = [
            max(group, key=lambda index: float(scores.at[index, "score"]))
            for group in groups
        ]
        leaders = sorted(
            leaders,
            key=lambda index: float(scores.at[index, "score"]),
            reverse=True,
        )[:max_per_day]
        alerts.loc[leaders] = True
    return alerts
