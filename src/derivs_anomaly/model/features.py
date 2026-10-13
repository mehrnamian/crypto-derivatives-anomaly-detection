from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd

FEATURE_COLUMNS = ("funding_change", "open_interest_change", "liquidation_notional")


def build_features(gold: pd.DataFrame) -> pd.DataFrame:
    """Build causal minute features using only current and prior observations."""
    required = {
        "minute",
        "symbol",
        "estimated_funding",
        "open_interest_change",
        "liquidation_notional",
    }
    missing = required.difference(gold.columns)
    if missing:
        raise ValueError(f"gold input is missing fields: {', '.join(sorted(missing))}")
    if gold.empty:
        return pd.DataFrame(columns=["minute", "symbol", *FEATURE_COLUMNS])

    frame = gold.copy()
    frame["minute"] = pd.to_datetime(frame["minute"], utc=True, errors="raise")
    frame = frame.sort_values(["symbol", "minute"], kind="stable").reset_index(drop=True)
    grouped = frame.groupby("symbol", sort=False)
    frame["funding_change"] = grouped["estimated_funding"].diff()
    frame["open_interest_change"] = pd.to_numeric(frame["open_interest_change"], errors="coerce")
    frame["liquidation_notional"] = pd.to_numeric(
        frame["liquidation_notional"], errors="coerce"
    ).fillna(0.0)
    for column in FEATURE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame[f"{column}_median"] = grouped[column].transform(
            lambda values: values.shift(1).rolling(window=1440, min_periods=60).median()
        )
        frame[f"{column}_mad"] = grouped[column].transform(
            lambda values: values.shift(1).rolling(window=1440, min_periods=60).apply(
                _median_absolute_deviation, raw=True
            )
        )
        scale = frame[f"{column}_mad"] * 1.4826
        frame[f"{column}_robust_z"] = (frame[column] - frame[f"{column}_median"]) / scale.replace(
            0.0, np.nan
        )
        frame[f"{column}_robust_z"] = frame[f"{column}_robust_z"].replace([np.inf, -np.inf], np.nan)
    return frame


def _median_absolute_deviation(values: np.ndarray) -> float:
    values = values[np.isfinite(values)]
    if values.size == 0:
        return float("nan")
    median = float(np.median(values))
    return float(np.median(np.abs(values - median)))


def feature_matrix(frame: pd.DataFrame, columns: Sequence[str] = FEATURE_COLUMNS) -> pd.DataFrame:
    """Return the model matrix without filling missing observations from the future."""
    missing = set(columns).difference(frame.columns)
    if missing:
        raise ValueError(f"feature frame is missing columns: {', '.join(sorted(missing))}")
    return frame.loc[:, list(columns)].replace([np.inf, -np.inf], np.nan)
