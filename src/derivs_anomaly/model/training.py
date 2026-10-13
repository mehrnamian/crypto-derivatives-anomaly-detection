from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import mlflow
import mlflow.pyfunc
import mlflow.sklearn
import pandas as pd

from derivs_anomaly.model.evaluation import (
    alerts_per_event_free_day,
    event_recall,
    fit_isolation_forest,
)
from derivs_anomaly.model.features import FEATURE_COLUMNS, build_features, feature_matrix
from derivs_anomaly.model.scoring import ROBUST_Z_COLUMNS, score_features


class RobustThresholdModel(mlflow.pyfunc.PythonModel):
    """Small serializable MLflow model wrapper for selected robust-z thresholds."""

    def __init__(self, threshold: float, model_version: str) -> None:
        self.threshold = threshold
        self.model_version = model_version

    def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> pd.DataFrame:
        robust = model_input.loc[:, ROBUST_Z_COLUMNS].abs()
        score = robust.max(axis=1, skipna=True).fillna(0.0)
        return pd.DataFrame(
            {
                "score": score,
                "is_alert": score >= self.threshold,
                "model_version": self.model_version,
            },
            index=model_input.index,
        )


def _reference_events(path: Path) -> pd.DataFrame:
    events = pd.read_csv(path)
    required = {"start_utc", "end_utc", "event_type", "source_url"}
    missing = required.difference(events.columns)
    if missing:
        raise ValueError(f"reference events file is missing columns: {', '.join(sorted(missing))}")
    if len(events) < 10:
        raise ValueError(f"at least 10 sourced reference events are required; found {len(events)}")
    events["start_utc"] = pd.to_datetime(events["start_utc"], utc=True, errors="raise")
    events["end_utc"] = pd.to_datetime(events["end_utc"], utc=True, errors="raise")
    if (events["end_utc"] < events["start_utc"]).any():
        raise ValueError("reference event end_utc must not precede start_utc")
    missing_sources = events["source_url"].isna() | events["source_url"].astype(
        str
    ).str.strip().eq("")
    if missing_sources.any():
        raise ValueError("every reference event must include its public source URL")
    return events.sort_values("start_utc", kind="stable").reset_index(drop=True)


def train_and_evaluate(
    *,
    gold_path: Path,
    reference_events_path: Path,
    tracking_uri: str,
    registered_model_name: str = "crypto-market-anomaly-detector",
) -> dict[str, Any]:
    if not tracking_uri:
        raise ValueError("MLflow tracking URI must point to the Azure ML workspace")
    gold = pd.read_parquet(gold_path)
    if gold.empty:
        raise ValueError("gold data is empty; replay historical data before evaluation")
    events = _reference_events(reference_events_path)
    features = build_features(gold)
    event_split_at = int(len(events) * 0.7)
    if event_split_at < 1 or event_split_at >= len(events):
        raise ValueError(
            "reference set must contain events on both sides of the chronological split"
        )
    ordered_events = events.iloc[:event_split_at].copy()
    final_events = events.iloc[event_split_at:].copy()
    final_start = final_events["start_utc"].min()
    tune_features = features.loc[features["minute"] < final_start].copy()
    final_features = features.loc[features["minute"] >= final_start].copy()
    if tune_features.empty or final_features.empty:
        raise ValueError(
            "gold replay must span both tuning and held-out event periods"
        )

    robust_tune = score_features(tune_features, threshold=1.0, model_version="robust-zscore-tune")
    robust_candidates = [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0]
    best_robust: dict[str, float] | None = None
    from derivs_anomaly.model.evaluation import tune_robust_threshold

    best_robust = tune_robust_threshold(
        robust_tune,
        ordered_events,
        candidates=robust_candidates,
        max_alerts_per_day=5.0,
    )
    robust_final = score_features(
        final_features,
        threshold=best_robust["threshold"],
        model_version="robust-zscore-v1",
    )

    training_rows = tune_features.loc[tune_features["minute"] < ordered_events["start_utc"].max()]
    forest = fit_isolation_forest(training_rows)
    forest_tune_matrix = feature_matrix(tune_features)
    forest_final_matrix = feature_matrix(final_features)
    forest_tune_complete = forest_tune_matrix.notna().all(axis=1)
    forest_final_complete = forest_final_matrix.notna().all(axis=1)
    forest_tune_raw = -forest.decision_function(forest_tune_matrix.loc[forest_tune_complete])
    forest_final_raw = -forest.decision_function(forest_final_matrix.loc[forest_final_complete])
    quantiles = pd.Series(forest_tune_raw).quantile(
        [0.90, 0.95, 0.97, 0.98, 0.99, 0.995]
    )
    forest_thresholds = sorted(set(float(value) for value in quantiles.tolist()))
    if not forest_thresholds:
        forest_thresholds = [float("inf")]
    best_forest: dict[str, float] | None = None
    for threshold in forest_thresholds:
        trial = pd.DataFrame({
            "minute": tune_features.loc[forest_tune_complete, "minute"].to_numpy(),
            "symbol": tune_features.loc[forest_tune_complete, "symbol"].to_numpy(),
            "score": forest_tune_raw,
            "is_alert": forest_tune_raw >= threshold,
            "model_version": "isolation-forest-v1",
        })
        recall = event_recall(trial, ordered_events)
        daily = alerts_per_event_free_day(trial, ordered_events)
        candidate = {"threshold": threshold, "event_recall": recall, "alerts_per_day": daily}
        if best_forest is None or (recall, -daily, -threshold) > (
            best_forest["event_recall"], -best_forest["alerts_per_day"], -best_forest["threshold"]
        ):
            best_forest = candidate
    assert best_forest is not None
    forest_scores = pd.DataFrame(0.0, index=final_features.index, columns=["score"])
    forest_scores.loc[forest_final_complete, "score"] = forest_final_raw
    forest_final = final_features.loc[:, ["minute", "symbol"]].copy()
    forest_final["score"] = forest_scores["score"]
    forest_final["is_alert"] = forest_final["score"] >= best_forest["threshold"]
    forest_final["model_version"] = "isolation-forest-v1"

    robust_metrics = {
        "recall": event_recall(robust_final, final_events),
        "alerts_per_day": alerts_per_event_free_day(robust_final, final_events),
    }
    forest_metrics = {
        "recall": event_recall(forest_final, final_events),
        "alerts_per_day": alerts_per_event_free_day(forest_final, final_events),
    }
    if (
        forest_metrics["recall"] >= 0.70
        and forest_metrics["alerts_per_day"] <= 5.0
        and (
            forest_metrics["recall"] > robust_metrics["recall"]
            or robust_metrics["alerts_per_day"] > 5.0
        )
    ):
        selected = "isolation_forest"
    else:
        selected = "robust_zscore"
    selected_version = (
        "isolation-forest-v1"
        if selected == "isolation_forest"
        else "robust-zscore-v1"
    )
    metrics: dict[str, Any] = {
        "selected_model": selected,
        "selected_model_version": selected_version,
        "robust_zscore": {
            "threshold": best_robust["threshold"],
            "tuning_recall": best_robust["event_recall"],
            "tuning_alerts_per_day": best_robust["alerts_per_day"],
            **robust_metrics,
        },
        "isolation_forest": {
            "threshold": best_forest["threshold"],
            "tuning_recall": best_forest["event_recall"],
            "tuning_alerts_per_day": best_forest["alerts_per_day"],
            **forest_metrics,
        },
        "final_event_count": len(final_events),
        "tuning_event_count": len(ordered_events),
        "final_split_start_utc": final_start.isoformat(),
        "acceptance_pass": (
            (forest_metrics if selected == "isolation_forest" else robust_metrics)["recall"] >= 0.70
            and (forest_metrics if selected == "isolation_forest" else robust_metrics)[
                "alerts_per_day"
            ]
            <= 5.0
        ),
    }
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("crypto-derivatives-anomaly-detection")
    with mlflow.start_run(run_name="chronological-model-comparison") as run:
        mlflow.log_params({
            "feature_columns": ",".join(FEATURE_COLUMNS),
            "train_fraction_events": 0.7,
            "final_event_count": len(final_events),
            "tuning_event_count": len(ordered_events),
            "random_split": False,
            "alert_merge_minutes": 15,
            "daily_alert_cap": 5,
        })
        mlflow.log_metrics({
            "robust_recall": robust_metrics["recall"],
            "robust_alerts_per_day": robust_metrics["alerts_per_day"],
            "robust_threshold": best_robust["threshold"],
            "isolation_forest_recall": forest_metrics["recall"],
            "isolation_forest_alerts_per_day": forest_metrics["alerts_per_day"],
            "isolation_forest_threshold": best_forest["threshold"],
            "acceptance_pass": float(metrics["acceptance_pass"]),
        })
        with tempfile.TemporaryDirectory(prefix="derivs-model-eval-") as temp_dir:
            report_path = Path(temp_dir) / "evaluation.json"
            report_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
            mlflow.log_artifact(str(report_path), artifact_path="evaluation")
            if selected == "robust_zscore":
                mlflow.pyfunc.log_model(
                    name="selected_model",
                    python_model=RobustThresholdModel(best_robust["threshold"], selected_version),
                    registered_model_name=registered_model_name,
                    input_example=tune_features.loc[:, ROBUST_Z_COLUMNS].head(5),
                )
            else:
                mlflow.sklearn.log_model(
                    sk_model=forest,
                    name="selected_model",
                    registered_model_name=registered_model_name,
                    input_example=training_rows.loc[:, list(FEATURE_COLUMNS)].dropna().head(5),
                )
        metrics["mlflow_run_id"] = run.info.run_id
    return metrics
