from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import numpy as np
import pandas as pd

from derivs_anomaly.model import training
from derivs_anomaly.model.training import train_and_evaluate


def test_training_uses_chronological_events_and_logs_registered_model(
    tmp_path,
    monkeypatch,
) -> None:
    count = 2600
    minutes = pd.date_range("2025-01-01T00:00:00Z", periods=count, freq="min")
    funding = 0.0001 + np.sin(np.arange(count) / 7) * 0.00001
    event_indices = [700 + 180 * index for index in range(10)]
    for event_index in event_indices:
        funding[event_index:] += 0.0003

    gold_path = tmp_path / "gold.parquet"
    pd.DataFrame({
        "minute": minutes,
        "symbol": "BTCUSDT",
        "estimated_funding": funding,
        "open_interest_change": np.sin(np.arange(count) / 5) * 2,
        "liquidation_notional": np.abs(np.sin(np.arange(count) / 3)) * 1000,
    }).to_parquet(gold_path, index=False)
    events_path = tmp_path / "events.csv"
    pd.DataFrame([
        {
            "start_utc": minutes[index],
            "end_utc": minutes[index + 2],
            "event_type": "funding_spike",
            "source_url": f"https://example.org/event/{index}",
        }
        for index in event_indices
    ]).to_csv(events_path, index=False)

    calls: list[tuple[str, object]] = []

    @contextmanager
    def fake_run(*, run_name: str):
        calls.append(("start_run", run_name))
        yield SimpleNamespace(info=SimpleNamespace(run_id="synthetic-run-id"))

    monkeypatch.setattr(
        training.mlflow,
        "set_tracking_uri",
        lambda uri: calls.append(("uri", uri)),
    )
    monkeypatch.setattr(
        training.mlflow,
        "set_experiment",
        lambda name: calls.append(("experiment", name)),
    )
    monkeypatch.setattr(training.mlflow, "start_run", fake_run)
    monkeypatch.setattr(
        training.mlflow,
        "log_params",
        lambda params: calls.append(("params", params)),
    )
    monkeypatch.setattr(
        training.mlflow,
        "log_metrics",
        lambda metrics: calls.append(("metrics", metrics)),
    )
    monkeypatch.setattr(
        training.mlflow,
        "log_artifact",
        lambda path, artifact_path: calls.append(("artifact", artifact_path)),
    )
    monkeypatch.setattr(
        training.mlflow.pyfunc,
        "log_model",
        lambda **kwargs: calls.append(("pyfunc_model", kwargs)),
    )
    monkeypatch.setattr(
        training.mlflow.sklearn,
        "log_model",
        lambda **kwargs: calls.append(("sklearn_model", kwargs)),
    )

    report = train_and_evaluate(
        gold_path=gold_path,
        reference_events_path=events_path,
        tracking_uri="azureml://synthetic-test-workspace",
        registered_model_name="local-smoke-anomaly-model",
    )

    assert report["tuning_event_count"] == 7
    assert report["final_event_count"] == 3
    assert report["final_split_start_utc"] > str(minutes[event_indices[6]])
    assert report["mlflow_run_id"] == "synthetic-run-id"
    assert report["selected_model"] in {"robust_zscore", "isolation_forest"}
    assert any(name in {"pyfunc_model", "sklearn_model"} for name, _ in calls)
    metrics = next(value for name, value in calls if name == "metrics")
    assert metrics["acceptance_pass"] in {0.0, 1.0}
