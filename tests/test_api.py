from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
from fastapi.testclient import TestClient
from pydantic import SecretStr

from derivs_anomaly.config import Settings
from derivs_anomaly.serving.api import create_app


def test_api_rejects_missing_key_and_serves_recent_data(tmp_path) -> None:
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    minute = datetime.now(UTC).replace(second=0, microsecond=0)
    pd.DataFrame([{
        "minute": minute,
        "symbol": "BTCUSDT",
        "mark_price": 62000.0,
        "estimated_funding": 0.0001,
        "open_interest": 1000.0,
        "open_interest_change": 1.0,
        "liquidation_count": 0,
        "liquidation_notional": 0.0,
        "coverage": 1.0,
    }]).to_parquet(gold_dir / "minutes.parquet", index=False)
    pd.DataFrame([{
        "minute": minute,
        "symbol": "BTCUSDT",
        "score": 7.0,
        "is_alert": True,
        "model_version": "test-v1",
    }]).to_parquet(gold_dir / "anomaly_scores.parquet", index=False)

    api_key = "test-secret-for-unit-tests-only-32chars"
    settings = Settings(data_dir=tmp_path, api_key=SecretStr(api_key))
    client = TestClient(create_app(settings))
    assert client.get("/health").status_code == 401
    assert client.get("/status").status_code == 401
    assert client.get("/anomalies").status_code == 401

    headers = {"X-API-Key": api_key}
    status = client.get("/status", headers=headers)
    assert status.status_code == 200
    assert status.json()["model_version"] == "test-v1"
    data = client.get("/anomalies", headers=headers)
    assert data.status_code == 200
    assert data.json()["count"] == 1
    assert data.json()["items"][0]["mark_price"] == 62000.0


def test_api_fails_closed_without_configured_key() -> None:
    settings = Settings(api_key=None)
    client = TestClient(create_app(settings))
    assert client.get("/health").status_code == 503
