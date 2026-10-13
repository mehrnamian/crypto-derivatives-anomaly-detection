from __future__ import annotations

import os

import httpx
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Perpetual Market Anomalies", page_icon="📊", layout="wide")
st.title("BTCUSDT Perpetual · Market Data Anomalies")
st.caption(
    "Descriptive data-quality/anomaly monitoring only — not trading signals or recommendations."
)

api_url = os.environ.get("DERIVS_API_URL", "http://127.0.0.1:8000").rstrip("/")
api_key = os.environ.get("DERIVS_API_KEY")
if not api_key:
    try:
        api_key = st.secrets["DERIVS_API_KEY"]
    except (KeyError, FileNotFoundError):
        api_key = None

if not api_key:
    st.error("Configure DERIVS_API_KEY through the dashboard's secret store before connecting.")
    st.stop()

headers = {"X-API-Key": api_key}
try:
    response = httpx.get(f"{api_url}/status", headers=headers, timeout=5.0)
    response.raise_for_status()
    status = response.json()
    response = httpx.get(f"{api_url}/anomalies", headers=headers, timeout=10.0)
    response.raise_for_status()
    rows = response.json().get("items", [])
except httpx.HTTPStatusError as exc:
    detail = exc.response.text.strip()
    if len(detail) > 500:
        detail = f"{detail[:500]}…"
    message = (
        f"GET {exc.request.url} returned HTTP {exc.response.status_code}."
        f" {detail}"
    )
    st.error(f"API request failed: {message}")
    st.stop()
except httpx.HTTPError as exc:
    request = getattr(exc, "request", None)
    url = request.url if request is not None else api_url
    st.error(f"API connection failed at {url}: {exc.__class__.__name__}")
    st.stop()

freshness = status.get("freshness_seconds")
freshness_minutes = freshness / 60 if freshness is not None else None
health, quality, reconnect, version = st.columns(4)
health.metric(
    "Latest data age",
    f"{freshness_minutes:.1f} min" if freshness_minutes is not None else "No data",
)
quality.metric(
    "24h minute coverage",
    (
        f"{100 * status['completeness_24h']:.2f}%"
        if status.get("completeness_24h") is not None
        else "N/A"
    ),
)
reconnect.metric("Latest reconnect", status.get("latest_reconnect") or "None recorded")
version.metric("Model version", status.get("model_version") or "Unknown")
if freshness_minutes is None or freshness_minutes > 15:
    st.error("Data freshness exceeds the 15-minute alert threshold.")

if not rows:
    st.info(
        "No gold minute records are available yet. Start ingestion and the "
        "five-minute processing job."
    )
    st.stop()

frame = pd.DataFrame(rows)
frame["minute"] = pd.to_datetime(frame["minute"], utc=True, errors="coerce")
frame = frame.dropna(subset=["minute"]).sort_values("minute")
for title, columns in (
    ("Mark price", ["mark_price"]),
    ("Estimated funding", ["estimated_funding"]),
    ("Open interest", ["open_interest"]),
    ("Liquidations", ["liquidation_notional"]),
):
    if columns[0] not in frame:
        continue
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=frame["minute"], y=frame[columns[0]], mode="lines", name=title)
    )
    alert_rows = (
        frame.loc[frame["is_alert"].astype(bool)]
        if "is_alert" in frame
        else frame.iloc[0:0]
    )
    if not alert_rows.empty:
        y_column = columns[0]
        fig.add_trace(
            go.Scatter(
                x=alert_rows["minute"],
                y=alert_rows[y_column],
                mode="markers",
                name="Data anomaly",
                marker={"color": "#ef4444", "size": 9, "symbol": "x"},
            )
        )
    fig.update_layout(title=title, height=300, margin={"l": 20, "r": 20, "t": 45, "b": 20})
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Recent anomaly-score minutes")
alerts = (
    frame.loc[frame["is_alert"].astype(bool)]
    if "is_alert" in frame
    else frame.iloc[0:0]
)
st.dataframe(alerts.tail(100), use_container_width=True, hide_index=True)
