# Crypto Derivatives Anomaly Detection

Local-first implementation and dev-only deployment preparation for detecting anomalies in BTCUSDT perpetual-market data. Outputs describe data anomalies only; they are not trading signals or recommendations.

## Current state

Implemented locally:

- Binance WebSocket/REST ingestion, event envelopes, resilient reconnects, gap journal/backfill, and Entra ID Event Hubs sender.
- Bronze-to-silver-to-gold processing with strict schemas, quarantine, idempotency/deduplication, DuckDB aggregates and shared causal features.
- Robust z-score scoring, Isolation Forest evaluation, chronological MLflow evaluation/registration path.
- API-key-protected FastAPI, 24-hour ADLS cache, and Streamlit dashboard.
- Dev-scoped Bicep, manual owner-run dev deployment scripts, and GitHub CI.

See [docs/dev-test-readiness.md](docs/dev-test-readiness.md) for the staged dev handoff and blockers; see [docs/implementation-status.md](docs/implementation-status.md) for the guide comparison. The reference events file remains intentionally empty until sourced event annotations are verified.

## Local checks

Use Python 3.11 or later. Install the project and development extras, then run `pytest -q` and `ruff check .`. The CI workflow also builds the dev Bicep and Docker image. Do not commit `.env`, credentials, generated lake data, model artifacts, or test data containing secrets.

## Upcoming improvements

The following affordable, code-focused improvements are candidates for future updates; they are not deployment or model-validation commitments:

- Split CLI command dispatch into small command handlers to improve readability and unit testing.
- Separate dashboard API access, data preparation, and rendering; cover empty and malformed API responses with focused tests.
- Replace mutation of derived settings paths during initialization with clearer computed properties.
- Make probe failures easier to diagnose while keeping error handling explicit.
- Reconcile test totals and validation status across the project documentation, using one recorded test run as the source of truth.

## Azure deployment

To be done...
