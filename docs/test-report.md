# Test report — dev readiness

No Azure environment has been deployed or tested from this workspace. This report records local automated checks only; Azure acceptance rows are intentionally pending rather than fabricated.

| Test | Date (UTC) | Measurement | Result | Evidence / next step |
| --- | --- | --- | --- | --- |
| Local unit/integration suite | 2026-10-14 | 26 passed, 0 failed | Pass (local only) | Python 3.12.3; `pytest -q`. Includes REST mark/funding fallback and skewed exchange-clock timestamp handling. MLflow calls are mocked in the fast test; no Azure ML workspace was contacted. This does not include Azure acceptance. |
| Ruff | 2026-10-14 | All checks passed | Pass (local only) | `ruff check .`; Bicep not compiled locally because Azure CLI is unavailable. |
| Deterministic replay | 2026-10-13 | Fixture replay twice; gold and score DataFrames equal | Pass (local fixture only) | `test_process_is_idempotent_deduplicates_and_quarantines`; full recorded-market replay remains pending. |
| Silver idempotency | 2026-10-13 | Silver row count 1 on both fixture runs; duplicate count 1 | Pass (local fixture only) | Integration test reruns same bronze fixture; live bronze capture remains pending. |
| WebSocket disconnect/recovery | — | — | Pending Azure | Measure reconnect ≤60s, recoverable gap fill ≤15m; document liquidation history limitation. |
| Pipeline interruption | — | — | Pending Azure | Stop processor for two hours, resume, verify freshness and key uniqueness. |
| Silent failure / alerts | — | — | Pending Azure | Scale ingestion to zero; verify freshness and input-volume notifications and email receipt. |
| Bad data | 2026-10-13 | Duplicate, invalid price, and future record quarantined; unique valid event retained | Pass (local fixture only) | Local tests; Azure quarantine metric/alert remains pending. |
| Load test | — | — | Pending Azure | 5 req/s for 10 min; API p95 <500ms, errors <1%. |
| Ingestion capacity | — | — | Pending | Replay at 10× and record lag onset. |
| Security | 2026-10-13 | Unauthenticated local API routes return 401; missing server key fails closed | Partial (local only) | Anonymous ADLS denial and repository secret scan remain pending in dev. |
| Model rollback | — | — | Pending Azure ML | Restore previous model version and verify next processing run. |
| Production soak | — | — | Not in dev scope | Owner-only production test: seven days; record SLO/cost values after approval. |

Every result must include UTC date, exact measured value, pass/fail, and a run/commit reference. Failures require cause, fix, and rerun evidence. A dev deployment or a passing local unit test is not evidence of production readiness.
