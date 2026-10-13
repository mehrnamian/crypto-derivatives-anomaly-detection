# Implementation status versus Azure guide

Reviewed against the October 13, 2026 guide and current source tree. This is a code review, not evidence of a live Azure environment. Azure deployment and subscription operations remain the owner's responsibility.

| Guide phase | Current local implementation | Status / outstanding gate |
| --- | --- | --- |
| Prerequisites | Dev-only Bicep and manual dev scripts exist; no prod config. | Owner must establish budget/alerts, region, GitHub OIDC/environment approvals, and valid unique resource names. No Azure operation was run. |
| 0 — Data source | Bounded five-minute-capable Binance probe and official-reference note exist. | **Blocked**: current docs, rate limits, source terms, historical limits, and selected-region connectivity have not been verified. See `docs/data-source-notes.md`. |
| 1 — Ingestion | WebSocket, REST mark-price/funding fallback, sampled liquidation, REST open-interest polling, validated envelope, Event Hubs Entra ID sink, local JSONL sink, retry/jitter, watchdog, proactive rotation, persistent captured gap markers, and REST mark/OI backfill. Polled mark/OI snapshots use local observation time to avoid exchange/host clock skew. | Code present; no 24-hour/Azure run yet. Sampled liquidation gaps cannot be recovered from the implemented public REST endpoints. |
| 2 — Storage / quality | Avro and local bronze, strict Pandera schemas, quarantine, deduplication, idempotent Parquet silver, DuckDB minute gold, two-hour recomputation, daily completeness measurement, Azure ADLS staging. | Local path exercised by tests. ADLS, Azure Monitor metrics, scale, late data, and real 99.5% daily measurement are pending. |
| 3 — Model / evaluation | Shared past-only features, robust z-score and Isolation Forest chronological comparison, reference-event validation, MLflow tracking/registration call. | Evaluation blocks without 10 sourced events and replay data. Reference CSV is deliberately header-only. Azure ML workspace/URI and drift monitoring/serving the selected Isolation Forest are not complete. Scoring pipeline is robust-zscore. |
| 4 — API / dashboard | FastAPI authenticated `/health`, `/status`, `/anomalies`; constant-time API-key check; bounded 24-hour cache with ADLS refresh; Streamlit charts and health panel. | Local API tests pass. Azure Key Vault reference, ACR pull, p95/load, and dashboard end-to-end test are pending. Shadow-model mode is not implemented. |
| 5 — IaC / CI / monitoring | Dev Bicep for ADLS containers, Event Hubs Capture, ACR, Key Vault, Container Apps, scheduled processing/backfill/quality jobs, Log Analytics and least-privilege user-assigned identities. GitHub CI builds/test/lints/Bicep/Docker; manual dev what-if/deploy scripts. | **Not deployment-validated**: Azure CLI/Bicep unavailable here. Azure Monitor alert rules/action group/email, Azure ML workspace, resource-group rebuild, OIDC config and budget/shutdown process remain owner tasks. No production deployment. |
| 6 — Acceptance | Local tests cover domain contracts, data-quality/idempotency/quarantine, features, API auth, and gold processing. | 26 local tests passed, Ruff passed (2026-10-14). Azure-only numeric SLOs/failure/load/soak tests are pending. Results are in `docs/test-report.md`. |

## Security and operational notes

- API credentials have no insecure default, are represented as `SecretStr`, and all API endpoints fail closed (401 for missing/wrong key, 503 if server auth is unconfigured). The Bicep app retrieves its key by Key Vault reference and managed identity.
- Event Hubs uses `DefaultAzureCredential` and no connection string; ADLS uses Entra ID. Bicep disables storage shared keys, anonymous blob access, Event Hubs local auth, and configures TLS 1.2.
- App-specific user-assigned identities receive scoped ACR/Event Hubs/ADLS roles. Event Hubs Capture's namespace identity writes only to bronze; processor reads bronze and writes silver/gold/quarantine; API/backfill only read gold.
- Event keys are deduplication identifiers, not authentication tokens. Bronze contains raw public market messages and should remain private.
- Azure resource configuration is code only. No deployments, budget changes, credentials, source data, or customer resources were touched.

## Go/no-go

**Ready for local CI and owner-reviewed dev deployment preparation; not yet ready to claim Phase 0 or the dev acceptance suite complete.** Complete the explicit owner blockers and workflow in `docs/dev-test-readiness.md`. Production remains no-go.
