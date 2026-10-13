# Development test readiness and owner handoff

This repository is being prepared for the **first dev validation**, not production. No Azure CLI action, resource creation, image push, source probe from Azure, or subscription change has been performed in this workspace.

## Implemented locally

- Binance USDⓈ-M mark-price/liquidation WebSocket reader, REST mark-price/funding fallback, open-interest REST poller, event envelope, bounded exponential retry/jitter, proactive connection rotation, inactivity watchdog, structured logs, local/Event Hubs sinks, and gap markers. Polled mark/open-interest snapshots use local observation time so a skewed exchange timestamp cannot make the captured snapshot appear to come from the future.
- Mark-price-kline and open-interest-history recovery. Binance public sources used here cannot reconstruct the sampled liquidation stream; treat this as an explicit coverage limitation.
- Avro/JSON bronze readers, per-stream Pandera checks, quarantine, deterministic globally unique silver Parquet, DuckDB one-minute gold, two-hour recompute window, daily UTC completeness calculation, robust-zscore scoring, and alert merge/daily cap.
- Chronological robust-zscore/Isolation Forest evaluation and MLflow model registration code; real evaluation is intentionally blocked until the owner supplies a sourced reference set (at least 10 events), a licensed/allowed replay dataset, and Azure ML tracking URI.
- API, API-key enforcement on all routes, 24-hour ADLS-backed cache, dashboard, dev-only Bicep, a CI workflow, and dev what-if/deploy scripts.

## Required owner actions before first Azure deployment

1. Complete all Phase 0 questions in [data-source-notes.md](data-source-notes.md): verify current exchange docs and terms, choose a European region, and run the five-minute probe from that Azure region. This is a hard gate; the fetch attempt from the coding workspace did not provide readable current Binance documentation.
2. Create separate dev/prod boundaries (for this stage only: `rg-derivs-dev`), set your monthly budget amount and contact/dates in `infra/subscription-budget.bicep`, and deploy that subscription-scoped template. It configures 50/80/100% notifications. Budgets notify; they do not stop spend.
3. Install/login Azure CLI, enable Bicep, Docker, and Git. Set the target subscription deliberately; verify `az account show`. The coding workspace did not have Azure CLI, so Bicep compilation could not be run here.
4. Replace the intentionally invalid globally-unique placeholders in `infra/parameters/dev.json`. Names must meet Azure resource naming rules. Review the complete output of `scripts/dev-what-if.ps1` before any `scripts/deploy-dev.ps1` run.
5. First deploy only baseline infrastructure with `-DeployWorkloads $false`. This creates the ACR, Event Hubs, ADLS containers, Key Vault, Log Analytics, and user-assigned identities/roles without starting billable application workloads.
6. Create a high-entropy API key directly in Key Vault using the owner's interactive terminal/secure secret workflow. Never paste it into chat, a parameter file, shell history, GitHub logs, or source. Record only the versionless Key Vault secret URI.
7. Build and push an immutable image tag to the new ACR after infra deployment. Then repeat reviewed what-if/deploy with `-DeployWorkloads $true`, the ACR image tag, and the secret URI. The scripts reject non-dev resource groups and require typed confirmation; they do not deploy automatically.
8. Run the Azure probe from the deployed European region. Then verify Capture AVRO files, scheduled process/backfill/quality jobs, ADLS output, authenticated API responses, and dashboard connectivity.

## First dev acceptance run

Run automated checks in CI/local, then record each numeric result in [test-report.md](test-report.md). In dev test a deterministic fixture replay, idempotency, injected bad/future/duplicate records, API 401 behavior, job execution, ingestion recovery, Capture, and alerts. The guide's two-hour recovery, 99.5% daily completeness, <500ms p95 load test, 10x throughput, notification delivery, and alert thresholds require measurements from the live dev subscription and must not be marked pass before measured.

## Known blockers before the development acceptance suite can be called complete

- The reference event CSV is header-only; no source events were fabricated. The model evaluation command rejects fewer than 10 verified events.
- Exchange terms, current rate limits, WebSocket lifetime, Azure-region connectivity, and historical data availability are not verified. Liquidation history cannot be filled by the current Binance REST backfill.
- Azure ML workspace/MLflow endpoint, model score distribution monitoring, and tested model rollback are not provisioned. MLflow training/registration source exists but has not been run against an Azure ML workspace. The 5-minute scorer currently uses robust-zscore; Isolation Forest selection is evaluation-only pending model-serving integration.
- Azure Monitor alert rules/action group/email delivery and a budget/shutdown procedure are not yet implemented/verified. Container Apps logs are structured JSON; a subscription owner must configure and validate alerts.
- The 24-hour continuous ingest, failure-injection, load/10x, alert, clean-room resource-group rebuild, and 7-day prod soak tests have not run.

**Stop point:** local CI is ready to run; dev infra and workload templates are staged for owner-reviewed what-if/deployment, but no deployment is authorized or performed here. Do not mark Phase 0, Phase 5, or the dev test suite complete until the owner gates above pass.
