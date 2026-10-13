# Dev runbook (first-response reference)

Azure alerts and notification actions are not provisioned by this local implementation. Add alert rules/action group and validate notification delivery before calling Phase 5 complete. All events reported here concern data anomalies, not trading recommendations.

| Alert | Meaning | First action |
| --- | --- | --- |
| Gold freshness >15 minutes | Pipeline or source has stopped advancing; a healthy process can still receive no data. | Check authenticated `/status`; compare latest bronze Capture timestamp, ingestion message count, and five-minute processing-job runs. Then run the source probe and inspect exchange status. |
| Input messages/minute is zero or unusually low | WebSocket or open-interest poller is stalled, blocked, or disconnected. | Inspect `websocket_disconnected`, `websocket_reconnect_wait`, and `open_interest_poll_failed` logs; verify DNS/egress, restart only after recording outage time. |
| Processing job failure | Bronze read, schema/data validation, DuckDB aggregation, ADLS authorization, or output write failed. | Read the first exception from the job; preserve bronze; rerun after the root cause is addressed and compare silver key counts. |
| Quarantine rate >1% | Malformed/unsupported schema records are accumulating. | Inspect quarantine reason counts and raw payload samples in restricted storage; never log credentials; update schema only after checking official exchange changes. |
| Reconnect count abnormal | Repeated source network interruptions or watchdog rotations. | Check the 30-second watchdog, network changes, rate-limit close codes, and the documented maximum connection duration; verify each gap closes after recovery. |
| Container restart | App crash, OOM, failed image pull, or readiness failure. | Inspect Container Apps system/console logs, replica exit reason, memory/cpu, and ACR managed-identity pull permissions. Keep ingestion at one replica. |
| API 5xx or latency | ADLS read/cache refresh or API process is failing/slow. | Test `/health` with the API key, check `/status`, ADLS RBAC, local cache and recent scores; do not disable authentication as a workaround. |
| API unauthorized volume | Missing or invalid key requests. | Check client secret rotation/configuration and request source; do not place keys in URLs, logs, source, or screenshots. |
| Daily completeness <99.5% | Too many UTC minute buckets have no source data after backfill. | Compare per-stream bronze/silver minute counts; run `derivs-anomaly recover`; remember sampled liquidation events cannot be reconstructed from the public history endpoint. |
| Event Hubs Capture missing/behind | No Avro files are reaching bronze. | Verify Event Hubs Capture status, namespace managed identity's bronze-only write assignment, hub ingress, container existence, and Archive name format. |
| Model score drift | Daily score distribution departs from the training-time population. | Check coverage/schema changes and reference replay, record the shift, run chronological evaluation; use the previous registered model version to roll back. |
| Budget threshold reached | Estimated subscription cost crossed 50%, 80%, or 100% alert. | Stop dev replicas/job and review Cost Management by tags; budget notifications do not block spend. |

## Safe shutdown / recovery

- To stop ongoing dev spend, use the documented Azure-side scale-to-zero/pause procedure only after deciding whether the scheduled job or capture should remain active. Capture and storage costs may remain.
- Retain bronze when rebuilding dev. Do not delete production resources from a dev runbook.
- Before retrying a processing job, note run ID/time and compare silver uniqueness and gold output. The bronze source remains the replay source of truth.
- Ingestion must remain one replica to avoid duplicate streams. Recovered history is sent to the same Event Hubs hub and uses the same envelope.
