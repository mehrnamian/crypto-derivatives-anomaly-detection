# Phase 0 — Data-source verification (dev gate)

**Status: blocked pending live/documentation verification by the owner.** Binance USDⓈ-M is the default hypothesis, not an approved source. This repo has not made a real exchange connection from Azure, reviewed current terms of use, or established the permitted retention/display policy. Do not provision a continuously running ingestor until these rows are completed and the regional test passes.

| Question | Current evidence / official reference | Required recorded answer |
| --- | --- | --- |
| Does the public liquidation stream include every liquidation? | [Liquidation Order Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams) | Verify the documentation's sampling behavior and record its impact on event recall. **Unverified here.** |
| Where do mark price, estimated/settled funding, and open interest come from and how often? | [Mark Price Stream](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream), [Open Interest](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest), [Funding Rate History](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History) | Confirm stream fields/cadence; REST polling weight/limits; historical endpoint retention. **Unverified here.** |
| What are request limits and maximum WebSocket lifetime? | [WebSocket Market Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams) and [General Info](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info) | Record the limits and set `DERIVS_WS_MAX_HOURS` below the documented lifetime. Current default is a conservative 23 hours, not proof of an official maximum. |
| Which historical data can fill gaps, and for how long? | [Mark Price Kline/Candlestick Data](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Mark-Price-Kline-Candlestick-Data), [Open Interest Statistics](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics) | Verify all endpoints, granularity, available history, and request weights. Liquidation gaps are currently explicitly unrecoverable through this implementation's public REST sources. |
| Do terms permit storing/processing/displaying this public data? | [Binance Terms](https://www.binance.com/en/terms) | Owner/legal review and dated conclusion required before data collection. **Not reviewed.** |
| Can the selected European Container Apps region connect? | Run `derivs-anomaly probe --minutes 5 --symbol BTCUSDT` inside a temporary Container Apps job/container in the chosen region. | Record region, start/end UTC, counts by event, total messages, errors, image digest, and date. **Not run in Azure.** |

## Local connectivity probe

`derivs-anomaly probe --minutes 5 --symbol BTCUSDT` connects to the Binance combined mark-price and liquidation stream, counts message types, and emits a JSON report. It is deliberately limited to ten minutes. Run it locally for a smoke check, then repeat from Azure; a local success does not satisfy regional acceptance.

## Go/no-go

Phase 0 passes only after every factual answer above has a dated owner verification, the terms are reviewed, and the Azure probe receives messages. If Binance is blocked from the selected Azure region, repeat the same investigation for Bybit and change the adapter before proceeding.
