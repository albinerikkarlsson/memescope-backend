# MemeScope backend

Always-on service that follows Pump.fun in real time and serves the data to the website.

Run locally: `npm install` then `npm start` (needs Node 18 or newer). Open http://localhost:3000/health

API: see docs/api-contract.md

## Environment variables (all optional)

| Variable | What it does |
| --- | --- |
| `PUMPPORTAL_API_KEY` | Funded PumpPortal key. Without it only new launches are streamed, and prices/volume come from DEX Screener. |
| `DAILY_TRADE_CAP` | Max streamed trades per UTC day (default 50000). Streaming stops at the cap and DEX Screener takes over until the next UTC day. |
| `DATA_DIR` | Where saved state is written (default `./data`). Point it at a Render persistent disk, e.g. `/var/data`, so history survives restarts. |
| `ALLOWED_ORIGINS` | Comma separated list of allowed website origins (default `*`). |
| `FORCE_TRADE_SUBSCRIBE` | `1` = try trade streams without a key (debugging only). |

## What /health shows

`solPriceSource`, `tradeStream` (`no_key`, `live`, `capped`, `connecting`), `tradesToday`, `dailyTradeCap`, `streamedCoins`, `dexLastOkAt`.
