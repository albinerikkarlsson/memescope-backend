# MemeScope backend: API contract (Phase 1)

Base URL (REST): `https://YOUR-SERVICE.up.railway.app`
WebSocket URL: `wss://YOUR-SERVICE.up.railway.app/ws`

General rules
- All money values are in **SOL**. The website converts to USD/SEK/EUR using the `sol_price` data (`/sol-price` or the WebSocket message).
- Timestamps are Unix milliseconds. `priceSol` = `marketCapSol / 1,000,000,000` (every Pump.fun token has 1 billion supply).
- Lists use **cursor pagination**: send `?limit=50&cursor=<nextCursor>`; `nextCursor` is `null` on the last page. Cursors are opaque strings.
- A value that is not ready is NEVER silently missing. It appears in the coin's `notes` object as `{ "status": "calculating" | "stale" | "error" | "not_applicable", "reason": "text" }`. The UI must show the reason (and a small spinner for `calculating`), never a bare "Unavailable".
- Errors: `{ "error": { "code": "not_found", "message": "Coin not found" } }` with a matching HTTP status.
- CORS: controlled by the `ALLOWED_ORIGINS` env var (default `*`).

## CoinSummary (used everywhere)

```json
{
  "mint": "string (token address)",
  "name": "string", "symbol": "string",
  "image": "https url or null (null while metadata loads)",
  "creator": "wallet address",
  "createdAt": 1700000000000,
  "status": "new | moving | about_to_graduate | graduated | dead | rugged | cooling",
  "categories": ["new", "moving"],
  "priceSol": 0.0000001,
  "marketCapSol": 104.38,
  "athMarketCapSol": 104.38,
  "initialMarketCapSol": 32,
  "bondingProgress": 0.24,
  "volumeSol": { "m1": 4.0, "m5": 4.0, "h1": 4.0 },
  "txns": { "m1": {"buys": 32, "sells": 8}, "m5": {"buys": 32, "sells": 8}, "h1": {"buys": 32, "sells": 8} },
  "changePct": { "m1": 12.5, "m5": 226.2, "h1": 226.2 },
  "holders": 8,
  "top10Pct": 8.9,
  "devHoldingPct": 5,
  "sniperPct": 4.2,
  "tradeCount": 40,
  "lastTradeAt": 1700000000000,
  "socials": { "twitter": null, "telegram": null, "website": null },
  "links": { "pumpfun": "https://...", "solscan": "https://..." },
  "notes": {
    "bundledPct": { "status": "calculating", "reason": "Needs on-chain wallet analysis (Phase 2)" },
    "riskScore": { "status": "calculating", "reason": "Risk engine arrives in Phase 3" },
    "metadata": { "status": "calculating", "reason": "Retrying metadata fetch (1/3)" }
  }
}
```

### Status and categories (version 1 rules, heuristics)
A coin has one primary `status` and a list of all `categories` it belongs to. The feed columns should use `categories` (a coin can be in both "new" and "moving").
- `new`: younger than 5 minutes.
- `moving`: traded in the last 60s AND (price up 20 percent or more over 5 minutes OR 10+ trades in the last minute).
- `about_to_graduate`: bonding curve progress between 0.8 and 1.
- `graduated`: bonding curve progress is 1 (estimated from curve reserves).
- `dead`: no trade for 10 minutes.
- `rugged`: crashed 60 percent from its peak AND the creator sold 80 percent or more of their peak balance, OR crashed 80 percent from a peak that was at least 2x the launch value.
- `cooling`: none of the above.
Priority for `status`: graduated, rugged, dead, about_to_graduate, moving, new, cooling.

`bondingProgress` is estimated from the virtual token reserve (start 1,073,000,000; about 793,100,000 tokens are sold before graduation). `holders`, `top10Pct`, `devHoldingPct`, `sniperPct` are derived from trades seen since the server started following the coin. "Sniper" = buyer (not the creator) within 3 seconds of launch.

## REST endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | `{status, upstream, tokensTracked, eventsPerSec, clients, uptimeSec, solPriceStatus, serverTime}` |
| `GET /sol-price` | `{usd, sek, eur, updatedAt, status, reason}` |
| `GET /coins` | `{items: CoinSummary[], nextCursor, total}` |
| `GET /coins/:mint` | CoinSummary + `description`, `uri`, `creatorOtherCoins: CoinSummary[]` |
| `GET /coins/:mint/trades` | `{items: Trade[], nextCursor}` newest first (last 200 kept) |
| `GET /coins/:mint/holders` | `{items: Holder[], nextCursor, totalHolders}` |
| `GET /coins/:mint/candles?tf=1m` | `tf` = `1m`, `5m`, `15m`, `1h`. `{tf, items: Candle[]}` (last 3 hours) |
| `GET /creators/:wallet` | `{wallet, coinsSeen, ruggedCount, deadCount, coins, coverage}` (only coins seen since server start) |
| `GET /search?q=` | `{coins: CoinSummary[], creators: [{wallet, coinsSeen}]}` (min 2 characters) |

### `GET /coins` query parameters
`status`, `category` (new, moving, about_to_graduate, graduated, dead, rugged, cooling), `creator`, `q` (name, symbol, mint prefix or creator prefix), `minMarketCapSol`, `maxMarketCapSol`, `minAgeSec`, `maxAgeSec`, `minHolders`, `maxTop10Pct`, `minVolume5mSol`, `maxDevHoldingPct`, `maxSniperPct`, `minBondingProgress`, `maxBondingProgress`, `hasTwitter`, `hasTelegram`, `hasWebsite` (true/false), `sort` (createdAt, marketCap, volume5m, volume1h, change5m, holders, lastTrade, progress), `order` (asc/desc, default desc), `limit` (1 to 200, default 50), `cursor`.

### Trade
`{signature, time, side: "buy"|"sell", solAmount, tokenAmount, wallet, marketCapSol, priceSol}`

### Holder
`{rank, wallet, balance, pctSupply, valueSol, avgEntrySol, realizedPnlSol, unrealizedPnlSol, status: "holding"|"partly_sold", tags: ["creator"|"sniper"], firstSeenAt, links: {solscan}}`
(P&L is estimated from the trades seen since the server started following the coin.)

### Candle
`{time, open, high, low, close, volumeSol, buys, sells}` (open/high/low/close are market cap in SOL)

## WebSocket `/ws`

On connect the server automatically sends, in this order:
1. `{"type":"system_status", upstream, tokensTracked, eventsPerSec, clients, uptimeSec, solPriceStatus, serverTime}` (repeated every 5 seconds)
2. `{"type":"sol_price", usd, sek, eur, updatedAt, status, reason}` (repeated whenever it changes, about every 30s)
3. `{"type":"snapshot", "channel":"feed", "coins": CoinSummary[]}` (up to 1000 most recently active coins)

Then every ~300 ms (only when something changed):
```json
{ "type": "batch", "ts": 1700000000000, "events": [
  { "type": "coin_new", "coin": { /* CoinSummary */ } },
  { "type": "coin_update", "mint": "...", "fields": { "marketCapSol": 110.2, "holders": 9 } }
] }
```
`coin_update.fields` contains ONLY the fields that changed; merge them into the stored coin. Replace objects (`volumeSol`, `txns`, `changePct`, `socials`, `notes`, `categories`) as a whole.

### Client messages
- `{"type":"subscribe","channel":"feed"}` / `{"type":"unsubscribe","channel":"feed"}` (feed is on by default)
- `{"type":"subscribe","channel":"coin","mint":"..."}` (max 20). The server answers with `{"type":"coin_snapshot","coin","trades":[...50],"holders":[...20]}`, then sends:
  - `{"type":"trades","mint","trades":[Trade...]}` as they happen
  - `{"type":"holders_update","mint","holders":[top 20]}` once per second
- `{"type":"unsubscribe","channel":"coin","mint":"..."}`
- `{"type":"ping"}` answered by `{"type":"pong","ts"}`
- Errors arrive as `{"type":"error","message"}`.

The browser must reconnect automatically with growing delays (1s, 2s, 4s ... up to 30s) and then receives a fresh snapshot.

## Environment variables (backend)
- `PORT` (set automatically by the host)
- `ALLOWED_ORIGINS` optional, comma separated, for example `https://your-site.lovable.app`. Default `*`.
- `PUMPPORTAL_WS_URL` optional, default `wss://pumpportal.fun/api/data`.
No Helius key is needed in Phase 1. It is used from Phase 2 (holder verification and creator history).

## Not in Phase 1 (arrives later)
`bundledPct`, `riskScore`, flags, creator history before the server started, social data (X/Twitch), 1s and 15s candles, data that survives a restart.
