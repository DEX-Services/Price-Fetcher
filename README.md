# Price-Fetcher

A small standalone service that streams live **index prices** from Binance and
publishes them to Redis, so every other service (backend, matching engine, bots,
frontend API) reads **one shared price** instead of each hitting Binance
independently.

## Why this exists

The platform previously derived prices from several independent sources — a
hardcoded mock seed, per-component Binance calls in the browser, and the
engine's own book — which drifted apart (e.g. header showing ~$64.7k while the
trade panel showed ~$67.4k). This service gives a single authoritative index
price with one upstream connection.

## What it is (and isn't)

- **Index / oracle price** — the external reference used for mark price,
  funding, and liquidations. **This service owns it.**
- **Order-book / last-trade price** — produced by the matching engine from real
  orders. **This service does NOT touch it.**

Keeping these separate is deliberate: copying Binance into the order book would
turn the exchange into a mirror with no real liquidity.

## How it works

```
Binance combined WS stream ──> price-fetcher ──> Redis ──> backend / engine / bots / frontend
   (<ASSET>usdt@ticker)          (this service)   (SET + PUBLISH)
```

- One WebSocket connection carries all tracked assets (combined stream), with
  automatic reconnect + exponential backoff and ping/pong keepalive.
- For each tick it writes to Redis in a single pipeline:
  - `SET  price:<ASSET>` → JSON, with a short TTL (default 30s).
  - `PUBLISH price.<ASSET>` → JSON, for push consumers.

## Redis contract

**Key** `price:BTC` and **channel** `price.BTC` both carry:

```json
{
  "asset": "BTC",
  "source": "binance:btcusdt",
  "last": 64711.05,
  "change_percent": 1.08,
  "high_24h": 64967.25,
  "low_24h": 63887.73,
  "quote_volume": 538960000.0,
  "timestamp_ms": 1752921600000
}
```

**Consumers must check `timestamp_ms`.** If it is older than your freshness
threshold, treat the price as stale — in particular, do NOT liquidate on a
frozen price. The key TTL is a backstop; the timestamp is the real guard.

## Configuration

All via environment (or a local `.env`). Only `REDIS_SERVICE_URI` is required.

| Var | Default | Meaning |
|---|---|---|
| `REDIS_SERVICE_URI` | — | `rediss://` (TLS) or `redis://` connection string |
| `ASSETS` | built-in list | Comma-separated base symbols, e.g. `BTC,ETH,SOL` |
| `PRICE_QUOTE` | `USDT` | Binance quote asset for stream names |
| `PRICE_KEY_PREFIX` | `price` | Namespace for keys (`price:BTC`) / channels (`price.BTC`) |
| `PRICE_STALE_TTL` | `30` | Latest-key TTL; seconds (`30`) or duration (`30s`) |
| `PRICE_HEALTH_ADDR` | `:8083` | Listen address for `/healthz` |

## Run

```bash
go mod tidy
go run ./cmd/fetcher
# or build:
go build -o price-fetcher ./cmd/fetcher && ./price-fetcher
```

## Health

`GET /healthz` returns `200` once at least one asset has a fresh (<30s) price,
`503` otherwise, with a per-asset age breakdown:

```json
{ "ok": true, "assets": { "BTC": { "last": 64711.05, "age_ms": 420, "updated_at": "..." } } }
```

## Consuming the price (examples)

Read latest (any service):

```go
data, err := rdb.Get(ctx, "price:BTC").Bytes()
```

Subscribe to updates:

```go
sub := rdb.Subscribe(ctx, "price.BTC")
for msg := range sub.Channel() { /* unmarshal msg.Payload */ }
```

## Next steps (not done here)

- Point the backend/frontend index+mark price at these keys.
- Reconcile the engine's BTC-USDC book to this index (re-seed/peg).
- Add a multi-source median (Binance + others) behind the same key contract.
