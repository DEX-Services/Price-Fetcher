// Package price defines the shared on-the-wire price representation written to
// Redis by the fetcher and read by every other service (backend, engine, bots,
// frontend API).
//
// This is the single source of truth for the INDEX price of an asset — the
// external reference used for mark price, funding, and liquidations. It is NOT
// the order-book last-trade price, which is owned by the matching engine.
package price

// IndexPrice is the JSON payload stored at "<prefix>:<ASSET>" and published on
// channel "<prefix>.<ASSET>".
type IndexPrice struct {
	// Asset is the base symbol, e.g. "BTC".
	Asset string `json:"asset"`

	// Source is the upstream feed, e.g. "binance:BTCUSDT".
	Source string `json:"source"`

	// Last is the latest traded price from the source.
	Last float64 `json:"last"`

	// ChangePercent is the source's 24h price change percentage.
	ChangePercent float64 `json:"change_percent"`

	// High24h / Low24h are the source's rolling 24h extremes.
	High24h float64 `json:"high_24h"`
	Low24h  float64 `json:"low_24h"`

	// QuoteVolume is the source's 24h quote-asset volume (~USD).
	QuoteVolume float64 `json:"quote_volume"`

	// TimestampMs is the fetcher's receive time in Unix milliseconds. Consumers
	// MUST treat the price as stale (and refuse to liquidate on it) if this is
	// older than their freshness threshold.
	TimestampMs int64 `json:"timestamp_ms"`
}
