// Package config loads Price-Fetcher settings from the environment.
//
// The service is intentionally small: it needs a Redis connection (shared with
// the rest of the platform), the set of assets to track, and a few timing
// knobs. Everything has a sane default so the service runs with only
// REDIS_SERVICE_URI set.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime settings for the price fetcher.
type Config struct {
	// RedisURI is the full rediss:// (TLS) or redis:// connection string.
	// Shared with the matching engine (REDIS_SERVICE_URI).
	RedisURI string

	// Assets is the list of base symbols to track, e.g. ["BTC","ETH","SOL"].
	// Each is mapped to a Binance <ASSET>USDT spot ticker stream.
	Assets []string

	// Quote is the Binance quote asset used to build stream names (USDT is the
	// most liquid and is treated as ~USD for index purposes).
	Quote string

	// KeyPrefix namespaces every Redis key written by this service, e.g.
	// "price:BTC" and channel "price.BTC".
	KeyPrefix string

	// StaleTTL is applied to each latest-price key so a crashed fetcher leaves
	// keys that expire instead of serving a frozen price forever. Consumers
	// should still check the embedded timestamp.
	StaleTTL time.Duration

	// HealthAddr is the listen address for the /healthz endpoint.
	HealthAddr string

	// LiveRatesAPIKey authenticates calls to Live-Rates.com, which supplies
	// every NON-crypto instrument (FX majors, GOLD/SILVER, CrudeOIL, US
	// stocks). Required whenever Instruments is non-empty (the default).
	LiveRatesAPIKey string

	// LiveRatesBaseURL overrides the API base URL. Empty/unset uses the
	// geo-routed apex; pin a region (eu/us/as.live-rates.com) for lower
	// latency at the cost of automatic fail-over.
	LiveRatesBaseURL string

	// Instruments is the list of non-crypto symbols fetched from
	// Live-Rates.com, e.g. ["EURUSD","GOLD","AAPL.us"]. Case matters: the
	// provider rejects "CRUDEOIL"/"aapl.us".
	Instruments []string

	// LiveRatesPoll is the REST polling cadence. One request carries ALL
	// instruments; keep the average below ~1 req/s (the provider's fair-use
	// throttle window).
	LiveRatesPoll time.Duration
}

// DefaultAssets is the tracked crypto set (served by Binance) when ASSETS is
// not provided. Mirrors the crypto perps shown in the frontend market list.
var DefaultAssets = []string{
	"BTC", "ETH", "SOL", "BNB",
}

// DefaultInstruments is the NON-crypto set (served by Live-Rates.com) when
// LIVERATES_INSTRUMENTS is not provided: FX majors, precious metals, energy,
// and US stocks. Case is significant — symbols must match the provider's
// catalog exactly ("CrudeOIL", not "CRUDEOIL"; "AAPL.us", not "aapl.us").
var DefaultInstruments = []string{
	"EURUSD", "GBPUSD", "AUDUSD",
	"GOLD", "SILVER", "CrudeOIL",
	"AAPL.us", "TSLA.us", "NVDA.us",
}

// Load reads configuration from the environment, applying defaults.
func Load() Config {
	return Config{
		RedisURI:         os.Getenv("REDIS_SERVICE_URI"),
		Assets:           parseAssets(os.Getenv("ASSETS")),
		Quote:            envOr("PRICE_QUOTE", "USDT"),
		KeyPrefix:        envOr("PRICE_KEY_PREFIX", "price"),
		StaleTTL:         envDuration("PRICE_STALE_TTL", 30*time.Second),
		HealthAddr:       healthAddr(),
		LiveRatesAPIKey:  strings.TrimSpace(os.Getenv("LIVERATES_API_KEY")),
		LiveRatesBaseURL: envOr("LIVERATES_BASE_URL", "https://www.live-rates.com"),
		Instruments:      parseInstruments(os.Getenv("LIVERATES_INSTRUMENTS")),
		LiveRatesPoll:    envDuration("LIVERATES_POLL_INTERVAL", time.Second),
	}
}

// healthAddr resolves the /healthz listen address. PaaS platforms like Render
// tell us which port to bind via $PORT — honoring it keeps platform routing
// and health probes working; PRICE_HEALTH_ADDR remains the local default.
func healthAddr() string {
	if p := strings.TrimSpace(os.Getenv("PORT")); p != "" {
		return ":" + strings.TrimPrefix(p, ":")
	}
	return envOr("PRICE_HEALTH_ADDR", ":8083")
}

// parseAssets splits a comma-separated ASSETS list, upper-casing and trimming
// each entry. Returns DefaultAssets when empty.
func parseAssets(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return DefaultAssets
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.ToUpper(strings.TrimSpace(p))
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return DefaultAssets
	}
	return out
}

// parseInstruments splits a comma-separated LIVERATES_INSTRUMENTS list,
// trimming whitespace around each entry but PRESERVING case — the provider's
// catalog is case-sensitive ("CrudeOIL", "AAPL.us"). Returns DefaultInstruments
// when empty.
func parseInstruments(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return DefaultInstruments
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return DefaultInstruments
	}
	return out
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	// Accept either a plain number of seconds ("30") or a Go duration ("30s").
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	return def
}
