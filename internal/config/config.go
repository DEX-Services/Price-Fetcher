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
}

// DefaultAssets is the tracked set when ASSETS is not provided. Mirrors the
// crypto perps shown in the frontend market list.
var DefaultAssets = []string{
	"BTC", "ETH", "SOL", "BNB",
}

// Load reads configuration from the environment, applying defaults.
func Load() Config {
	return Config{
		RedisURI:   os.Getenv("REDIS_SERVICE_URI"),
		Assets:     parseAssets(os.Getenv("ASSETS")),
		Quote:      envOr("PRICE_QUOTE", "USDT"),
		KeyPrefix:  envOr("PRICE_KEY_PREFIX", "price"),
		StaleTTL:   envDuration("PRICE_STALE_TTL", 30*time.Second),
		HealthAddr: envOr("PRICE_HEALTH_ADDR", ":8083"),
	}
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
