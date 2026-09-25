// Command price-fetcher streams live index prices and publishes them to
// Redis, giving every other service (backend, matching engine, bots,
// frontend API) a single shared source of truth for the INDEX price.
//
// Three upstream feeds, one Redis contract:
//   - Crypto (BTC, ETH, …) streams from Coinbase Exchange's ticker WebSocket
//     (switched from Binance 2026-09-25 — Binance blocks this host's
//     outbound IPs at the WebSocket handshake; see internal/coinbase).
//   - Non-crypto instruments (FX majors, GOLD/SILVER, CrudeOIL, US stocks)
//     are polled from the Live-Rates.com REST API. Currently DISABLED
//     (empty DefaultInstruments) — crypto-only launch, see config.go.
//   - BI2X polls its own dedicated single-symbol feed (internal/bitdxfeed).
//
// This price is used for mark price, funding, and liquidation reference. It is
// deliberately separate from the order-book last-trade price, which is owned by
// the matching engine.
//
// Usage:
//
//	price-fetcher            # reads config from environment / .env
//
// Required env:  REDIS_SERVICE_URI
// Optional env:  ASSETS (crypto), PRICE_QUOTE, PRICE_KEY_PREFIX,
//
//	PRICE_STALE_TTL, PRICE_HEALTH_ADDR,
//	LIVERATES_API_KEY, LIVERATES_INSTRUMENTS, LIVERATES_BASE_URL,
//	LIVERATES_POLL_INTERVAL,
//	BITDX_FEED_ENABLED, BITDX_FEED_ASSET, BITDX_FEED_URL,
//	BITDX_FEED_POLL_INTERVAL
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/dex/price-fetcher/internal/bitdxfeed"
	"github.com/dex/price-fetcher/internal/coinbase"
	"github.com/dex/price-fetcher/internal/config"
	"github.com/dex/price-fetcher/internal/liverates"
	"github.com/dex/price-fetcher/internal/price"
	"github.com/dex/price-fetcher/internal/store"
	"github.com/joho/godotenv"
)

func main() {
	// Load .env if present (matches backend/engine convention). Ignore error:
	// in production env vars are set directly.
	_ = godotenv.Load()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg := config.Load()
	log.Info("price-fetcher starting",
		"crypto_assets", cfg.Assets,
		"instruments", cfg.Instruments,
		"bitdx_feed_enabled", cfg.BitDxFeedEnabled,
		"bitdx_feed_asset", cfg.BitDxFeedAsset,
		"quote", cfg.Quote,
		"key_prefix", cfg.KeyPrefix,
		"health_addr", cfg.HealthAddr,
	)

	// Fail fast rather than silently serving half the catalog: consumers key
	// liquidation logic off these prices, so a missing feed must be loud.
	if len(cfg.Instruments) > 0 && cfg.LiveRatesAPIKey == "" {
		log.Error("LIVERATES_API_KEY is required: non-crypto instruments are tracked via Live-Rates.com",
			"instruments", cfg.Instruments)
		os.Exit(1)
	}

	// Root context cancelled on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Connect with bounded retries: fresh instances and platform restarts can
	// hit transient DNS/TLS hiccups against the managed Redis host (seen as
	// "lookup ...: i/o timeout" on Render), and a single failed ping must not
	// kill the deploy. Gives up after ~95s of consecutive failures.
	var (
		st  *store.Store
		err error
	)
	connectBackoff := time.Second
	for attempt := 1; ; attempt++ {
		st, err = store.New(ctx, cfg.RedisURI, cfg.KeyPrefix, cfg.StaleTTL, log)
		if err == nil {
			break
		}
		if ctx.Err() != nil || attempt >= 12 {
			log.Error("failed to connect to redis", "err", err, "attempts", attempt)
			os.Exit(1)
		}
		log.Warn("redis connect failed, retrying",
			"err", err, "attempt", attempt, "backoff", connectBackoff)
		select {
		case <-ctx.Done():
			os.Exit(1)
		case <-time.After(connectBackoff):
		}
		connectBackoff *= 2
		if connectBackoff > 10*time.Second {
			connectBackoff = 10 * time.Second
		}
	}
	defer st.Close()

	// health tracks the last time we successfully published each asset, exposed
	// via /healthz so orchestration can tell a live feed from a stalled one.
	health := newHealthTracker()
	go serveHealth(cfg, health, log)

	// onPrice is called for every normalized tick from EITHER feed. Publishing
	// to Redis is fast (single pipeline), so we do it inline; failures are
	// logged, not fatal. Both feeds funnel through here, so Redis keys,
	// channels, and health tracking treat every asset identically.
	onPrice := func(p price.IndexPrice) {
		if err := st.Publish(ctx, p); err != nil {
			if ctx.Err() != nil {
				return // shutting down: in-flight publishes are expected to fail
			}
			log.Warn("failed to publish price", "asset", p.Asset, "err", err)
			return
		}
		health.mark(p.Asset, p.Last)
	}

	// Non-crypto instruments poll Live-Rates.com in their own goroutine; the
	// loop stops when ctx is cancelled on shutdown.
	if len(cfg.Instruments) > 0 {
		lrClient := liverates.New(
			cfg.Instruments, cfg.LiveRatesAPIKey, cfg.LiveRatesBaseURL,
			cfg.LiveRatesPoll, log,
		)
		go lrClient.Run(ctx, onPrice)
	}

	// BI2X polls its own dedicated single-symbol feed (internal/bitdxfeed),
	// same shape as the Live-Rates goroutine above: its own poll loop, same
	// onPrice funnel, so Redis keys/channels/health tracking treat it
	// identically to every other asset. See BitDxFeedEnabled's doc comment
	// for the disable-without-a-code-change escape hatch.
	if cfg.BitDxFeedEnabled {
		bdClient := bitdxfeed.New(cfg.BitDxFeedAsset, cfg.BitDxFeedURL, cfg.BitDxFeedPoll, log)
		go bdClient.Run(ctx, onPrice)
	}

	// Run blocks until ctx is cancelled, reconnecting internally on failure.
	// Coinbase Exchange, not Binance (2026-09-25): Binance's WebSocket
	// gateway blocks this service's hosting provider's outbound IPs at the
	// handshake — see internal/coinbase's doc comment.
	client := coinbase.New(cfg.Assets, cfg.Quote, log)
	client.Run(ctx, onPrice)

	log.Info("price-fetcher shutting down")
}

// healthTracker records the last publish time and price per asset.
type healthTracker struct {
	mu      sync.RWMutex
	updated map[string]assetStatus
}

type assetStatus struct {
	Last      float64 `json:"last"`
	UpdatedAt string  `json:"updated_at"`
	AgeMs     int64   `json:"age_ms"`
}

func newHealthTracker() *healthTracker {
	return &healthTracker{updated: make(map[string]assetStatus)}
}

func (h *healthTracker) mark(asset string, last float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.updated[asset] = assetStatus{Last: last, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
}

// snapshot returns a copy of current statuses with freshly computed ages.
func (h *healthTracker) snapshot() (map[string]assetStatus, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]assetStatus, len(h.updated))
	anyFresh := false
	for asset, s := range h.updated {
		if t, err := time.Parse(time.RFC3339Nano, s.UpdatedAt); err == nil {
			s.AgeMs = time.Since(t).Milliseconds()
			if s.AgeMs < 30_000 {
				anyFresh = true
			}
		}
		out[asset] = s
	}
	return out, anyFresh
}

// serveHealth exposes /healthz. It returns 200 once at least one asset has a
// fresh (<30s) price, else 503 — so a stalled feed is visibly unhealthy.
func serveHealth(cfg config.Config, h *healthTracker, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		statuses, anyFresh := h.snapshot()
		w.Header().Set("Content-Type", "application/json")
		if !anyFresh {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":     anyFresh,
			"assets": statuses,
		})
	})
	srv := &http.Server{Addr: cfg.HealthAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Info("health endpoint listening", "addr", cfg.HealthAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Warn("health server stopped", "err", err)
	}
}
