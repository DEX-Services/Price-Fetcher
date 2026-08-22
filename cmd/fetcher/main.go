// Command price-fetcher streams live index prices from Binance and publishes
// them to Redis, giving every other service (backend, matching engine, bots,
// frontend API) a single shared source of truth for the INDEX price.
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
// Optional env:  ASSETS, PRICE_QUOTE, PRICE_KEY_PREFIX, PRICE_STALE_TTL,
//
//	PRICE_HEALTH_ADDR
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

	"github.com/dex/price-fetcher/internal/binance"
	"github.com/dex/price-fetcher/internal/config"
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
		"assets", cfg.Assets,
		"quote", cfg.Quote,
		"key_prefix", cfg.KeyPrefix,
		"health_addr", cfg.HealthAddr,
	)

	// Root context cancelled on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.RedisURI, cfg.KeyPrefix, cfg.StaleTTL, log)
	if err != nil {
		log.Error("failed to connect to redis", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// health tracks the last time we successfully published each asset, exposed
	// via /healthz so orchestration can tell a live feed from a stalled one.
	health := newHealthTracker()
	go serveHealth(cfg, health, log)

	client := binance.New(cfg.Assets, cfg.Quote, log)

	// onPrice is called for every normalized tick. Publishing to Redis is fast
	// (single pipeline), so we do it inline; failures are logged, not fatal.
	onPrice := func(p price.IndexPrice) {
		if err := st.Publish(ctx, p); err != nil {
			log.Warn("failed to publish price", "asset", p.Asset, "err", err)
			return
		}
		health.mark(p.Asset, p.Last)
	}

	// Run blocks until ctx is cancelled, reconnecting internally on failure.
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
