// Package store persists index prices to Redis so every other service reads a
// single shared value instead of hitting Binance independently.
//
// For each update we do two things:
//   - SET  "<prefix>:<ASSET>"  -> JSON, with a short TTL. The TTL means a dead
//     fetcher's keys expire rather than serving a frozen price forever.
//   - PUBLISH "<prefix>.<ASSET>" -> JSON, for consumers that want push updates
//     (e.g. the engine's mark-price / liquidation loop) instead of polling.
//
// Connection setup mirrors the matching engine (REDIS_SERVICE_URI, rediss:// =
// TLS, go-redis/v9) so both services share the same Aiven instance.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/dex/price-fetcher/internal/price"
	"github.com/redis/go-redis/v9"
)

// Store wraps a redis.Client with price-specific helpers.
type Store struct {
	rdb    *redis.Client
	prefix string
	ttl    time.Duration
	log    *slog.Logger
}

// New connects to Redis using the given URI (rediss:// for TLS). keyPrefix
// namespaces every key/channel; ttl bounds how long a latest-price key lives
// without a refresh.
func New(ctx context.Context, uri, keyPrefix string, ttl time.Duration, log *slog.Logger) (*Store, error) {
	if uri == "" {
		return nil, fmt.Errorf("REDIS_SERVICE_URI is not set")
	}
	opts, err := redis.ParseURL(uri)
	if err != nil {
		return nil, fmt.Errorf("parse redis URI: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	log.Info("redis connected", "prefix", keyPrefix, "ttl", ttl)
	return &Store{rdb: rdb, prefix: keyPrefix, ttl: ttl, log: log}, nil
}

// Publish stores the latest value and publishes it to subscribers. Both the SET
// and PUBLISH are issued in a single pipeline round-trip.
func (s *Store) Publish(ctx context.Context, p price.IndexPrice) error {
	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal price: %w", err)
	}
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, s.key(p.Asset), data, s.ttl)
	pipe.Publish(ctx, s.channel(p.Asset), data)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis pipeline: %w", err)
	}
	return nil
}

// Close releases the Redis connection.
func (s *Store) Close() error {
	return s.rdb.Close()
}

// key is the latest-price key for an asset, e.g. "price:BTC".
func (s *Store) key(asset string) string {
	return fmt.Sprintf("%s:%s", s.prefix, asset)
}

// channel is the pub/sub channel for an asset, e.g. "price.BTC".
func (s *Store) channel(asset string) string {
	return fmt.Sprintf("%s.%s", s.prefix, asset)
}
