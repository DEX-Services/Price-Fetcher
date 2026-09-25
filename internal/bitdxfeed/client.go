// Package bitdxfeed polls the BitDx BI2X data-feed API — a single-symbol REST
// endpoint (https://bitdx-feed-ez3b.onrender.com/) that always returns the
// current BI2X/BI2XUSD rate as one JSON object, no auth required:
//
//	{"symbol":"BI2X/BI2XUSD","rate":"3.29142","high":"3.29507","low":"3.28551",
//	 "open":"3.29280","close":"3.29142","volume":"0.05","timestamp":"1789155525000"}
//
// This is a much simpler wire format than Live-Rates.com (internal/liverates):
// one symbol, one flat object, plain string-encoded numbers with no "n/a"
// placeholders and no error-envelope shape to defend against. A dedicated
// package rather than folding this into liverates because the two providers
// share nothing beyond "poll REST, get numbers" — different host, different
// request shape (no query params/API key here), different response envelope
// (single object vs an array of quotes).
package bitdxfeed

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dex/price-fetcher/internal/price"
)

const (
	// defaultBaseURL is the feed the user provided 2026-09-12 for BI2X.
	defaultBaseURL = "https://bitdx-feed-jk3y.onrender.com/"

	httpTimeout  = 10 * time.Second
	maxBodyBytes = 1 << 16 // response is ~200 bytes; this is generous headroom
)

// quoteResponse mirrors the feed's exact response shape. Every numeric field
// is JSON-string-encoded, same defensive posture as internal/liverates.
type quoteResponse struct {
	Symbol    string `json:"symbol"`
	Rate      string `json:"rate"`
	High      string `json:"high"`
	Low       string `json:"low"`
	Open      string `json:"open"`
	Close     string `json:"close"`
	Volume    string `json:"volume"`
	Timestamp string `json:"timestamp"`
}

// Client polls the BitDx feed for one asset (BI2X today; the feed is
// single-symbol, so a second instrument would need its own Client pointed at
// a different baseURL, not a second entry in a list like liverates.Client).
type Client struct {
	asset        string // canonical asset name published to Redis, e.g. "BI2X"
	baseURL      string
	pollInterval time.Duration
	httpc        *http.Client
	log          *slog.Logger
}

// New builds a Client. asset is the canonical name this feed's price is
// published under (e.g. "BI2X"); baseURL empty uses the default the user
// provided; pollInterval <= 0 defaults to 2s, matching liverates' cadence.
func New(asset, baseURL string, pollInterval time.Duration, log *slog.Logger) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultBaseURL
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	return &Client{
		asset:        asset,
		baseURL:      strings.TrimRight(baseURL, "/"),
		pollInterval: pollInterval,
		httpc:        &http.Client{Timeout: httpTimeout},
		log:          log,
	}
}

// Run polls until ctx is cancelled. Transient failures are logged and
// retried on the next tick — same shape as liverates.Client.Run and
// binance.Client.Run, so a BI2X feed outage never takes the process down,
// it just stops refreshing that one asset's price (which the freshness TTL
// on the Redis key then correctly surfaces to consumers as stale).
func (c *Client) Run(ctx context.Context, onPrice func(price.IndexPrice)) {
	c.log.Info("bitdx feed poller starting", "asset", c.asset, "url", c.baseURL, "interval", c.pollInterval)
	c.pollOnce(ctx, onPrice)
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.pollOnce(ctx, onPrice)
		}
	}
}

func (c *Client) pollOnce(ctx context.Context, onPrice func(price.IndexPrice)) {
	q, err := c.fetch(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		c.log.Warn("bitdx feed fetch failed", "asset", c.asset, "err", err)
		return
	}
	p, ok := c.normalize(q)
	if !ok {
		c.log.Warn("bitdx feed returned an unusable quote", "asset", c.asset, "raw", q)
		return
	}
	onPrice(p)
}

func (c *Client) fetch(ctx context.Context) (quoteResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/", nil)
	if err != nil {
		return quoteResponse{}, err
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return quoteResponse{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return quoteResponse{}, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return quoteResponse{}, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(body, 200))
	}

	var q quoteResponse
	if err := json.Unmarshal(body, &q); err != nil {
		return quoteResponse{}, fmt.Errorf("decode quote: %w (%s)", err, truncate(body, 200))
	}
	return q, nil
}

// normalize converts the raw response into an IndexPrice keyed by our
// canonical asset name. "rate" is the feed's authoritative current price —
// unlike liverates there is no separate bid/ask to average, so rate IS last.
func (c *Client) normalize(q quoteResponse) (price.IndexPrice, bool) {
	last := parseFloat(q.Rate, 0)
	if last <= 0 {
		return price.IndexPrice{}, false
	}

	open := parseFloat(q.Open, 0)
	changePct := 0.0
	if open > 0 {
		changePct = (last - open) / open * 100
	}

	return price.IndexPrice{
		Asset:         c.asset,
		Source:        "bitdxfeed:" + c.asset,
		Last:          last,
		ChangePercent: changePct,
		High24h:       parseFloat(q.High, 0),
		Low24h:        parseFloat(q.Low, 0),
		QuoteVolume:   parseFloat(q.Volume, 0),
		// The feed's own "timestamp" is when ITS upstream last moved, which
		// can lag behind our poll time during a quiet market — using our own
		// receive time here matches every other feed in this service
		// (binance, liverates) and is what the downstream staleness check
		// (price.Fresh) actually needs: "how long ago did WE last hear from
		// this feed", not "how old is the tick upstream".
		TimestampMs: time.Now().UnixMilli(),
	}, true
}

func parseFloat(s string, def float64) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return def
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "...(truncated)"
}
