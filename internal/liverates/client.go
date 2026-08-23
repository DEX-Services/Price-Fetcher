// Package liverates polls the Live-Rates.com REST API for NON-crypto
// instruments — FX majors (EURUSD…), metals (GOLD, SILVER), energy
// (CrudeOIL) and US stocks (AAPL.us…) — and emits normalized
// price.IndexPrice values on the same Redis contract as the Binance feed.
//
// Why REST polling rather than the socket.io streaming API: a single
// /api/price request carries every instrument we track, needs only the Go
// stdlib, and keeps the service dependency-light. Upstream rates refresh
// ~1x/second; the default 2s poll interval averages 0.5 req/s, safely under
// the provider's fair-use throttle (>1 req/s avg over 10min => 503 lockout).
//
// Wire-format quirks this package defends against:
//   - every numeric field is JSON-string-encoded ("1.16755"), and some are
//     literally "n/a" (e.g. "close" for US stocks);
//   - FX pairs echo back slashed ("EUR/USD") even though we ask for
//     "EURUSD";
//   - symbol case is SIGNIFICANT ("CrudeOIL", "AAPL.us" — the provider
//     rejects "CRUDEOIL"/"AAPL.US");
//   - auth/param failures arrive as HTTP 200 with a body of
//     [{"error":"Invalid Authentication"}].
package liverates

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dex/price-fetcher/internal/price"
)

const (
	// defaultBaseURL is the geo-routed apex domain. Regional pins
	// (eu/us/as.live-rates.com) trade automatic fail-over for lower latency.
	defaultBaseURL = "https://www.live-rates.com"

	// httpTimeout bounds a single REST call; polls are sequential per tick so
	// this also caps worst-case tick drift.
	httpTimeout = 10 * time.Second

	// maxBodyBytes guards against a hostile/broken upstream response.
	maxBodyBytes = 1 << 20 // 1 MiB — /api/price for ~9 instruments is ~2 KiB.
)

// quote mirrors one entry of the /api/price response. Every numeric field
// arrives string-encoded and may be "n/a", so all fields stay raw here and
// are parsed defensively by numOr/strOr.
type quote struct {
	Currency  json.RawMessage `json:"currency"`
	Rate      json.RawMessage `json:"rate"`
	Bid       json.RawMessage `json:"bid"`
	Ask       json.RawMessage `json:"ask"`
	High      json.RawMessage `json:"high"`
	Low       json.RawMessage `json:"low"`
	Open      json.RawMessage `json:"open"`
	Close     json.RawMessage `json:"close"`
	Timestamp json.RawMessage `json:"timestamp"`
}

// errorEnvelope matches the provider's error shape, which arrives inside an
// HTTP 200 as [{"error":"Invalid Authentication"}] (or "Invalid params
// supplied" when any requested symbol is unknown).
type errorEnvelope struct {
	Err string `json:"error"`
}

// Client polls Live-Rates.com for a fixed set of instruments.
type Client struct {
	instruments  []string          // exact-case provider symbols, e.g. ["CrudeOIL","AAPL.us"]
	lookup       map[string]string // normalized reply currency -> canonical asset name
	apiKey       string
	baseURL      string
	pollInterval time.Duration
	httpc        *http.Client
	log          *slog.Logger
}

// New builds a Client. instruments must use the provider's EXACT symbol case
// (e.g. "CrudeOIL", "AAPL.us"); baseURL may be empty for the default apex;
// pollInterval is the REST polling cadence.
func New(instruments []string, apiKey, baseURL string, pollInterval time.Duration, log *slog.Logger) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultBaseURL
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	lookup := make(map[string]string, len(instruments))
	for _, inst := range instruments {
		lookup[normSym(inst)] = inst
	}
	return &Client{
		instruments:  instruments,
		lookup:       lookup,
		apiKey:       apiKey,
		baseURL:      strings.TrimRight(baseURL, "/"),
		pollInterval: pollInterval,
		httpc:        &http.Client{Timeout: httpTimeout},
		log:          log,
	}
}

// normSym canonicalizes an instrument code for lookup matching: trimmed,
// slash-stripped, upper-cased — so the provider's "EUR/USD" reply matches our
// requested "EURUSD". Only used for MATCHING; the canonical configured name
// (with its original case) is what gets published as the asset label.
func normSym(s string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(s), "/", ""))
}

// Run polls until ctx is cancelled. Transient failures are logged and retried
// on the next tick; the loop never takes the process down. Each successful
// poll delivers every usable quote to onPrice (called inline, in order).
func (c *Client) Run(ctx context.Context, onPrice func(price.IndexPrice)) {
	c.log.Info("live-rates poller starting",
		"instruments", c.instruments,
		"interval", c.pollInterval,
	)
	// Poll immediately so first prices land without waiting a full interval.
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

// pollOnce performs one fetch-publish cycle.
func (c *Client) pollOnce(ctx context.Context, onPrice func(price.IndexPrice)) {
	quotes, err := c.fetch(ctx)
	if err != nil {
		c.log.Warn("live-rates fetch failed", "err", err)
		return
	}
	published := 0
	for _, q := range quotes {
		p, ok := c.normalize(q)
		if !ok {
			continue
		}
		onPrice(p)
		published++
	}
	if published == 0 && len(quotes) > 0 {
		c.log.Warn("live-rates returned no usable quotes", "entries", len(quotes))
	}
}

// fetch requests all instruments in one /api/price call.
func (c *Client) fetch(ctx context.Context) ([]quote, error) {
	q := url.Values{
		"key":  {c.apiKey},
		"rate": {strings.Join(c.instruments, ",")},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/price?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(body, 200))
	}

	// Provider signals auth/param problems INSIDE a 200 via an error envelope.
	var objEnv errorEnvelope
	if err := json.Unmarshal(body, &objEnv); err == nil && objEnv.Err != "" {
		return nil, fmt.Errorf("live-rates error: %s", objEnv.Err)
	}
	var arrEnv []errorEnvelope
	if err := json.Unmarshal(body, &arrEnv); err == nil && len(arrEnv) == 1 && arrEnv[0].Err != "" {
		return nil, fmt.Errorf("live-rates error: %s", arrEnv[0].Err)
	}

	var quotes []quote
	if err := json.Unmarshal(body, &quotes); err != nil {
		return nil, fmt.Errorf("decode quotes: %w (%s)", err, truncate(body, 200))
	}
	return quotes, nil
}

// normalize converts a provider quote into an IndexPrice keyed by OUR asset
// name. The index price is the bid/ask MID — the fairest reference for mark
// price — falling back to bid, ask, then the "rate" field when either side of
// the spread is missing or unusable.
func (c *Client) normalize(q quote) (price.IndexPrice, bool) {
	cur := strOr(q.Currency)
	if cur == "" {
		return price.IndexPrice{}, false
	}
	asset, ok := c.lookup[normSym(cur)]
	if !ok {
		return price.IndexPrice{}, false // not something we asked for; ignore
	}

	bid := numOr(q.Bid, 0)
	ask := numOr(q.Ask, 0)
	last := 0.0
	switch {
	case bid > 0 && ask > 0:
		last = (bid + ask) / 2
	case bid > 0:
		last = bid
	case ask > 0:
		last = ask
	default:
		last = numOr(q.Rate, 0)
	}
	if last <= 0 {
		c.log.Debug("skip quote with unusable price", "currency", cur)
		return price.IndexPrice{}, false
	}

	// Day change vs the session open; live-rates has no rolling 24h open.
	open := numOr(q.Open, 0)
	changePct := 0.0
	if open > 0 {
		changePct = (last - open) / open * 100
	}

	return price.IndexPrice{
		Asset:         asset,
		Source:        "liverates:" + asset,
		Last:          last,
		ChangePercent: changePct,
		High24h:       numOr(q.High, 0),
		Low24h:        numOr(q.Low, 0),
		QuoteVolume:   0, // not provided by live-rates
		TimestampMs:   time.Now().UnixMilli(),
	}, true
}

// strOr extracts a plain string from a raw JSON value ("").
func strOr(r json.RawMessage) string {
	if len(r) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(r, &s); err != nil {
		return ""
	}
	return s
}

// numOr parses a numeric field that may arrive quoted ("1.23"), bare (1.23),
// or as a placeholder like "n/a"; returns def when unusable.
func numOr(r json.RawMessage, def float64) float64 {
	if len(r) == 0 {
		return def
	}
	if r[0] == '"' {
		var s string
		if err := json.Unmarshal(r, &s); err != nil {
			return def
		}
		s = strings.TrimSpace(s)
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
		return def
	}
	if f, err := strconv.ParseFloat(string(r), 64); err == nil {
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
