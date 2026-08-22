// Package binance streams live ticker data from Binance over a single combined
// WebSocket connection and emits normalized price.IndexPrice values.
//
// We use the WebSocket !ticker combined stream rather than REST polling: it
// pushes updates as they happen (sub-second), costs no REST rate-limit budget,
// and avoids the many-services-many-connections divergence that a poll-per-
// consumer design creates.
package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/dex/price-fetcher/internal/price"
	"github.com/gorilla/websocket"
)

const (
	// wsBase is the Binance spot combined-stream endpoint.
	wsBase = "wss://stream.binance.com:9443/stream"

	// writeWait / pongWait / pingPeriod keep the connection healthy. Binance
	// sends a ping every ~3 min and closes the socket if we don't pong; we also
	// send our own pings to detect a dead peer quickly.
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 30 * time.Second

	// reconnectMin / reconnectMax bound the exponential backoff between retries.
	reconnectMin = 1 * time.Second
	reconnectMax = 30 * time.Second
)

// streamMessage is the envelope Binance wraps each event in on a combined
// stream: {"stream":"btcusdt@ticker","data":{...}}.
type streamMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// tickerEvent is the subset of Binance's 24hr ticker payload we care about.
//
// The payload contains BOTH lowercase and uppercase single-letter keys that
// collide case-insensitively — e.g. "c" (last price, string) and "C" (stat
// close time, number). Go's encoding/json matches fields case-insensitively
// and, when both keys are present, may bind the WRONG one (we observed "c"
// receiving the "C" timestamp). To avoid that ambiguity we decode into a
// map[string]json.RawMessage and pull exact keys ourselves.
type tickerEvent struct {
	Symbol      string
	LastPrice   json.Number
	PriceChange json.Number
	High        json.Number
	Low         json.Number
	QuoteVolume json.Number
}

// parseTicker extracts the fields we need from a raw ticker payload using
// exact (case-sensitive) key lookups.
func parseTicker(data json.RawMessage) (tickerEvent, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return tickerEvent{}, err
	}
	var ev tickerEvent
	_ = json.Unmarshal(raw["s"], &ev.Symbol)
	ev.LastPrice = numFrom(raw["c"])
	ev.PriceChange = numFrom(raw["P"])
	ev.High = numFrom(raw["h"])
	ev.Low = numFrom(raw["l"])
	ev.QuoteVolume = numFrom(raw["q"])
	return ev, nil
}

// numFrom converts a raw JSON value (string "1.23" or number 1.23) into a
// json.Number, tolerating either encoding.
func numFrom(r json.RawMessage) json.Number {
	if len(r) == 0 {
		return ""
	}
	// String-encoded: strip the surrounding quotes.
	if r[0] == '"' {
		var s string
		if err := json.Unmarshal(r, &s); err == nil {
			return json.Number(s)
		}
		return ""
	}
	return json.Number(r)
}

// Client streams ticker updates for a set of assets.
type Client struct {
	assets    []string          // base symbols, e.g. ["BTC","ETH"]
	quote     string            // e.g. "USDT"
	streamMap map[string]string // "BTCUSDT" -> "BTC"
	log       *slog.Logger
}

// New builds a Client for the given base assets and quote asset.
func New(assets []string, quote string, log *slog.Logger) *Client {
	quote = strings.ToUpper(quote)
	streamMap := make(map[string]string, len(assets))
	for _, a := range assets {
		a = strings.ToUpper(a)
		streamMap[a+quote] = a
	}
	return &Client{assets: assets, quote: quote, streamMap: streamMap, log: log}
}

// Run connects and streams until ctx is cancelled, reconnecting with backoff on
// any error. Each normalized update is delivered to onPrice. onPrice must be
// safe to call from this goroutine and should not block for long.
func (c *Client) Run(ctx context.Context, onPrice func(price.IndexPrice)) {
	backoff := reconnectMin
	for {
		if ctx.Err() != nil {
			return
		}
		err := c.connectAndStream(ctx, onPrice)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.log.Warn("binance stream ended, reconnecting", "err", err, "backoff", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		// Exponential backoff, capped.
		backoff *= 2
		if backoff > reconnectMax {
			backoff = reconnectMax
		}
	}
}

// connectAndStream runs a single connection lifecycle. It returns when the
// connection drops or ctx is cancelled.
func (c *Client) connectAndStream(ctx context.Context, onPrice func(price.IndexPrice)) error {
	url := c.streamURL()
	c.log.Info("connecting to binance", "streams", len(c.streamMap))

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// A successful connection resets nothing here; backoff reset happens in Run
	// only after we actually receive data would be stricter, but resetting on
	// connect is fine for this feed.
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Ping loop in a child goroutine; cancelled when this function returns.
	pingCtx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	go c.pingLoop(pingCtx, conn)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		conn.SetReadDeadline(time.Now().Add(pongWait))

		var msg streamMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			c.log.Debug("skip unparsable frame", "err", err)
			continue
		}
		ev, err := parseTicker(msg.Data)
		if err != nil {
			c.log.Debug("skip unparsable data", "err", err)
			continue
		}
		asset, ok := c.streamMap[ev.Symbol]
		if !ok {
			continue
		}
		p, ok := c.normalize(asset, ev)
		if !ok {
			continue
		}
		onPrice(p)
	}
}

// pingLoop sends periodic WebSocket pings so a dead peer is detected via the
// read deadline even when Binance is quiet.
func (c *Client) pingLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// normalize converts a raw Binance ticker into an IndexPrice, dropping updates
// with an unparsable or non-positive last price.
func (c *Client) normalize(asset string, ev tickerEvent) (price.IndexPrice, bool) {
	last, err := strconv.ParseFloat(ev.LastPrice.String(), 64)
	if err != nil || last <= 0 {
		return price.IndexPrice{}, false
	}
	return price.IndexPrice{
		Asset:         asset,
		Source:        "binance:" + strings.ToLower(ev.Symbol),
		Last:          last,
		ChangePercent: parseFloatOr(ev.PriceChange, 0),
		High24h:       parseFloatOr(ev.High, 0),
		Low24h:        parseFloatOr(ev.Low, 0),
		QuoteVolume:   parseFloatOr(ev.QuoteVolume, 0),
		TimestampMs:   time.Now().UnixMilli(),
	}, true
}

// streamURL builds the combined-stream URL, e.g.
// wss://.../stream?streams=btcusdt@ticker/ethusdt@ticker
func (c *Client) streamURL() string {
	parts := make([]string, 0, len(c.streamMap))
	for sym := range c.streamMap {
		parts = append(parts, strings.ToLower(sym)+"@ticker")
	}
	return wsBase + "?streams=" + strings.Join(parts, "/")
}

func parseFloatOr(n json.Number, def float64) float64 {
	if f, err := strconv.ParseFloat(n.String(), 64); err == nil {
		return f
	}
	return def
}
