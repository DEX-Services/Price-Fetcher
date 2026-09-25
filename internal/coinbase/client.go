// Package coinbase streams live ticker data from Coinbase Exchange's public
// WebSocket feed and emits normalized price.IndexPrice values.
//
// Swapped in for internal/binance (2026-09-25): Binance's WebSocket gateway
// began rejecting the handshake outright ("bad handshake", no data at all)
// from this service's hosting provider's outbound IPs, while working fine
// from other networks — a cloud/datacenter IP block on Binance's side, not
// a bug in the dialer code (confirmed by testing the exact same handshake
// directly from a non-blocked network, which succeeded and streamed data
// normally). Coinbase Exchange's public feed needs no API key/auth for
// ticker data and has not shown this restriction.
//
// Same shape as internal/binance's Client (New/Run) so cmd/fetcher/main.go's
// wiring only needed a one-line swap, not a redesign.
package coinbase

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
	// wsURL is Coinbase Exchange's public WebSocket feed. Subscription is a
	// message sent after connecting (unlike Binance's URL-embedded stream
	// list), so every asset shares one connection regardless of count.
	wsURL = "wss://ws-feed.exchange.coinbase.com"

	// writeWait / pongWait / pingPeriod keep the connection healthy. Coinbase
	// sends its own periodic "heartbeat"-less ticker traffic, but an idle
	// market (or a dead connection) still needs an app-level ping to detect
	// promptly — same posture as internal/binance.
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 30 * time.Second

	// reconnectMin / reconnectMax bound the exponential backoff between retries.
	reconnectMin = 1 * time.Second
	reconnectMax = 30 * time.Second
)

// tickerMessage is the subset of Coinbase's "ticker" channel message we care
// about. Every numeric field is JSON-string-encoded.
type tickerMessage struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Open24h   string `json:"open_24h"`
	High24h   string `json:"high_24h"`
	Low24h    string `json:"low_24h"`
	Volume24h string `json:"volume_24h"`
}

// Client streams ticker updates for a set of assets from Coinbase Exchange.
type Client struct {
	assets     []string          // base symbols, e.g. ["BTC","ETH"]
	quote      string            // e.g. "USD" — Coinbase has no USDT market for every asset, so this is fixed to USD regardless of the configured quote (see New)
	productMap map[string]string // "BTC-USD" -> "BTC"
	log        *slog.Logger
}

// New builds a Client for the given base assets. quote is accepted for
// interface parity with binance.New but is otherwise ignored: Coinbase
// Exchange's spot markets are USD-quoted (not USDT), and BI2XUSD is pegged
// close enough to USD that this substitution doesn't need its own config
// knob — see BitDx's existing USDT/USDC/BI2XUSD 1:1 peg conventions
// elsewhere in this codebase.
func New(assets []string, quote string, log *slog.Logger) *Client {
	_ = quote
	productMap := make(map[string]string, len(assets))
	for _, a := range assets {
		a = strings.ToUpper(a)
		productMap[a+"-USD"] = a
	}
	return &Client{assets: assets, quote: "USD", productMap: productMap, log: log}
}

// Run connects and streams until ctx is cancelled, reconnecting with backoff
// on any error. Each normalized update is delivered to onPrice. onPrice must
// be safe to call from this goroutine and should not block for long.
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
			c.log.Warn("coinbase stream ended, reconnecting", "err", err, "backoff", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > reconnectMax {
			backoff = reconnectMax
		}
	}
}

// connectAndStream runs a single connection lifecycle: connect, subscribe,
// then read until the connection drops or ctx is cancelled.
func (c *Client) connectAndStream(ctx context.Context, onPrice func(price.IndexPrice)) error {
	c.log.Info("connecting to coinbase", "products", len(c.productMap))

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	productIDs := make([]string, 0, len(c.productMap))
	for id := range c.productMap {
		productIDs = append(productIDs, id)
	}
	sub := map[string]any{
		"type":        "subscribe",
		"product_ids": productIDs,
		"channels":    []string{"ticker"},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

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

		var msg tickerMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			c.log.Debug("skip unparsable frame", "err", err)
			continue
		}
		if msg.Type != "ticker" {
			continue // subscriptions ack, heartbeats, errors, etc.
		}
		asset, ok := c.productMap[msg.ProductID]
		if !ok {
			continue
		}
		p, ok := c.normalize(asset, msg)
		if !ok {
			continue
		}
		onPrice(p)
	}
}

// pingLoop sends periodic WebSocket pings so a dead peer is detected via the
// read deadline even when Coinbase is quiet.
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

// normalize converts a raw Coinbase ticker into an IndexPrice, dropping
// updates with an unparsable or non-positive last price. ChangePercent is
// computed from open_24h since Coinbase's ticker message doesn't include it
// directly (unlike Binance's "P" field).
func (c *Client) normalize(asset string, msg tickerMessage) (price.IndexPrice, bool) {
	last, err := strconv.ParseFloat(msg.Price, 64)
	if err != nil || last <= 0 {
		return price.IndexPrice{}, false
	}
	open := parseFloatOr(msg.Open24h, 0)
	changePercent := 0.0
	if open > 0 {
		changePercent = (last - open) / open * 100
	}
	baseVolume := parseFloatOr(msg.Volume24h, 0)
	return price.IndexPrice{
		Asset:         asset,
		Source:        "coinbase:" + strings.ToLower(asset) + "-usd",
		Last:          last,
		ChangePercent: changePercent,
		High24h:       parseFloatOr(msg.High24h, 0),
		Low24h:        parseFloatOr(msg.Low24h, 0),
		QuoteVolume:   baseVolume * last,
		TimestampMs:   time.Now().UnixMilli(),
	}, true
}

func parseFloatOr(s string, def float64) float64 {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return def
}
