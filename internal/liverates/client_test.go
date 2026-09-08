package liverates

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dex/price-fetcher/internal/price"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// normSym: provider replies like "EUR/USD" must match requested "EURUSD";
// matching is case-insensitive even though published names preserve case.
func TestNormSym(t *testing.T) {
	cases := map[string]string{
		"EURUSD":     "EURUSD",
		"EUR/USD":    "EURUSD",
		"aapl.us":    "AAPL.US",
		"AAPL.us":    "AAPL.US",
		" CrudeOIL ": "CRUDEOIL",
	}
	for in, want := range cases {
		if got := normSym(in); got != want {
			t.Errorf("normSym(%q) = %q, want %q", in, got, want)
		}
	}
}

// numOr must accept string-encoded numbers (the provider's format), bare
// numbers, and tolerate "n/a"/garbage by returning the default.
func TestNumOr(t *testing.T) {
	cases := []struct {
		raw  string // JSON-encoded raw value
		def  float64
		want float64
	}{
		{`"1.16755"`, -1, 1.16755},
		{`158.972`, -1, 158.972},
		{`"n/a"`, -1, -1},
		{`""`, -1, -1},
		{`null`, -1, -1},
		{`" 4603.57 "`, -1, 4603.57},
		{`"abc"`, 7, 7},
		{``, 3.5, 3.5},
	}
	for _, c := range cases {
		if got := numOr([]byte(c.raw), c.def); got != c.want {
			t.Errorf("numOr(%s, %v) = %v, want %v", c.raw, c.def, got, c.want)
		}
	}
}

// normalize: mid price from bid/ask, change vs open, asset mapped through
// lookup, source tagged, stock-style "n/a" close ignored.
func TestNormalizeMidPrice(t *testing.T) {
	c := New([]string{"EURUSD", "GOLD"}, "k", "", time.Second, testLogger())

	body := []byte(`{
		"currency": "EUR/USD",
		"rate": "1.10",
		"bid": "1.10000",
		"ask": "1.10004",
		"high": "1.11",
		"low": "1.09",
		"open": "1.0880",
		"close": "1.10",
		"timestamp": "1787345877456"
	}`)
	var q quote
	if err := json.Unmarshal(body, &q); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got, ok := c.normalize(q)
	if !ok {
		t.Fatal("normalize returned ok=false")
	}
	if got.Asset != "EURUSD" {
		t.Errorf("Asset = %q, want EURUSD", got.Asset)
	}
	if got.Source != "liverates:EURUSD" {
		t.Errorf("Source = %q", got.Source)
	}
	if want := (1.10000 + 1.10004) / 2; got.Last != want {
		t.Errorf("Last = %v, want %v", got.Last, want)
	}
	wantPct := ((1.10002 - 1.0880) / 1.0880) * 100
	if diff := got.ChangePercent - wantPct; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("ChangePercent = %v, want ~%v", got.ChangePercent, wantPct)
	}
	if got.High24h != 1.11 || got.Low24h != 1.09 {
		t.Errorf("High/Low = %v/%v", got.High24h, got.Low24h)
	}
	if got.TimestampMs <= 0 {
		t.Errorf("TimestampMs not set: %v", got.TimestampMs)
	}
}

// A reply currency we didn't request must be dropped, and a quote with no
// usable price side must be skipped.
func TestNormalizeRejects(t *testing.T) {
	c := New([]string{"GOLD"}, "k", "", time.Second, testLogger())

	var unknown quote
	_ = json.Unmarshal([]byte(`{"currency":"XYZ/USD","bid":"1","ask":"1.01","open":"1"}`), &unknown)
	if _, ok := c.normalize(unknown); ok {
		t.Error("unknown currency should be rejected")
	}

	var unusable quote
	_ = json.Unmarshal([]byte(`{"currency":"GOLD","bid":"n/a","ask":"","rate":""}`), &unusable)
	if _, ok := c.normalize(unusable); ok {
		t.Error("quote without usable price should be rejected")
	}

	var empty quote
	if _, ok := c.normalize(empty); ok {
		t.Error("empty quote should be rejected")
	}
}

// The provider returns errors INSIDE a 200 as [{"error":"..."}] — fetch must
// surface that as an error, not an empty quote list.
func TestFetchDetectsErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"error":"Invalid Authentication"}]`))
	}))
	defer srv.Close()

	c := New([]string{"EURUSD"}, "bad-key", srv.URL, time.Second, testLogger())
	if _, err := c.fetch(context.Background()); err == nil {
		t.Fatal("fetch should fail on error envelope, got nil")
	}
}

// Happy path over HTTP: query params carry key + exact-case CSV rate list,
// and the payload decodes into quotes.
func TestFetchQuotes(t *testing.T) {
	var gotKey, gotRate string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.URL.Query().Get("key")
		gotRate = r.URL.Query().Get("rate")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"currency":"GOLD","rate":"4603.57","bid":"4603.57","ask":"4604.61","high":"4632.09","low":"4508.83","open":"4525.96","close":"4603.57","timestamp":"1787345939898"},
			{"currency":"AAPL.us","rate":"309.42","bid":"309.31","ask":"309.53","high":"312.61","low":"306.89","open":"309.42","close":"n/a","timestamp":"1787468280427"}
		]`))
	}))
	defer srv.Close()

	instruments := []string{"GOLD", "AAPL.us"}
	c := New(instruments, "f718db9b4d", srv.URL, time.Second, testLogger())
	quotes, err := c.fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if gotKey != "f718db9b4d" {
		t.Errorf("key param = %q", gotKey)
	}
	if gotRate != "GOLD,AAPL.us" {
		t.Errorf("rate param = %q, want exact-case CSV", gotRate)
	}
	if len(quotes) != 2 {
		t.Fatalf("got %d quotes, want 2", len(quotes))
	}

	// End-to-end through pollOnce: both instruments publish under our labels.
	published := map[string]float64{}
	c.pollOnce(context.Background(), func(p price.IndexPrice) { published[p.Asset] = p.Last })
	if len(published) != 2 {
		t.Fatalf("published %d prices (%v), want 2", len(published), published)
	}
	// Mid of 309.31/309.53 — compare with tolerance for float rounding.
	if want := (309.31 + 309.53) / 2; math.Abs(published["AAPL.us"]-want) > 1e-9 {
		t.Errorf("AAPL.us last = %v, want ~%v", published["AAPL.us"], want)
	}
}
