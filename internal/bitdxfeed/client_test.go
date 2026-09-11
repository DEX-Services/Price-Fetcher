package bitdxfeed

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dex/price-fetcher/internal/price"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// parseFloat must accept string-encoded numbers (the feed's actual format)
// and tolerate garbage/empty by returning the default — same defensive
// posture as liverates.numOr, minus the "n/a" placeholder this feed doesn't
// use.
func TestParseFloat(t *testing.T) {
	cases := []struct {
		raw  string
		def  float64
		want float64
	}{
		{"3.29142", -1, 3.29142},
		{" 3.29142 ", -1, 3.29142},
		{"", -1, -1},
		{"abc", 7, 7},
	}
	for _, c := range cases {
		if got := parseFloat(c.raw, c.def); got != c.want {
			t.Errorf("parseFloat(%q, %v) = %v, want %v", c.raw, c.def, got, c.want)
		}
	}
}

// normalize on the exact response shape confirmed live from
// https://bitdx-feed-jk3y.onrender.com/ 2026-09-12:
//
//	{"symbol":"BI2X/BIUSDB","rate":"3.29142","high":"3.29507","low":"3.28551",
//	 "open":"3.29280","close":"3.29142","volume":"0.05","timestamp":"1789155525000"}
func TestNormalize(t *testing.T) {
	c := New("BI2X", "", time.Second, testLogger())
	q := quoteResponse{
		Symbol: "BI2X/BIUSDB", Rate: "3.29142", High: "3.29507", Low: "3.28551",
		Open: "3.29280", Close: "3.29142", Volume: "0.05", Timestamp: "1789155525000",
	}
	p, ok := c.normalize(q)
	if !ok {
		t.Fatal("expected a usable quote")
	}
	if p.Asset != "BI2X" {
		t.Errorf("Asset = %q, want BI2X", p.Asset)
	}
	if p.Source != "bitdxfeed:BI2X" {
		t.Errorf("Source = %q", p.Source)
	}
	if p.Last != 3.29142 {
		t.Errorf("Last = %v, want the rate field's value", p.Last)
	}
	if p.High24h != 3.29507 || p.Low24h != 3.28551 {
		t.Errorf("High24h/Low24h = %v/%v", p.High24h, p.Low24h)
	}
	if p.QuoteVolume != 0.05 {
		t.Errorf("QuoteVolume = %v", p.QuoteVolume)
	}
	// (3.29142 - 3.29280) / 3.29280 * 100
	wantChange := (3.29142 - 3.29280) / 3.29280 * 100
	if diff := p.ChangePercent - wantChange; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("ChangePercent = %v, want %v", p.ChangePercent, wantChange)
	}
	if p.TimestampMs <= 0 {
		t.Error("expected a populated TimestampMs (our own receive time, not the feed's)")
	}
}

// A non-positive or unparseable rate must be rejected outright, never
// published as a zero/negative price a market maker could quote against.
func TestNormalizeRejectsUnusableRate(t *testing.T) {
	c := New("BI2X", "", time.Second, testLogger())
	for _, raw := range []string{"", "0", "-1", "not-a-number"} {
		if _, ok := c.normalize(quoteResponse{Rate: raw}); ok {
			t.Errorf("rate=%q should be rejected, got ok=true", raw)
		}
	}
}

// Happy path over HTTP: the client hits baseURL/ with no query params or
// auth header (unlike liverates), and the flat single-object response
// decodes correctly.
func TestFetchQuote(t *testing.T) {
	var gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"symbol":"BI2X/BIUSDB","rate":"3.29142","high":"3.29507","low":"3.28551","open":"3.29280","close":"3.29142","volume":"0.05","timestamp":"1789155525000"}`))
	}))
	defer srv.Close()

	c := New("BI2X", srv.URL, time.Second, testLogger())
	q, err := c.fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if gotPath != "/" {
		t.Errorf("path = %q, want /", gotPath)
	}
	if gotQuery != "" {
		t.Errorf("query = %q, want no params (this feed takes none)", gotQuery)
	}
	if q.Rate != "3.29142" {
		t.Errorf("Rate = %q", q.Rate)
	}
}

// A non-200 response must surface as an error, not a silently-empty/zero
// quote.
func TestFetchNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("down for maintenance"))
	}))
	defer srv.Close()

	c := New("BI2X", srv.URL, time.Second, testLogger())
	if _, err := c.fetch(context.Background()); err == nil {
		t.Fatal("expected an error on HTTP 503, got nil")
	}
}

// pollOnce end-to-end: a live fetch that decodes and normalizes successfully
// must call onPrice exactly once with the right asset.
func TestPollOnceCallsOnPrice(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"symbol":"BI2X/BIUSDB","rate":"3.30","high":"3.31","low":"3.29","open":"3.29","close":"3.30","volume":"1.0","timestamp":"1789155525000"}`))
	}))
	defer srv.Close()

	c := New("BI2X", srv.URL, time.Second, testLogger())
	calls := 0
	c.pollOnce(context.Background(), func(p price.IndexPrice) {
		calls++
		if p.Last != 3.30 {
			t.Errorf("Last = %v", p.Last)
		}
	})
	if calls != 1 {
		t.Fatalf("onPrice called %d times, want 1", calls)
	}
}
