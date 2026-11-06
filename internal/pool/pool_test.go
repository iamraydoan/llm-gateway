package pool

import (
	"context"
	"net/http"
	"testing"
	"time"

	"llm-gateway/internal/config"
)

func testPool(t *testing.T, keys []config.Key) *Pool {
	t.Helper()
	cfg := &config.Config{
		Upstream: config.Upstream{BaseURL: "http://127.0.0.1:9", TimeoutSeconds: 5},
		Keys:     keys,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	return p
}

func TestPriorityOrder(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k2", APIKey: "sk-2", Priority: 2, Enabled: true},
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
		{ID: "k3", APIKey: "sk-3", Priority: 3, Enabled: true},
	})

	k, release, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if k.ID() != "k1" {
		t.Fatalf("expected k1, got %s", k.ID())
	}
	release()
}

func TestSkipDisabledAndCooldown(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: false},
		{ID: "k2", APIKey: "sk-2", Priority: 2, Enabled: true},
	})
	k, release, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if k.ID() != "k2" {
		t.Fatalf("expected k2 (k1 disabled), got %s", k.ID())
	}
	release()

	k2 := p.Find("k2")
	k2.ReportFailure(429, 0, false, time.Time{}, "rate_limit_exceeded: Rate limit reached")
	if _, _, err := p.Acquire(context.Background()); err == nil {
		t.Fatalf("expected no key available while k2 cools down")
	}
}

func TestConcurrencyLimit(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true, MaxConcurrency: 1},
		{ID: "k2", APIKey: "sk-2", Priority: 2, Enabled: true, MaxConcurrency: 5},
	})
	k1, rel1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if k1.ID() != "k1" {
		t.Fatalf("expected k1 first, got %s", k1.ID())
	}

	k2, rel2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if k2.ID() != "k2" {
		t.Fatalf("expected k2 while k1 busy, got %s", k2.ID())
	}
	rel1()
	rel2()

	k3, rel3, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if k3.ID() != "k1" {
		t.Fatalf("expected k1 after release, got %s", k3.ID())
	}
	rel3()
}

func TestMultiWindowRateLimit(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true, RateLimits: []config.RateLimitRule{
			{Max: 2, Window: "1m"},
			{Max: 3, Window: "1h"},
		}},
		{ID: "k2", APIKey: "sk-2", Priority: 2, Enabled: true},
	})

	for i := 0; i < 2; i++ {
		k, rel, err := p.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		if k.ID() != "k1" {
			t.Fatalf("acquire %d: expected k1, got %s", i, k.ID())
		}
		rel()
	}

	k, rel, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if k.ID() != "k2" {
		t.Fatalf("expected k2 after k1 rate-limited, got %s", k.ID())
	}
	rel()
}

func TestBackoffGrows(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
	})
	k := p.Find("k1")
	k.ReportFailure(429, 0, false, time.Time{}, "rate_limit_exceeded: Rate limit reached")
	first := k.cooldownUntil
	k.ReportFailure(429, 0, false, time.Time{}, "rate_limit_exceeded: Rate limit reached")
	second := k.cooldownUntil
	if !second.After(first) {
		t.Fatalf("backoff did not grow: first=%v second=%v", first, second)
	}

	k.ReportSuccess()
	k.ReportFailure(429, 0, false, time.Time{}, "rate_limit_exceeded: Rate limit reached")
	third := k.cooldownUntil
	if !third.Before(second) {
		t.Fatalf("backoff did not reset after success: %v vs %v", third, second)
	}
}

func TestQuotaExhaustionSkipsUntilReset(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
		{ID: "k2", APIKey: "sk-2", Priority: 2, Enabled: true},
	})
	k1 := p.Find("k1")
	reset := time.Now().Add(30 * time.Minute)
	k1.ReportFailure(429, 0, true, reset, "insufficient_quota: You exceeded your current quota")
	k, rel, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if k.ID() != "k2" {
		t.Fatalf("expected k2 while k1 quota-exhausted, got %s", k.ID())
	}
	rel()
}

func TestParseRetryAfter(t *testing.T) {
	if d := ParseRetryAfter("120"); d != 120*time.Second {
		t.Fatalf("seconds: got %v", d)
	}
	future := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	if d := ParseRetryAfter(future); d <= 0 || d > 31*time.Second {
		t.Fatalf("http date: got %v", d)
	}
	if d := ParseRetryAfter("garbage"); d != 0 {
		t.Fatalf("garbage should be 0, got %v", d)
	}
}

func TestParseResetDuration(t *testing.T) {
	if d := ParseResetDuration("6m0s"); d != 6*time.Minute {
		t.Fatalf("got %v", d)
	}
	if d := ParseResetDuration("20"); d != 20*time.Second {
		t.Fatalf("plain seconds: got %v", d)
	}
}

func TestSyncPreservesState(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", Name: "One", APIKey: "sk-1", Priority: 1, Enabled: true},
	})

	k1 := p.Find("k1")
	k1.ReportFailure(429, 0, false, time.Time{}, "rate_limit_exceeded: Rate limit reached")
	if err := p.Sync([]config.Key{
		{ID: "k1", Name: "One-renamed", APIKey: "sk-1", Priority: 1, Enabled: true},
		{ID: "k2", Name: "Two", APIKey: "sk-2", Priority: 2, Enabled: true},
	}); err != nil {
		t.Fatalf("sync: %v", err)
	}

	if got := p.Find("k1"); got == nil || got.Name() != "One-renamed" {
		t.Fatalf("k1 should exist renamed, got %v", got)
	}
	k, rel, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if k.ID() != "k2" {
		t.Fatalf("expected k2 (k1 still cooling down), got %s", k.ID())
	}
	rel()

	if err := p.Sync([]config.Key{
		{ID: "k2", Name: "Two", APIKey: "sk-2", Priority: 2, Enabled: true},
	}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if p.Find("k1") != nil {
		t.Fatalf("k1 should be gone after sync")
	}
}

func TestStatusChangeHook(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
	})
	calls := 0
	p.SetStatusChangeHook(func() { calls++ })
	k := p.Find("k1")
	k.ReportSuccess()
	if calls != 1 {
		t.Fatalf("expected hook after success, got %d calls", calls)
	}
	k.ReportFailure(429, 0, false, time.Time{}, "rate_limit_exceeded: Rate limit reached")
	if calls != 2 {
		t.Fatalf("expected hook after failure, got %d calls", calls)
	}

	if err := p.Sync([]config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
		{ID: "k2", APIKey: "sk-2", Priority: 2, Enabled: true},
	}); err != nil {
		t.Fatalf("sync: %v", err)
	}
	p.Find("k2").ReportFailure(500, 0, false, time.Time{}, "server_error: internal error")
	if calls != 3 {
		t.Fatalf("expected hook for synced key, got %d calls", calls)
	}
}

func TestLastErrorDetail(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
	})
	k := p.Find("k1")
	k.ReportFailure(429, 0, false, time.Time{}, "rate_limit_exceeded: Rate limit reached")
	st := k.Status(time.Now())
	if st.LastError != "rate_limit_exceeded: Rate limit reached" {
		t.Fatalf("last error not stored: %q", st.LastError)
	}
	if st.LastErrorAt == "" {
		t.Fatal("last_error_at missing")
	}
	if st.Fails != 1 {
		t.Fatalf("expected 1 consecutive failure, got %d", st.Fails)
	}
	if st.RetryInSeconds <= 0 {
		t.Fatalf("expected retry_in_seconds > 0, got %d", st.RetryInSeconds)
	}
	k.ReportFailure(429, 0, false, time.Time{}, "rate_limit_exceeded: Rate limit reached")
	if st := k.Status(time.Now()); st.Fails != 2 {
		t.Fatalf("expected 2 consecutive failures, got %d", st.Fails)
	}
	k.ReportSuccess()
	st = k.Status(time.Now())
	if st.LastError != "" || st.Fails != 0 {
		t.Fatalf("success should clear last error, got %q fails %d", st.LastError, st.Fails)
	}
}
