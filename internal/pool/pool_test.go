package pool

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
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

func TestStatusChangeHook(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
		{ID: "k2", APIKey: "sk-2", Priority: 2, Enabled: true},
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

	p.Find("k2").ReportFailure(500, 0, false, time.Time{}, "server_error: internal error")
	if calls != 3 {
		t.Fatalf("expected hook after k2 failure, got %d calls", calls)
	}
}

func TestLastErrorDetail(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
	})
	k := p.Find("k1")
	k.ReportFailure(429, 0, false, time.Time{}, `{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}`)
	st := k.Status(time.Now())
	if st.LastError != `{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}` {
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
	k.ReportFailure(429, 0, false, time.Time{}, `{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}`)
	if st := k.Status(time.Now()); st.Fails != 2 {
		t.Fatalf("expected 2 consecutive failures, got %d", st.Fails)
	}
	k.ReportSuccess()
	st = k.Status(time.Now())
	if st.LastError != "" || st.Fails != 0 {
		t.Fatalf("success should clear last error, got %q fails %d", st.LastError, st.Fails)
	}
}

func TestReportFailureBackoffMax(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
	})
	k := p.Find("k1")
	now := time.Now()
	k.ReportFailure(429, 60*time.Second, false, time.Time{}, "rate limited")
	if secs := k.Status(now).RetryInSeconds; secs < 59 || secs > 60 {
		t.Fatalf("Retry-After 60s should win over 5s base backoff, got retry in %ds", secs)
	}
	k.ReportSuccess()
	k.ReportFailure(429, 0, true, now.Add(5*time.Second), "quota")
	if secs := k.Status(now).RetryInSeconds; secs < 4 || secs > 5 {
		t.Fatalf("quota reset 5s should win over 5s backoff tie, got retry in %ds", secs)
	}
	k.ReportFailure(429, 0, true, now.Add(5*time.Second), "quota")
	if secs := k.Status(now).RetryInSeconds; secs < 9 || secs > 10 {
		t.Fatalf("2nd failure backoff 10s should win over quota reset 5s, got retry in %ds", secs)
	}
}

func TestConcurrentAcquireHoldsLimit(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true, MaxConcurrency: 3},
	})
	var active, maxActive, failed int32
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, rel, err := p.Acquire(context.Background())
			if err != nil {
				atomic.AddInt32(&failed, 1)
				return
			}
			cur := atomic.AddInt32(&active, 1)
			mu.Lock()
			if cur > maxActive {
				maxActive = cur
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&active, -1)
			rel()
		}()
	}
	wg.Wait()
	if maxActive != 3 {
		t.Fatalf("max simultaneous holders = %d, want exactly 3 (cap reached, never exceeded)", maxActive)
	}
	if failed == 0 {
		t.Fatalf("expected some Acquire calls to fail while slots are full, got 0 failures")
	}
}

func TestSlidingWindowExpiry(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true, RateLimits: []config.RateLimitRule{
			{Max: 2, Window: "1m"},
		}},
	})
	k := p.Find("k1")
	now := time.Now()
	rel1 := k.tryReserve(now)
	if rel1 == nil {
		t.Fatal("reserve 1 should succeed")
	}
	rel2 := k.tryReserve(now)
	if rel2 == nil {
		t.Fatal("reserve 2 should succeed")
	}
	if rel := k.tryReserve(now); rel != nil {
		t.Fatal("reserve 3 should fail inside the 2/min window")
	}
	rel1()
	rel2()
	if rel := k.tryReserve(now.Add(61 * time.Second)); rel == nil {
		t.Fatal("reserve after window expiry should succeed")
	} else {
		rel()
	}
}
