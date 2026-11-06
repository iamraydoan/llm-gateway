package pool

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"llm-gateway/internal/config"
	"llm-gateway/internal/store"
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

func TestCustomBackoff(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
	})
	p.SetBackoff(2*time.Second, 3, 10*time.Second)
	k := p.Find("k1")
	now := time.Now()
	k.ReportFailure(429, 0, false, time.Time{}, "rate limited")
	if secs := k.Status(now).RetryInSeconds; secs < 1 || secs > 2 {
		t.Fatalf("1st failure retry = %ds, want ~2s", secs)
	}
	k.ReportFailure(429, 0, false, time.Time{}, "rate limited")
	if secs := k.Status(now).RetryInSeconds; secs < 5 || secs > 6 {
		t.Fatalf("2nd failure retry = %ds, want ~6s", secs)
	}
	k.ReportFailure(429, 0, false, time.Time{}, "rate limited")
	if secs := k.Status(now).RetryInSeconds; secs < 9 || secs > 10 {
		t.Fatalf("3rd failure retry = %ds, want ~10s (capped)", secs)
	}
	k.ReportFailure(429, 0, false, time.Time{}, "rate limited")
	if secs := k.Status(now).RetryInSeconds; secs < 9 || secs > 10 {
		t.Fatalf("4th failure retry = %ds, want ~10s (still capped)", secs)
	}
}

func testPoolWithStore(t *testing.T, keys []config.Key) (*Pool, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("store open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	p := testPool(t, keys)
	p.SetStore(st)
	p.LoadState()
	return p, st
}

func reopenPool(t *testing.T, keys []config.Key, st *store.Store) *Pool {
	t.Helper()
	p := testPool(t, keys)
	p.SetStore(st)
	p.LoadState()
	return p
}

func TestPersistKeyState(t *testing.T) {
	keys := []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
		{ID: "k2", APIKey: "sk-2", Priority: 2, Enabled: true},
		{ID: "k3", APIKey: "sk-3", Priority: 3, Enabled: true},
	}
	p1, st := testPoolWithStore(t, keys)
	p1.SetBackoff(5*time.Second, 2, 10*time.Minute)
	k1 := p1.Find("k1")
	k1.ReportFailure(429, 0, false, time.Time{}, "rate limited")
	k1.ReportFailure(429, 0, false, time.Time{}, "rate limited")
	states, err := st.LoadKeyStates()
	if err != nil {
		t.Fatalf("load states: %v", err)
	}
	if states["k1"].Fails != 2 {
		t.Fatalf("db fails = %d, want 2", states["k1"].Fails)
	}
	if states["k1"].CooldownUntil.Before(time.Now()) {
		t.Fatalf("db cooldown_until = %v, want a future time", states["k1"].CooldownUntil)
	}
	if _, ok := states["k2"]; !ok {
		t.Fatal("db has no row for k2, want a row for every key")
	}
	k3 := p1.Find("k3")
	k3.ReportFailure(429, 0, false, time.Time{}, "rate limited")
	k3.ReportSuccess()
	if states, err := st.LoadKeyStates(); err != nil || states["k3"].Fails != 0 || states["k3"].LastError != "" {
		t.Fatalf("db k3 = %+v err %v, want fails 0 and empty last error", states["k3"], err)
	}

	p2 := reopenPool(t, keys, st)
	p2.SetBackoff(5*time.Second, 2, 10*time.Minute)
	rk1 := p2.Find("k1")
	if st := rk1.Status(time.Now()); st.Fails != 2 {
		t.Fatalf("reloaded fails = %d, want 2", st.Fails)
	}
	if rk1.skipReason(time.Now()) != "cooldown" {
		t.Fatalf("reloaded skipReason = %q, want cooldown", rk1.skipReason(time.Now()))
	}
	k, rel, err := p2.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if k.ID() != "k2" {
		t.Fatalf("acquire returned %s, want k2 (k1 still cooling down)", k.ID())
	}
	rel()
}

func TestPersistQuotaState(t *testing.T) {
	keys := []config.Key{{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true}}
	p1, st := testPoolWithStore(t, keys)
	now := time.Now()
	reset := now.Add(72 * time.Hour).UTC()
	body := fmt.Sprintf(`{"error":{"message":"You've used this period's free allowance. Your next rolling 7-day period starts on %s. Use the paid model to keep going.","type":"free_tier_limit_reached","code":"free_tier_limit_reached"}}`, reset.Format("2 Jan 2006 at 15:04 MST"))
	p1.Find("k1").ReportFailure(429, 0, true, reset, body)
	states, err := st.LoadKeyStates()
	if err != nil {
		t.Fatalf("load states: %v", err)
	}
	if states["k1"].QuotaResetAt.IsZero() {
		t.Fatal("db quota_reset_at empty, want the reset date")
	}

	p2 := reopenPool(t, keys, st)
	if st := p2.Find("k1").Status(now); st.QuotaResetAt == "" {
		t.Fatal("reloaded QuotaResetAt empty, want the reset date")
	}
	if _, _, err := p2.Acquire(context.Background()); err == nil {
		t.Fatal("acquire should fail while the key is quota-exhausted")
	}
}

func TestPersistRateHits(t *testing.T) {
	keys := []config.Key{{
		ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true,
		RateLimits: []config.RateLimitRule{{Max: 1, Window: "1h"}},
	}}
	p1, st := testPoolWithStore(t, keys)
	if _, rel, err := p1.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire: %v", err)
	} else {
		rel()
	}
	hits, err := st.LoadRateHits("k1", 3600, time.Now().Add(-time.Hour))
	if err != nil || len(hits) != 1 {
		t.Fatalf("db hits = %d err %v, want 1", len(hits), err)
	}

	p2 := reopenPool(t, keys, st)
	if _, _, err := p2.Acquire(context.Background()); err == nil {
		t.Fatal("acquire should fail, the 1/h unit is still spent after reload")
	}
	if p2.Find("k1").skipReason(time.Now()) != "rate-limited" {
		t.Fatalf("skipReason = %q, want rate-limited", p2.Find("k1").skipReason(time.Now()))
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

func TestParseFreeTierReset(t *testing.T) {
	msg := "You've used this period's free allowance. Your next rolling 7-day period starts on 10 Nov 2026 at 02:38 UTC. Use the paid model 'deepseek-v4-flash' to keep going."
	got := ParseFreeTierReset(msg)
	want := time.Date(2026, time.November, 10, 2, 38, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got := ParseFreeTierReset("no reset date here"); !got.IsZero() {
		t.Fatalf("no-date message should give zero time, got %v", got)
	}
	if got := ParseFreeTierReset("starts on 32 Foo 2026 at 99:99 UTC"); !got.IsZero() {
		t.Fatalf("bad date should give zero time, got %v", got)
	}
}

func TestIsQuotaErrorFreeTier(t *testing.T) {
	body := `{"error":{"message":"You've used this period's free allowance. Your next rolling 7-day period starts on 10 Nov 2026 at 02:38 UTC.","type":"free_tier_limit_reached","code":"free_tier_limit_reached"}}`
	if !IsQuotaError(body) {
		t.Fatal("free tier limit body should be a quota error")
	}
}

func TestMatchErrorRules(t *testing.T) {
	rules := []config.ErrorRule{
		{Path: "error.code", Value: "server_busy", DelaySeconds: 5, CooldownSeconds: 300},
		{Path: "error.type", Value: "rate_limit_error", DelaySeconds: 1},
	}
	busy := []byte(`{"error":{"message":"Token Harbor is under heavy load.","type":"rate_limit_error","code":"server_busy"}}`)
	d, c, ok := MatchErrorRules(busy, rules)
	if !ok || d != 5*time.Second || c != 300*time.Second {
		t.Fatalf("server_busy: got delay %v cooldown %v matched %v, want 5s 300s true", d, c, ok)
	}
	other := []byte(`{"error":{"message":"slow down","type":"rate_limit_error","code":"other"}}`)
	d, c, ok = MatchErrorRules(other, rules)
	if !ok || d != time.Second || c != 0 {
		t.Fatalf("type-only match: got delay %v cooldown %v matched %v, want 1s 0 true", d, c, ok)
	}
	nomatch := []byte(`{"error":{"message":"boom","type":"server_error","code":"internal"}}`)
	if _, _, ok := MatchErrorRules(nomatch, rules); ok {
		t.Fatal("unrelated error should not match")
	}
	if _, _, ok := MatchErrorRules([]byte(`not json`), rules); ok {
		t.Fatal("non-JSON body should not match")
	}
	num := []byte(`{"error":{"status":429}}`)
	d, _, ok = MatchErrorRules(num, []config.ErrorRule{{Path: "error.status", Value: "429", DelaySeconds: 2}})
	if !ok || d != 2*time.Second {
		t.Fatalf("numeric value: got delay %v matched %v, want 2s true", d, ok)
	}
}

func TestExtendCooldown(t *testing.T) {
	p := testPool(t, []config.Key{
		{ID: "k1", APIKey: "sk-1", Priority: 1, Enabled: true},
	})
	k := p.Find("k1")
	now := time.Now()
	k.ReportFailure(500, 0, false, time.Time{}, "boom")
	k.ExtendCooldown(60 * time.Second)
	if secs := k.Status(now).RetryInSeconds; secs < 55 || secs > 60 {
		t.Fatalf("extend to 60s: got retry in %ds", secs)
	}
	k.ExtendCooldown(time.Second)
	if secs := k.Status(now).RetryInSeconds; secs < 55 || secs > 60 {
		t.Fatalf("shorter extend must not shorten cooldown, got retry in %ds", secs)
	}
}
