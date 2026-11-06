package pool

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"llm-gateway/internal/config"
)

const (
	baseBackoff = 5 * time.Second
	maxBackoff  = 10 * time.Minute
)

type ErrNoKeyAvailable struct {
	RetryAfter time.Duration
	Reason     string
}

func (e *ErrNoKeyAvailable) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("no key available (%s), retry in %s", e.Reason, e.RetryAfter.Round(time.Second))
	}
	return fmt.Sprintf("no key available (%s)", e.Reason)
}

type slidingWindow struct {
	max      int
	duration time.Duration
	hits     []time.Time
}

func (w *slidingWindow) allow(now time.Time) bool {
	cutoff := now.Add(-w.duration)

	i := 0
	for i < len(w.hits) && !w.hits[i].After(cutoff) {
		i++
	}
	if i > 0 {
		w.hits = append([]time.Time(nil), w.hits[i:]...)
	}
	if len(w.hits) >= w.max {
		return false
	}
	w.hits = append(w.hits, now)
	return true
}

func (w *slidingWindow) rollback() {
	if len(w.hits) > 0 {
		w.hits = w.hits[:len(w.hits)-1]
	}
}

func (w *slidingWindow) remaining(now time.Time) int {
	cutoff := now.Add(-w.duration)
	n := 0
	for _, t := range w.hits {
		if t.After(cutoff) {
			n++
		}
	}
	left := w.max - n
	if left < 0 {
		left = 0
	}
	return left
}

func (w *slidingWindow) resetAt(now time.Time) time.Time {
	cutoff := now.Add(-w.duration)
	for _, t := range w.hits {
		if t.After(cutoff) {
			return t.Add(w.duration)
		}
	}
	return time.Time{}
}

type Key struct {
	cfg config.Key

	mu            sync.Mutex
	sem           chan struct{} // concurrency semaphore, nil = unlimited
	windows       []*slidingWindow
	cooldownUntil time.Time
	quotaResetAt  time.Time
	fails         int // consecutive failures, drives exponential backoff
	inFlight      int

	TotalRequests int
	TotalErrors   int
	LastError     string
	LastErrorAt   time.Time
	LastUsedAt    time.Time

	// notify is called (after unlocking) when the key's live state changes
	// (success / failure). Set by the pool; used for realtime pushes.
	notify func()

	// one http.Client per key so each key can use its own proxy
	client *http.Client
}

func newKey(cfg config.Key, timeout time.Duration) (*Key, error) {
	k := &Key{cfg: cfg}
	if cfg.MaxConcurrency > 0 {
		k.sem = make(chan struct{}, cfg.MaxConcurrency)
	}
	for _, r := range cfg.RateLimits {
		k.windows = append(k.windows, &slidingWindow{max: r.Max, duration: r.Duration})
	}
	client, err := buildClient(cfg.Proxy, timeout)
	if err != nil {
		return nil, fmt.Errorf("key %q: %w", cfg.ID, err)
	}
	k.client = client
	return k, nil
}

func (k *Key) ID() string { return k.cfg.ID }

func (k *Key) Name() string { return k.cfg.Name }

func (k *Key) Config() config.Key {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.cfg
}

func (k *Key) Priority() int { return k.cfg.Priority }

func (k *Key) Proxy() string { return k.cfg.Proxy }

func (k *Key) APIKey() string { return k.cfg.APIKey }

func (k *Key) Client() *http.Client { return k.client }

func (k *Key) Enabled() bool { return k.cfg.Enabled }

func (k *Key) SetEnabled(v bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.cfg.Enabled = v
}

// tryReserve attempts to reserve one concurrency slot + one hit in every
// rate-limit window. Returns a release func on success (nil on failure);
// the caller must call it when the request finishes. The release func
// captures the semaphore channel in use at reserve time, so rebuilding the
// semaphore (e.g. after a config change) cannot steal another request's slot.
func (k *Key) tryReserve(now time.Time) func() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.cfg.Enabled {
		return nil
	}
	if now.Before(k.cooldownUntil) {
		return nil
	}
	if !k.quotaResetAt.IsZero() && now.Before(k.quotaResetAt) {
		return nil
	}

	// rate limits first (cheap), then semaphore
	reserved := 0
	for _, w := range k.windows {
		if !w.allow(now) {
			for i := 0; i < reserved; i++ {
				k.windows[i].rollback()
			}
			return nil
		}
		reserved++
	}
	sem := k.sem
	if sem != nil {
		select {
		case sem <- struct{}{}:
		default:
			for _, w := range k.windows {
				w.rollback()
			}
			return nil
		}
	}
	k.inFlight++
	k.LastUsedAt = now
	return func() {
		if sem != nil {
			select {
			case <-sem:
			default:
			}
		}
		k.mu.Lock()
		defer k.mu.Unlock()
		if k.inFlight > 0 {
			k.inFlight--
		}
	}
}

func (k *Key) skipReason(now time.Time) string {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.cfg.Enabled {
		return "disabled"
	}
	if now.Before(k.cooldownUntil) {
		return "cooldown"
	}
	if !k.quotaResetAt.IsZero() && now.Before(k.quotaResetAt) {
		return "quota-exhausted"
	}
	for _, w := range k.windows {
		if w.remaining(now) <= 0 {
			return "rate-limited"
		}
	}
	if k.sem != nil && len(k.sem) >= cap(k.sem) {
		return "busy"
	}
	return ""
}

func (k *Key) usableAtLocked(now time.Time) time.Time {
	var best time.Time
	consider := func(t time.Time) {
		if t.IsZero() || !t.After(now) {
			return
		}
		if t.After(best) {
			best = t
		}
	}
	consider(k.cooldownUntil)
	consider(k.quotaResetAt)
	for _, w := range k.windows {
		if w.remaining(now) <= 0 {
			consider(w.resetAt(now))
		}
	}
	return best
}

func (k *Key) earliestRetry(now time.Time) time.Time {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.earliestRetryLocked(now)
}

func (k *Key) earliestRetryLocked(now time.Time) time.Time {
	var best time.Time
	consider := func(t time.Time) {
		if t.IsZero() || !t.After(now) {
			return
		}
		if best.IsZero() || t.Before(best) {
			best = t
		}
	}
	consider(k.cooldownUntil)
	consider(k.quotaResetAt)
	for _, w := range k.windows {
		consider(w.resetAt(now))
	}
	return best
}

func (k *Key) ReportSuccess() {
	k.mu.Lock()
	k.TotalRequests++
	k.fails = 0
	k.LastError = ""
	k.LastErrorAt = time.Time{}
	notify := k.notify
	k.mu.Unlock()
	if notify != nil {
		notify()
	}
}

func (k *Key) ReportFailure(status int, retryAfter time.Duration, quotaExhausted bool, quotaReset time.Time, detail string) {
	k.mu.Lock()
	k.TotalErrors++
	k.fails++
	if detail == "" {
		detail = fmt.Sprintf("upstream %d", status)
	}
	k.LastError = detail
	now := time.Now()
	k.LastErrorAt = now
	delay := backoffDelay(k.fails)
	if retryAfter > delay {
		delay = retryAfter
	}
	if !quotaReset.IsZero() {
		if d := time.Until(quotaReset); d > delay {
			delay = d
		}
	}
	k.cooldownUntil = now.Add(delay)
	if quotaExhausted {
		k.quotaResetAt = quotaReset
		if k.quotaResetAt.IsZero() {
			k.quotaResetAt = k.cooldownUntil
		}
	}
	notify := k.notify
	k.mu.Unlock()
	if notify != nil {
		notify()
	}
}

func backoffDelay(fails int) time.Duration {
	if fails <= 0 {
		return baseBackoff
	}
	d := baseBackoff
	for i := 1; i < fails; i++ {
		d *= 2
		if d >= maxBackoff {
			return maxBackoff
		}
	}
	return d
}

type Status struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	APIKeyMasked   string            `json:"api_key_masked"`
	Priority       int               `json:"priority"`
	Enabled        bool              `json:"enabled"`
	Proxy          string            `json:"proxy"`
	MaxConcurrency int               `json:"max_concurrency"`
	InFlight       int               `json:"in_flight"`
	CooldownUntil  string            `json:"cooldown_until,omitempty"`
	QuotaResetAt   string            `json:"quota_reset_at,omitempty"`
	TotalRequests  int               `json:"total_requests"`
	TotalErrors    int               `json:"total_errors"`
	LastError      string            `json:"last_error,omitempty"`
	LastErrorAt    string            `json:"last_error_at,omitempty"`
	Fails          int               `json:"consecutive_failures"`
	RetryInSeconds int               `json:"retry_in_seconds,omitempty"`
	SkipReason     string            `json:"skip_reason,omitempty"`
	RateLimits     []RateLimitStatus `json:"rate_limits"`
}

type RateLimitStatus struct {
	Max       int    `json:"max"`
	Window    string `json:"window"`
	Remaining int    `json:"remaining"`
}

func (k *Key) Status(now time.Time) Status {
	k.mu.Lock()
	defer k.mu.Unlock()
	s := Status{
		ID:             k.cfg.ID,
		Name:           k.cfg.Name,
		APIKeyMasked:   k.cfg.MaskedAPIKey(),
		Priority:       k.cfg.Priority,
		Enabled:        k.cfg.Enabled,
		Proxy:          k.cfg.Proxy,
		MaxConcurrency: k.cfg.MaxConcurrency,
		InFlight:       k.inFlight,
		TotalRequests:  k.TotalRequests,
		TotalErrors:    k.TotalErrors,
		LastError:      k.LastError,
		Fails:          k.fails,
	}
	if !k.LastErrorAt.IsZero() {
		s.LastErrorAt = k.LastErrorAt.Format(time.RFC3339)
	}
	if next := k.usableAtLocked(now); !next.IsZero() {
		if secs := int(time.Until(next).Seconds()); secs > 0 {
			s.RetryInSeconds = secs
		}
	}
	if !k.cooldownUntil.IsZero() && now.Before(k.cooldownUntil) {
		s.CooldownUntil = k.cooldownUntil.Format(time.RFC3339)
	}
	if !k.quotaResetAt.IsZero() && now.Before(k.quotaResetAt) {
		s.QuotaResetAt = k.quotaResetAt.Format(time.RFC3339)
	}
	for i, w := range k.windows {
		s.RateLimits = append(s.RateLimits, RateLimitStatus{
			Max:       w.max,
			Window:    k.cfg.RateLimits[i].Window,
			Remaining: w.remaining(now),
		})
	}
	return s
}

type Pool struct {
	mu      sync.RWMutex
	keys    []*Key
	timeout time.Duration

	onStatusChange func()
}

func (p *Pool) SetStatusChangeHook(fn func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onStatusChange = fn
	for _, k := range p.keys {
		k.mu.Lock()
		k.notify = fn
		k.mu.Unlock()
	}
}

func New(cfg *config.Config) (*Pool, error) {
	return NewFromKeys(cfg.Keys, time.Duration(cfg.Upstream.TimeoutSeconds)*time.Second)
}

func NewFromKeys(keys []config.Key, timeout time.Duration) (*Pool, error) {
	ks := make([]*Key, 0, len(keys))
	for _, kc := range keys {
		k, err := newKey(kc, timeout)
		if err != nil {
			return nil, err
		}
		ks = append(ks, k)
	}
	sort.SliceStable(ks, func(i, j int) bool {
		return ks[i].cfg.Priority < ks[j].cfg.Priority
	})
	return &Pool{keys: ks, timeout: timeout}, nil
}

func buildClient(proxy string, timeout time.Duration) (*http.Client, error) {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	}
	if proxy != "" {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, fmt.Errorf("bad proxy URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	return &http.Client{Transport: transport, Timeout: timeout}, nil
}

func (p *Pool) snapshot() []*Key {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Key, len(p.keys))
	copy(out, p.keys)
	return out
}

func (p *Pool) Acquire(ctx context.Context) (*Key, func(), error) {
	now := time.Now()
	reasons := map[string]int{}
	var earliest time.Time
	for _, k := range p.snapshot() {
		if release := k.tryReserve(now); release != nil {
			return k, release, nil
		}
		if r := k.skipReason(now); r != "" {
			reasons[r]++
		}
		if t := k.earliestRetry(now); !t.IsZero() {
			if earliest.IsZero() || t.Before(earliest) {
				earliest = t
			}
		}
	}
	reason := "all keys unavailable"

	best, bestN := "", 0
	for r, n := range reasons {
		if n > bestN {
			best, bestN = r, n
		}
	}
	if best != "" {
		reason = "all keys " + best
	}
	var retry time.Duration
	if !earliest.IsZero() {
		retry = time.Until(earliest)
		if retry < 0 {
			retry = 0
		}
	}
	return nil, nil, &ErrNoKeyAvailable{RetryAfter: retry, Reason: reason}
}

func (p *Pool) Keys() []*Key { return p.snapshot() }

func (p *Pool) Find(id string) *Key {
	for _, k := range p.snapshot() {
		if k.cfg.ID == id {
			return k
		}
	}
	return nil
}

func IsQuotaError(body string) bool {
	lower := strings.ToLower(body)
	for _, s := range []string{
		"insufficient_quota",
		"quota_exceeded",
		"exceeded your current quota",
		"billing",
		"quota",
	} {
		if strings.Contains(lower, s) {
			return true
		}
	}
	return false
}

func ParseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	var secs int
	if _, err := fmt.Sscanf(v, "%d", &secs); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func ParseResetDuration(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}

	var secs float64
	if _, err := fmt.Sscanf(v, "%g", &secs); err == nil && secs > 0 {
		return time.Duration(secs * float64(time.Second))
	}
	return 0
}

var _ = errors.New
