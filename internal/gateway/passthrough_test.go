package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"llm-gateway/internal/config"
	"llm-gateway/internal/pool"
	"llm-gateway/internal/store"
)

type fakeUpstream struct {
	mu        sync.Mutex
	received  map[string]int
	inFlight  map[string]int
	maxFlight map[string]int
	behaviors map[string]http.HandlerFunc
}

func newFakeUpstream() *fakeUpstream {
	return &fakeUpstream{
		received:  map[string]int{},
		inFlight:  map[string]int{},
		maxFlight: map[string]int{},
		behaviors: map[string]http.HandlerFunc{},
	}
}

func (f *fakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	f.mu.Lock()
	f.received[token]++
	f.inFlight[token]++
	if f.inFlight[token] > f.maxFlight[token] {
		f.maxFlight[token] = f.inFlight[token]
	}
	h := f.behaviors[token]
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight[token]--
		f.mu.Unlock()
	}()
	if h == nil {
		h = okJSON(0)
	}
	h(w, r)
}

func (f *fakeUpstream) receivedCount(token string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.received[token]
}

func (f *fakeUpstream) maxInFlight(token string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxFlight[token]
}

func okJSON(delay time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			time.Sleep(delay)
		}
		body, _ := io.ReadAll(r.Body)
		model := "unknown"
		var payload map[string]any
		if json.Unmarshal(body, &payload) == nil {
			if m, ok := payload["model"].(string); ok {
				model = m
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-test", "object": "chat.completion", "model": model,
			"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "hi"}}},
			"usage":   map[string]any{"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
		})
	}
}

func errJSON(status int, body string, headers map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for k, v := range headers {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func streamOK() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		fmt.Fprintln(w, `data: {"id":"x","choices":[{"delta":{"content":"hel"}}]}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, `data: {"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, "data: [DONE]")
		fmt.Fprintln(w)
		if fl != nil {
			fl.Flush()
		}
	}
}

func newTestGateway(t *testing.T, upstreamURL string, keys []config.Key, modelMap map[string]string, masterKey string) (*httptest.Server, *store.Store) {
	t.Helper()
	cfg := &config.Config{}
	cfg.Server.Listen = ":0"
	cfg.Server.MasterKey = masterKey
	cfg.Upstream.BaseURL = upstreamURL
	cfg.Upstream.TimeoutSeconds = 5
	cfg.Keys = keys
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validate: %v", err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("store open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	p, err := pool.NewFromKeys(cfg.Keys, 5*time.Second)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	gw := New(cfg, p, st, modelMap)
	srv := httptest.NewServer(gw.Routes())
	t.Cleanup(srv.Close)
	return srv, st
}

func postChat(t *testing.T, client *http.Client, srvURL, model string, stream bool, masterKey string) (int, []byte) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"model":    model,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		"stream":   stream,
	})
	req, err := http.NewRequest(http.MethodPost, srvURL+"/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if masterKey != "" {
		req.Header.Set("Authorization", "Bearer "+masterKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return resp.StatusCode, body
}

func readAllLogs(t *testing.T, st *store.Store) []store.Entry {
	t.Helper()
	entries, err := st.Recent(store.LogFilter{}, 10000, 0)
	if err != nil {
		t.Fatalf("recent logs: %v", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	return entries
}

func keyIDs(entries []store.Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.KeyID
	}
	return out
}

func TestPassthroughRoutesFirstKeyAndMapsModel(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true},
	}, map[string]string{"cheap": "gpt-4o-mini"}, "")
	client := &http.Client{Timeout: 10 * time.Second}

	status, body := postChat(t, client, gw.URL, "cheap", false, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("response json: %v", err)
	}
	if resp["model"] != "gpt-4o-mini" {
		t.Fatalf("model = %v, want gpt-4o-mini (mapped from cheap)", resp["model"])
	}

	logs := readAllLogs(t, st)
	if len(logs) != 1 {
		t.Fatalf("log entries = %d, want 1", len(logs))
	}
	e := logs[0]
	if e.KeyID != "good" || e.ModelRequested != "cheap" || e.ModelUpstream != "gpt-4o-mini" {
		t.Fatalf("log = %+v, want key good cheap->gpt-4o-mini", e)
	}
	if e.Status != 200 || e.TotalTokens != 15 || e.PromptTokens != 10 || e.CompletionTokens != 5 {
		t.Fatalf("log status/tokens = %d/%d, want 200/15", e.Status, e.TotalTokens)
	}
}

func TestPassthroughFailover429(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-bad"] = errJSON(429,
		`{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}`,
		map[string]string{"Retry-After": "60"})
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "bad", APIKey: "tok-bad", Priority: 1, Enabled: true},
		{ID: "good", APIKey: "tok-good", Priority: 2, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}

	status, body := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	if up.receivedCount("tok-bad") != 1 {
		t.Fatalf("bad key upstream calls = %d, want 1", up.receivedCount("tok-bad"))
	}
	if up.receivedCount("tok-good") != 1 {
		t.Fatalf("good key upstream calls = %d, want 1", up.receivedCount("tok-good"))
	}
	logs := readAllLogs(t, st)
	if len(logs) != 1 || logs[0].KeyID != "good" || logs[0].Status != 200 {
		t.Fatalf("log = %+v, want 1 entry served by good with 200", logs)
	}
}

func TestPassthroughFailover500(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-bad"] = errJSON(500, `{"error":{"message":"internal error","type":"server_error"}}`, nil)
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, _ := newTestGateway(t, srv.URL, []config.Key{
		{ID: "bad", APIKey: "tok-bad", Priority: 1, Enabled: true},
		{ID: "good", APIKey: "tok-good", Priority: 2, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}

	status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 via failover", status)
	}
	if up.receivedCount("tok-bad") != 1 || up.receivedCount("tok-good") != 1 {
		t.Fatalf("upstream calls bad=%d good=%d, want 1/1", up.receivedCount("tok-bad"), up.receivedCount("tok-good"))
	}
}

func TestPassthroughQuotaKeySkipped(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-quota"] = errJSON(429,
		`{"error":{"message":"You exceeded your current quota","type":"insufficient_quota","code":"insufficient_quota"}}`,
		map[string]string{"x-ratelimit-reset-requests": "60s"})
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "quota", APIKey: "tok-quota", Priority: 1, Enabled: true},
		{ID: "good", APIKey: "tok-good", Priority: 2, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}

	for i := 0; i < 2; i++ {
		status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
		if status != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, status)
		}
	}
	if up.receivedCount("tok-quota") != 1 {
		t.Fatalf("quota key upstream calls = %d, want 1 (blacklisted after first quota error)", up.receivedCount("tok-quota"))
	}
	logs := readAllLogs(t, st)
	if got := keyIDs(logs); len(got) != 2 || got[0] != "good" || got[1] != "good" {
		t.Fatalf("log keys = %v, want [good good]", got)
	}
}

func TestPassthroughAllKeysFail(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-a"] = errJSON(429, `{"error":{"message":"limited","type":"rate_limit_error"}}`, map[string]string{"Retry-After": "30"})
	up.behaviors["tok-b"] = errJSON(429, `{"error":{"message":"limited","type":"rate_limit_error"}}`, map[string]string{"Retry-After": "30"})
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "a", APIKey: "tok-a", Priority: 1, Enabled: true},
		{ID: "b", APIKey: "tok-b", Priority: 2, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}

	status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", status)
	}
	logs := readAllLogs(t, st)
	if len(logs) != 1 || logs[0].Status != 429 {
		t.Fatalf("log = %+v, want 1 entry with status 429", logs)
	}
}

func TestPassthroughAuth(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, _ := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true},
	}, nil, "test-secret")
	client := &http.Client{Timeout: 10 * time.Second}

	payload := []byte(`{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}`)
	call := func(auth string) int {
		req, _ := http.NewRequest(http.MethodPost, gw.URL+"/v1/chat/completions", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		if auth != "" {
			req.Header.Set("Authorization", "Bearer "+auth)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		return resp.StatusCode
	}
	if got := call(""); got != http.StatusUnauthorized {
		t.Fatalf("no auth: status = %d, want 401", got)
	}
	if got := call("wrong"); got != http.StatusUnauthorized {
		t.Fatalf("wrong auth: status = %d, want 401", got)
	}
	if got := call("test-secret"); got != http.StatusOK {
		t.Fatalf("right auth: status = %d, want 200", got)
	}
}

func TestPassthroughStreaming(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-good"] = streamOK()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}

	status, body := postChat(t, client, gw.URL, "gpt-4o-mini", true, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	if !strings.Contains(string(body), "[DONE]") || !strings.Contains(string(body), "hel") {
		t.Fatalf("stream body missing chunks/DONE: %s", body)
	}
	logs := readAllLogs(t, st)
	if len(logs) != 1 || !logs[0].Stream || logs[0].TotalTokens != 15 {
		t.Fatalf("log = %+v, want 1 streaming entry with 15 tokens", logs)
	}
}

func TestPassthroughRateLimit(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "limited", APIKey: "tok-limited", Priority: 1, Enabled: true,
			RateLimits: []config.RateLimitRule{{Max: 2, Window: "1m"}}},
		{ID: "fallback", APIKey: "tok-fallback", Priority: 2, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}

	for i := 0; i < 5; i++ {
		status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
		if status != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, status)
		}
	}
	if up.receivedCount("tok-limited") != 2 {
		t.Fatalf("limited upstream calls = %d, want 2 (2/min window)", up.receivedCount("tok-limited"))
	}
	if up.receivedCount("tok-fallback") != 3 {
		t.Fatalf("fallback upstream calls = %d, want 3", up.receivedCount("tok-fallback"))
	}
	logs := readAllLogs(t, st)
	want := []string{"limited", "limited", "fallback", "fallback", "fallback"}
	if got := keyIDs(logs); len(got) != 5 || strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("log keys = %v, want %v", got, want)
	}
}

func TestPassthroughConcurrency(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-a"] = okJSON(60 * time.Millisecond)
	up.behaviors["tok-b"] = okJSON(60 * time.Millisecond)
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "a", APIKey: "tok-a", Priority: 1, Enabled: true, MaxConcurrency: 2},
		{ID: "b", APIKey: "tok-b", Priority: 2, Enabled: true, MaxConcurrency: 2},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}

	const n = 20
	statuses := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
			statuses[i] = status
		}(i)
	}
	wg.Wait()

	for _, s := range statuses {
		if s != http.StatusOK && s != http.StatusTooManyRequests {
			t.Fatalf("unexpected status %d, want only 200/429", s)
		}
	}
	if got := up.maxInFlight("tok-a"); got != 2 {
		t.Fatalf("key a max in-flight = %d, want exactly 2 (cap reached, never exceeded)", got)
	}
	if got := up.maxInFlight("tok-b"); got != 2 {
		t.Fatalf("key b max in-flight = %d, want exactly 2 (cap reached, never exceeded)", got)
	}
	if logs := readAllLogs(t, st); len(logs) != n {
		t.Fatalf("log entries = %d, want %d (one per request)", len(logs), n)
	}
}

func maxLogOverlap(entries []store.Entry, keyID string) int {
	type event struct {
		at    time.Time
		delta int
	}
	var events []event
	for _, e := range entries {
		if e.KeyID != keyID || e.LatencyMs <= 1 {
			continue
		}
		start, err := time.Parse(time.RFC3339Nano, e.TS)
		if err != nil {
			continue
		}
		end := start.Add(time.Duration(e.LatencyMs-1) * time.Millisecond)
		events = append(events, event{start, 1}, event{end, -1})
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].at.Equal(events[j].at) {
			return events[i].delta < events[j].delta
		}
		return events[i].at.Before(events[j].at)
	})
	max, cur := 0, 0
	for _, ev := range events {
		cur += ev.delta
		if cur > max {
			max = cur
		}
	}
	return max
}

func TestGatewayManyRequestsAuditLog(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-429"] = errJSON(429,
		`{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}`,
		map[string]string{"Retry-After": "60"})
	up.behaviors["tok-limited"] = okJSON(0)
	up.behaviors["tok-slow"] = okJSON(30 * time.Millisecond)
	up.behaviors["tok-quota"] = errJSON(429,
		`{"error":{"message":"You exceeded your current quota","type":"insufficient_quota","code":"insufficient_quota"}}`,
		map[string]string{"x-ratelimit-reset-requests": "60s"})
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "q-429", APIKey: "tok-429", Priority: 1, Enabled: true, MaxConcurrency: 1},
		{ID: "q-limited", APIKey: "tok-limited", Priority: 2, Enabled: true, MaxConcurrency: 2,
			RateLimits: []config.RateLimitRule{{Max: 2, Window: "1m"}}},
		{ID: "q-slow", APIKey: "tok-slow", Priority: 3, Enabled: true, MaxConcurrency: 3},
		{ID: "q-quota", APIKey: "tok-quota", Priority: 4, Enabled: true, MaxConcurrency: 1},
	}, map[string]string{"cheap": "gpt-4o-mini"}, "")
	client := &http.Client{Timeout: 10 * time.Second}

	const total = 50
	statuses := make([]int, 0, total)
	var mu sync.Mutex
	fire := func() {
		status, _ := postChat(t, client, gw.URL, "cheap", false, "")
		mu.Lock()
		statuses = append(statuses, status)
		mu.Unlock()
	}
	fire()
	time.Sleep(100 * time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < total-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fire()
		}()
	}
	wg.Wait()

	if len(statuses) != total {
		t.Fatalf("client results = %d, want %d", len(statuses), total)
	}
	ok, tooMany := 0, 0
	for _, s := range statuses {
		switch s {
		case http.StatusOK:
			ok++
		case http.StatusTooManyRequests:
			tooMany++
		default:
			t.Fatalf("unexpected client status %d, want only 200/429", s)
		}
	}
	if ok < 5 {
		t.Fatalf("successes = %d, want >= 5 (limited 2 + slow slots)", ok)
	}
	if ok+tooMany != total {
		t.Fatalf("200 (%d) + 429 (%d) != %d", ok, tooMany, total)
	}

	if got := up.receivedCount("tok-429"); got != 1 {
		t.Fatalf("q-429 upstream calls = %d, want 1 (blacklisted 60s after first 429)", got)
	}
	if got := up.receivedCount("tok-limited"); got != 2 {
		t.Fatalf("q-limited upstream calls = %d, want 2 (2/min window)", got)
	}
	if got := up.receivedCount("tok-quota"); got != 1 {
		t.Fatalf("q-quota upstream calls = %d, want 1 (blacklisted after first quota error)", got)
	}
	if got := up.maxInFlight("tok-slow"); got != 3 {
		t.Fatalf("q-slow max in-flight = %d, want exactly 3 (cap reached, never exceeded)", got)
	}
	if got := up.maxInFlight("tok-limited"); got > 2 {
		t.Fatalf("q-limited max in-flight = %d, want <= 2", got)
	}

	logs := readAllLogs(t, st)
	if len(logs) != total {
		t.Fatalf("log entries = %d, want %d (one per request)", len(logs), total)
	}
	byKey := map[string]int{}
	logOK, log429 := 0, 0
	for _, e := range logs {
		byKey[e.KeyID]++
		switch e.Status {
		case 200:
			logOK++
			if e.TotalTokens != 15 || e.ModelRequested != "cheap" || e.ModelUpstream != "gpt-4o-mini" {
				t.Fatalf("log entry %+v: 200 must carry cheap->gpt-4o-mini with 15 tokens", e)
			}
		case 429:
			log429++
		default:
			t.Fatalf("log entry %+v: unexpected status, want 200/429", e)
		}
		if e.KeyID == "" && e.Status == 200 {
			t.Fatalf("log entry %+v: 200 with no key", e)
		}
	}
	if logOK != ok || log429 != tooMany {
		t.Fatalf("log 200/429 = %d/%d, client = %d/%d", logOK, log429, ok, tooMany)
	}
	if byKey["q-limited"] != 2 {
		t.Fatalf("log q-limited = %d, want 2", byKey["q-limited"])
	}
	if byKey["q-429"] != 0 {
		t.Fatalf("log q-429 = %d, want 0 (it only failed, never served)", byKey["q-429"])
	}
	if byKey["q-quota"] != 1 {
		t.Fatalf("log q-quota = %d, want 1 (the single quota failure)", byKey["q-quota"])
	}
	if byKey["q-slow"] != ok-2 {
		t.Fatalf("log q-slow = %d, want %d (all successes minus limited)", byKey["q-slow"], ok-2)
	}
	if overlap := maxLogOverlap(logs, "q-slow"); overlap > 3 {
		t.Fatalf("log overlap on q-slow = %d, want <= 3 (max concurrency)", overlap)
	}
	limitedInWindow := 0
	for _, e := range logs {
		if e.KeyID == "q-limited" {
			limitedInWindow++
		}
	}
	if limitedInWindow > 2 {
		t.Fatalf("q-limited log entries in window = %d, want <= 2 (2/min)", limitedInWindow)
	}
}
