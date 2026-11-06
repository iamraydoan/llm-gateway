package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

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

type fakeRelay struct {
	mu     sync.Mutex
	target string
	path   string
	auth   string
	hits   int
	fail   bool
}

func newRelayServer(t *testing.T) (*httptest.Server, *fakeRelay) {
	t.Helper()
	r := &fakeRelay{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.target = req.Header.Get("x-relay-target")
		r.path = req.Header.Get("x-relay-path")
		r.auth = req.Header.Get("Authorization")
		r.hits++
		fail := r.fail
		r.mu.Unlock()
		if r.target == "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"Missing x-relay-target header"}`))
			return
		}
		if fail {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"relay rate limited","type":"rate_limit_error"}}`))
			return
		}
		body, _ := io.ReadAll(req.Body)
		up, err := http.NewRequest(req.Method, r.target+r.path, bytes.NewReader(body))
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		for k, vs := range req.Header {
			lk := strings.ToLower(k)
			if lk == "x-relay-target" || lk == "x-relay-path" || lk == "host" {
				continue
			}
			for _, v := range vs {
				up.Header.Add(k, v)
			}
		}
		resp, err := http.DefaultClient.Do(up)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		fl, _ := w.(http.Flusher)
		buf := make([]byte, 32*1024)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				_, _ = w.Write(buf[:n])
				if fl != nil {
					fl.Flush()
				}
			}
			if err != nil {
				break
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv, r
}

func (r *fakeRelay) snapshot() (target, path, auth string, hits int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.target, r.path, r.auth, r.hits
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
			"usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
				"prompt_tokens_details": map[string]any{"cached_tokens": 4}},
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
		fmt.Fprintln(w, `data: {"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"prompt_tokens_details":{"cached_tokens":4}}}`)
		fmt.Fprintln(w)
		fmt.Fprintln(w, "data: [DONE]")
		fmt.Fprintln(w)
		if fl != nil {
			fl.Flush()
		}
	}
}

func newTestGateway(t *testing.T, upstreamURL string, keys []config.Key, modelMap map[string]string, masterKey string) (*httptest.Server, *store.Store) {
	return newTestGatewayWithFailovers(t, upstreamURL, keys, modelMap, masterKey, 0)
}

func newTestGatewayWithFailovers(t *testing.T, upstreamURL string, keys []config.Key, modelMap map[string]string, masterKey string, maxFailovers int) (*httptest.Server, *store.Store) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("store open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return newTestGatewayWithStore(t, upstreamURL, keys, modelMap, masterKey, maxFailovers, st)
}

func newTestGatewayWithStore(t *testing.T, upstreamURL string, keys []config.Key, modelMap map[string]string, masterKey string, maxFailovers int, st *store.Store) (*httptest.Server, *store.Store) {
	t.Helper()
	cfg := &config.Config{}
	cfg.Server.Listen = ":0"
	cfg.Server.MasterKey = masterKey
	cfg.Server.MaxFailovers = maxFailovers
	cfg.Upstream.BaseURL = upstreamURL
	cfg.Upstream.TimeoutSeconds = 5
	cfg.Keys = keys
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validate: %v", err)
	}
	p, err := pool.NewFromKeys(cfg.Keys, 5*time.Second)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	gw := New(cfg, p, st, modelMap)
	srv := httptest.NewServer(gw.Routes())
	t.Cleanup(srv.Close)
	return srv, st
}

func keysList(t *testing.T, srvURL string) []map[string]any {
	t.Helper()
	resp, err := http.Get(srvURL + "/api/keys")
	if err != nil {
		t.Fatalf("keys get: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Keys []map[string]any `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("keys decode: %v", err)
	}
	return out.Keys
}

func keyRowByID(keys []map[string]any, id string) map[string]any {
	for _, k := range keys {
		if k["id"] == id {
			return k
		}
	}
	return nil
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

func statsPayload(t *testing.T, statsURL string) map[string]any {
	t.Helper()
	resp, err := http.Get(statsURL)
	if err != nil {
		t.Fatalf("stats get: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("stats decode: %v", err)
	}
	return out
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
	if e.Status != 200 || e.TotalTokens != 15 || e.PromptTokens != 10 || e.CachedTokens != 4 || e.CompletionTokens != 5 {
		t.Fatalf("log status/tokens = %d/%d/%d, want 200/15/4", e.Status, e.TotalTokens, e.CachedTokens)
	}
	stats := statsPayload(t, gw.URL+"/api/stats")
	if stats["total_prompt_tokens"] != float64(10) || stats["total_cached_tokens"] != float64(4) ||
		stats["total_completion_tokens"] != float64(5) || stats["total_tokens"] != float64(15) {
		t.Fatalf("stats tokens = %+v, want prompt 10 cached 4 completion 5 total 15", stats)
	}
	byKey, ok := stats["tokens_by_key"].([]any)
	if !ok || len(byKey) != 1 {
		t.Fatalf("tokens_by_key = %+v, want 1 row", stats["tokens_by_key"])
	}
	row := byKey[0].(map[string]any)
	if row["cached_tokens"] != float64(4) || row["prompt_tokens"] != float64(10) {
		t.Fatalf("tokens_by_key row = %+v, want cached 4 prompt 10", row)
	}
}

func TestAPIStatsRange(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, _ := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}
	if status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, ""); status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	now := time.Now()
	wide := statsPayload(t, gw.URL+"/api/stats?from="+url.QueryEscape(now.Add(-time.Hour).Format(time.RFC3339))+"&to="+url.QueryEscape(now.Add(time.Hour).Format(time.RFC3339)))
	if wide["total_requests"] != float64(1) || wide["total_cached_tokens"] != float64(4) {
		t.Fatalf("wide range stats = %+v, want 1 request 4 cached", wide)
	}
	future := statsPayload(t, gw.URL+"/api/stats?from="+url.QueryEscape(now.Add(24*time.Hour).Format(time.RFC3339)))
	if future["total_requests"] != float64(0) {
		t.Fatalf("future range stats = %+v, want 0 requests", future)
	}
}

func TestKeysSurviveRestart(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-bad"] = errJSON(429,
		`{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}`,
		map[string]string{"Retry-After": "60"})
	srv := httptest.NewServer(up)
	defer srv.Close()
	keys := []config.Key{
		{ID: "bad", APIKey: "tok-bad", Priority: 1, Enabled: true},
		{ID: "good", APIKey: "tok-good", Priority: 2, Enabled: true,
			RateLimits: []config.RateLimitRule{{Max: 2, Window: "1h"}}},
	}
	gw1, st := newTestGateway(t, srv.URL, keys, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}
	if status, _ := postChat(t, client, gw1.URL, "gpt-4o-mini", false, ""); status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	gw2, _ := newTestGatewayWithStore(t, srv.URL, keys, nil, "", 0, st)
	ks := keysList(t, gw2.URL)
	good := keyRowByID(ks, "good")
	if good == nil {
		t.Fatalf("keys = %+v, want good", ks)
	}
	if good["total_requests"] != float64(1) {
		t.Fatalf("good total_requests = %v, want 1 from logs after restart", good["total_requests"])
	}
	rl, ok := good["rate_limits"].([]any)
	if !ok || len(rl) != 1 {
		t.Fatalf("good rate_limits = %+v, want 1 window", good["rate_limits"])
	}
	if rl[0].(map[string]any)["remaining"] != float64(1) {
		t.Fatalf("good remaining = %+v, want 1 (max 2 minus 1 logged hit)", rl[0])
	}
	bad := keyRowByID(ks, "bad")
	if bad == nil {
		t.Fatalf("keys = %+v, want bad", ks)
	}
	if bad["total_errors"] != float64(1) {
		t.Fatalf("bad total_errors = %v, want 1 from logs after restart", bad["total_errors"])
	}
	if le, _ := bad["last_error"].(string); le == "" || !strings.Contains(le, "Rate limit reached") {
		t.Fatalf("bad last_error = %q, want raw 429 body from logs", le)
	}
}

func TestPassthroughRelay(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	relay, cap := newRelayServer(t)
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true, RelayURL: relay.URL},
	}, map[string]string{"cheap": "gpt-4o-mini"}, "")
	client := &http.Client{Timeout: 10 * time.Second}
	status, body := postChat(t, client, gw.URL, "cheap", false, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	if up.receivedCount("tok-good") != 1 {
		t.Fatalf("upstream received = %d, want 1 via relay", up.receivedCount("tok-good"))
	}
	target, path, auth, hits := cap.snapshot()
	if hits != 1 || target != srv.URL || path != "/v1/chat/completions" || auth != "Bearer tok-good" {
		t.Fatalf("relay saw target=%q path=%q auth=%q hits=%d", target, path, auth, hits)
	}
	logs := readAllLogs(t, st)
	if len(logs) != 1 || logs[0].TotalTokens != 15 || logs[0].CachedTokens != 4 {
		t.Fatalf("log = %+v, want 1 line 15 tokens 4 cached", logs)
	}
}

func TestPassthroughRelayStream(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-good"] = streamOK()
	srv := httptest.NewServer(up)
	defer srv.Close()
	relay, _ := newRelayServer(t)
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true, RelayURL: relay.URL},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}
	status, body := postChat(t, client, gw.URL, "gpt-4o-mini", true, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	if !strings.Contains(string(body), "usage") {
		t.Fatalf("stream body missing usage chunk: %s", body)
	}
	logs := readAllLogs(t, st)
	if len(logs) != 1 || !logs[0].Stream || logs[0].TotalTokens != 15 || logs[0].CachedTokens != 4 {
		t.Fatalf("log = %+v, want 1 stream line 15 tokens 4 cached", logs)
	}
}

func TestPassthroughRelayFailover(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	relay, cap := newRelayServer(t)
	cap.fail = true
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "bad", APIKey: "tok-bad", Priority: 1, Enabled: true, RelayURL: relay.URL},
		{ID: "good", APIKey: "tok-good", Priority: 2, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}
	status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if up.receivedCount("tok-good") != 1 {
		t.Fatalf("good received = %d, want 1 after relay 429 failover", up.receivedCount("tok-good"))
	}
	logs := readAllLogs(t, st)
	if len(logs) != 2 || logs[0].KeyID != "bad" || logs[0].Status != 429 || logs[1].KeyID != "good" {
		t.Fatalf("logs = %+v, want bad/429 then good/200", logs)
	}
}

func TestPassthroughKeyDefaults(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	var cfg config.Config
	text := `
upstream:
  base_url: "http://unused.invalid"
key_defaults:
  priority: 1
  max_concurrency: 1
  enabled: true
  rate_limits:
    - max: 2
      window: 1m
keys:
  - tok-good
`
	if err := yaml.Unmarshal([]byte(text), &cfg); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	gw, _ := newTestGateway(t, srv.URL, cfg.Keys, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}
	status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (string-form key + defaults must serve)", status)
	}
	if up.receivedCount("tok-good") != 1 {
		t.Fatalf("upstream received = %d, want 1", up.receivedCount("tok-good"))
	}
	row := keyRowByID(keysList(t, gw.URL), "key-1")
	if row == nil || row["priority"] != float64(1) || row["enabled"] != true {
		t.Fatalf("keys row = %v, want priority 1 enabled true from defaults", row)
	}
	rls, _ := row["rate_limits"].([]any)
	if len(rls) != 1 || rls[0].(map[string]any)["remaining"] != float64(1) {
		t.Fatalf("rate_limits = %v, want 1 window with remaining 1 after one request", row["rate_limits"])
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
	if len(logs) != 2 {
		t.Fatalf("log entries = %d, want 2 (one per attempt)", len(logs))
	}
	if logs[0].KeyID != "bad" || logs[0].Status != 429 || logs[0].Error == "" {
		t.Fatalf("log[0] = %+v, want bad/429 with error", logs[0])
	}
	if logs[1].KeyID != "good" || logs[1].Status != 200 || logs[1].TotalTokens != 15 {
		t.Fatalf("log[1] = %+v, want good/200 with 15 tokens", logs[1])
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
	if got := keyIDs(logs); len(got) != 3 || got[0] != "quota" || got[1] != "good" || got[2] != "good" {
		t.Fatalf("log keys = %v, want [quota good good]", got)
	}
	if logs[0].Status != 429 || logs[1].Status != 200 || logs[2].Status != 200 {
		t.Fatalf("log statuses = %d/%d/%d, want 429/200/200", logs[0].Status, logs[1].Status, logs[2].Status)
	}
}

func TestPassthroughAllKeysFail(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-a"] = errJSON(429, `{"error":{"message":"limited-a","type":"rate_limit_error"}}`, map[string]string{"Retry-After": "30"})
	up.behaviors["tok-b"] = errJSON(429, `{"error":{"message":"limited-b","type":"rate_limit_error"}}`, map[string]string{"Retry-After": "30"})
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "a", APIKey: "tok-a", Priority: 1, Enabled: true},
		{ID: "b", APIKey: "tok-b", Priority: 2, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}

	status, body := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", status)
	}
	if !strings.Contains(string(body), "limited-b") || strings.Contains(string(body), "limited-a") {
		t.Fatalf("body = %s, want the LAST upstream error (limited-b) verbatim", body)
	}
	if up.receivedCount("tok-a") != 1 || up.receivedCount("tok-b") != 1 {
		t.Fatalf("upstream calls a=%d b=%d, want 1/1 (each key tried once, no extra retries)", up.receivedCount("tok-a"), up.receivedCount("tok-b"))
	}
	logs := readAllLogs(t, st)
	if len(logs) != 2 || logs[0].Status != 429 || logs[1].Status != 429 {
		t.Fatalf("log = %+v, want 2 entries with status 429", logs)
	}
	if got := keyIDs(logs); got[0] != "a" || got[1] != "b" {
		t.Fatalf("log keys = %v, want [a b]", got)
	}
}

func TestPassthroughAcquireExhaustedReturnsLastError(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-a"] = errJSON(429, `{"error":{"message":"limited-a","type":"rate_limit_error"}}`, map[string]string{"Retry-After": "60"})
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "a", APIKey: "tok-a", Priority: 1, Enabled: true},
		{ID: "b", APIKey: "tok-b", Priority: 2, Enabled: false},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}

	status, body := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", status)
	}
	if !strings.Contains(string(body), "limited-a") {
		t.Fatalf("body = %s, want last upstream error (limited-a), not generic no-keys message", body)
	}
	if up.receivedCount("tok-a") != 1 || up.receivedCount("tok-b") != 0 {
		t.Fatalf("upstream calls a=%d b=%d, want 1/0", up.receivedCount("tok-a"), up.receivedCount("tok-b"))
	}
	logs := readAllLogs(t, st)
	if len(logs) != 2 || logs[0].Status != 429 || logs[1].Status != 429 {
		t.Fatalf("log = %+v, want 2 entries with status 429", logs)
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
	if len(logs) != 1 || !logs[0].Stream || logs[0].TotalTokens != 15 || logs[0].CachedTokens != 4 {
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
	if len(logs) != total+2 {
		t.Fatalf("log entries = %d, want %d (one per attempt + no-key outcomes)", len(logs), total+2)
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
	if logOK != ok || log429 != tooMany+2 {
		t.Fatalf("log 200/429 = %d/%d, want %d/%d (client 429s + 2 extra attempt lines)", logOK, log429, ok, tooMany+2)
	}
	if byKey["q-limited"] != 2 {
		t.Fatalf("log q-limited = %d, want 2", byKey["q-limited"])
	}
	if byKey["q-429"] != 1 {
		t.Fatalf("log q-429 = %d, want 1 (the wave-0 failure attempt)", byKey["q-429"])
	}
	if byKey[""] != 50-ok && byKey[""] != 49-ok {
		t.Fatalf("log no-key = %d, want %d or %d (depends on whether the quota request's 2nd attempt caught a slow slot)", byKey[""], 50-ok, 49-ok)
	}
	if byKey["q-quota"] != 1 {
		t.Fatalf("log q-quota = %d, want 1 (the single quota failure)", byKey["q-quota"])
	}
	if byKey["q-slow"] != ok-2 {
		t.Fatalf("log q-slow = %d, want %d (all successes minus limited)", byKey["q-slow"], ok-2)
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

func keysStatus(t *testing.T, client *http.Client, srvURL string) []map[string]any {
	t.Helper()
	resp, err := client.Get(srvURL + "/api/keys")
	if err != nil {
		t.Fatalf("get keys: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var data struct {
		Keys []map[string]any `json:"keys"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		t.Fatalf("keys json: %v", err)
	}
	return data.Keys
}

func keyByID(t *testing.T, client *http.Client, srvURL, id string) map[string]any {
	t.Helper()
	for _, k := range keysStatus(t, client, srvURL) {
		if k["id"] == id {
			return k
		}
	}
	t.Fatalf("key %q not in /api/keys", id)
	return nil
}

func TestPassthroughLogsEveryAttempt(t *testing.T) {
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

	status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 via failover", status)
	}
	logs := readAllLogs(t, st)
	if len(logs) != 2 {
		t.Fatalf("log entries = %d, want 2 (key1 429 attempt + key2 200)", len(logs))
	}
	wantErr := `{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}`
	if logs[0].KeyID != "bad" || logs[0].Status != 429 || logs[0].Error != wantErr {
		t.Fatalf("log[0] = %+v, want bad/429 with the raw upstream body", logs[0])
	}
	if logs[1].KeyID != "good" || logs[1].Status != 200 || logs[1].TotalTokens != 15 {
		t.Fatalf("log[1] = %+v, want good/200 with 15 tokens", logs[1])
	}
	bad := keyByID(t, client, gw.URL, "bad")
	if bad["last_error"] != wantErr {
		t.Fatalf("bad last_error = %v, want the raw upstream body", bad["last_error"])
	}
	if bad["consecutive_failures"] != float64(1) {
		t.Fatalf("bad consecutive_failures = %v, want 1", bad["consecutive_failures"])
	}
}

func TestPassthroughMaxFailovers(t *testing.T) {
	up := newFakeUpstream()
	up.behaviors["tok-bad1"] = errJSON(429, `{"error":{"message":"limited","type":"rate_limit_error"}}`, map[string]string{"Retry-After": "60"})
	up.behaviors["tok-bad2"] = errJSON(429, `{"error":{"message":"limited","type":"rate_limit_error"}}`, map[string]string{"Retry-After": "60"})
	srv := httptest.NewServer(up)
	defer srv.Close()
	keys := []config.Key{
		{ID: "bad1", APIKey: "tok-bad1", Priority: 1, Enabled: true},
		{ID: "bad2", APIKey: "tok-bad2", Priority: 2, Enabled: true},
		{ID: "good", APIKey: "tok-good", Priority: 3, Enabled: true},
	}
	client := &http.Client{Timeout: 10 * time.Second}

	capped, stCapped := newTestGatewayWithFailovers(t, srv.URL, keys, nil, "", 2)
	status, _ := postChat(t, client, capped.URL, "gpt-4o-mini", false, "")
	if status != http.StatusTooManyRequests {
		t.Fatalf("capped: status = %d, want 429 (only 2 keys tried)", status)
	}
	if up.receivedCount("tok-good") != 0 {
		t.Fatalf("capped: good upstream calls = %d, want 0", up.receivedCount("tok-good"))
	}
	if logs := readAllLogs(t, stCapped); len(logs) != 2 {
		t.Fatalf("capped: log entries = %d, want 2", len(logs))
	}

	def, stDef := newTestGateway(t, srv.URL, keys, nil, "")
	status, _ = postChat(t, client, def.URL, "gpt-4o-mini", false, "")
	if status != http.StatusOK {
		t.Fatalf("default: status = %d, want 200 (3rd key reached)", status)
	}
	if up.receivedCount("tok-good") != 1 {
		t.Fatalf("default: good upstream calls = %d, want 1", up.receivedCount("tok-good"))
	}
	if logs := readAllLogs(t, stDef); len(logs) != 3 {
		t.Fatalf("default: log entries = %d, want 3", len(logs))
	}
}

func TestPassthroughSuccessClearsStoredError(t *testing.T) {
	up := newFakeUpstream()
	var mu sync.Mutex
	calls := 0
	up.behaviors["tok-flaky"] = func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			errJSON(429,
				`{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}`,
				map[string]string{"Retry-After": "1"})(w, r)
			return
		}
		okJSON(0)(w, r)
	}
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "flaky", APIKey: "tok-flaky", Priority: 1, Enabled: true},
		{ID: "good", APIKey: "tok-good", Priority: 2, Enabled: true},
	}, nil, "")
	client := &http.Client{Timeout: 10 * time.Second}

	status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
	if status != http.StatusOK {
		t.Fatalf("request 1: status = %d, want 200 via good", status)
	}
	flaky := keyByID(t, client, gw.URL, "flaky")
	if flaky["last_error"] == "" || flaky["last_error"] == nil {
		t.Fatalf("flaky last_error should be set after the 429, got %v", flaky["last_error"])
	}

	time.Sleep(5500 * time.Millisecond)
	status, _ = postChat(t, client, gw.URL, "gpt-4o-mini", false, "")
	if status != http.StatusOK {
		t.Fatalf("request 2: status = %d, want 200 via flaky (cooldown over)", status)
	}
	flaky = keyByID(t, client, gw.URL, "flaky")
	if v, ok := flaky["last_error"]; ok && v != "" {
		t.Fatalf("flaky last_error = %v, want empty/absent after a success", v)
	}
	if flaky["consecutive_failures"] != float64(0) {
		t.Fatalf("flaky consecutive_failures = %v, want 0 after a success", flaky["consecutive_failures"])
	}
	logs := readAllLogs(t, st)
	if len(logs) != 3 {
		t.Fatalf("log entries = %d, want 3 (flaky 429 + good 200 + flaky 200)", len(logs))
	}
	if got := keyIDs(logs); got[0] != "flaky" || got[1] != "good" || got[2] != "flaky" {
		t.Fatalf("log keys = %v, want [flaky good flaky]", got)
	}
}

func TestDashboardUIMobile(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, _ := newTestGateway(t, srv.URL, []config.Key{
		{ID: "key-1", APIKey: "sk-test", Priority: 1, MaxConcurrency: 1},
	}, nil, "")
	defer gw.Close()
	res, err := http.Get(gw.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET / status = %d", res.StatusCode)
	}
	for _, want := range []string{
		`name="viewport"`,
		`id="nav-toggle"`,
		`id="nav-backdrop"`,
		`class="topbar"`,
		`class="table-wrap"`,
		`@media (max-width: 900px)`,
		`setNav(`,
		`min-width:0`,
		`id="log-presets"`,
		`fmtLocalInput(`,
		`logPresetHours`,
		`l.status === 499`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("dashboard HTML missing %q", want)
		}
	}
}

func TestAPIStatsHoursZeroAllTime(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true},
	}, nil, "")
	defer gw.Close()
	if _, err := st.Log(&store.Entry{
		TS: time.Now().Add(-48 * time.Hour).Format(time.RFC3339), KeyID: "good",
		ModelRequested: "gpt-4o-mini", ModelUpstream: "gpt-4o-mini",
		Status: 200, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15,
	}); err != nil {
		t.Fatalf("seed old log: %v", err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	if status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, ""); status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if got := statsPayload(t, gw.URL+"/api/stats?hours=24")["total_requests"]; got != float64(1) {
		t.Fatalf("hours=24 total_requests = %v, want 1", got)
	}
	if got := statsPayload(t, gw.URL+"/api/stats?hours=0")["total_requests"]; got != float64(2) {
		t.Fatalf("hours=0 total_requests = %v, want 2", got)
	}
	if got := statsPayload(t, gw.URL+"/api/stats")["total_requests"]; got != float64(1) {
		t.Fatalf("no hours param total_requests = %v, want 1 (default 24h)", got)
	}
}

func TestAPILogsHoursParam(t *testing.T) {
	up := newFakeUpstream()
	srv := httptest.NewServer(up)
	defer srv.Close()
	gw, st := newTestGateway(t, srv.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true},
	}, nil, "")
	defer gw.Close()
	if _, err := st.Log(&store.Entry{
		TS: time.Now().Add(-48 * time.Hour).Format(time.RFC3339), KeyID: "good",
		ModelRequested: "gpt-4o-mini", ModelUpstream: "gpt-4o-mini",
		Status: 200, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15,
	}); err != nil {
		t.Fatalf("seed old log: %v", err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	if status, _ := postChat(t, client, gw.URL, "gpt-4o-mini", false, ""); status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	logs := func(q string) map[string]any {
		t.Helper()
		resp, err := http.Get(gw.URL + "/api/logs?" + q)
		if err != nil {
			t.Fatalf("logs get: %v", err)
		}
		defer resp.Body.Close()
		var out map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("logs decode: %v", err)
		}
		return out
	}
	if got := logs("hours=24")["total"]; got != float64(1) {
		t.Fatalf("hours=24 total = %v, want 1", got)
	}
	if got := logs("limit=50")["total"]; got != float64(2) {
		t.Fatalf("no hours total = %v, want 2", got)
	}
}

func TestClientCancelDoesNotPenalizeKey(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(10 * time.Second):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"chatcmpl-x","object":"chat.completion","choices":[{"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`))
		}
	}))
	defer up.Close()
	gw, st := newTestGateway(t, up.URL, []config.Key{
		{ID: "good", APIKey: "tok-good", Priority: 1, Enabled: true,
			RateLimits: []config.RateLimitRule{{Max: 1, Window: "1m"}}},
	}, nil, "")
	defer gw.Close()
	ctx, cancel := context.WithCancel(context.Background())
	payload, _ := json.Marshal(map[string]any{
		"model": "gpt-4o-mini", "messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gw.URL+"/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	done := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("canceled request unexpectedly succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled request did not return")
	}
	var entries []store.Entry
	for i := 0; i < 20; i++ {
		entries = readAllLogs(t, st)
		if len(entries) == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(entries) != 1 {
		t.Fatalf("logged entries = %d, want 1", len(entries))
	}
	if entries[0].Status != 499 || entries[0].Error != "client canceled" {
		t.Fatalf("log entry = status %d error %q, want 499 \"client canceled\"", entries[0].Status, entries[0].Error)
	}
	row := keyRowByID(keysList(t, gw.URL), "good")
	if row == nil {
		t.Fatal("key good missing from /api/keys")
	}
	if row["skip_reason"] != "rate-limited" {
		t.Fatalf("skip_reason = %v, want rate-limited (cancel still consumes the rate-limit unit)", row["skip_reason"])
	}
	if row["consecutive_failures"] != float64(0) {
		t.Fatalf("consecutive_failures = %v, want 0", row["consecutive_failures"])
	}
	if row["last_error"] != "" && row["last_error"] != nil {
		t.Fatalf("last_error = %v, want empty", row["last_error"])
	}
	stats := statsPayload(t, gw.URL+"/api/stats?hours=0")
	if stats["total_requests"] != float64(1) || stats["total_errors"] != float64(0) {
		t.Fatalf("stats = %+v, want 1 request 0 errors", stats)
	}
	resp, err := http.Get(gw.URL + "/api/logs?errors=true")
	if err != nil {
		t.Fatalf("logs errors get: %v", err)
	}
	defer resp.Body.Close()
	var logsOut struct {
		Total int `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&logsOut); err != nil {
		t.Fatalf("logs decode: %v", err)
	}
	if logsOut.Total != 0 {
		t.Fatalf("errors-only logs total = %d, want 0", logsOut.Total)
	}
}
