// Package gateway is the OpenAI-compatible reverse proxy: it routes each
// request through the key pool, applies model mapping, handles streaming,
// classifies upstream errors (rate limit / quota / transient) and logs
// every request.
package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"llm-gateway/internal/config"
	"llm-gateway/internal/pool"
	"llm-gateway/internal/store"
)

type Gateway struct {
	cfg   *config.Config
	pool  *pool.Pool
	store *store.Store
	hub   *Hub

	modelMu  sync.RWMutex
	modelMap map[string]string

	directClient   *http.Client
	upstreamTarget string
}

func New(cfg *config.Config, p *pool.Pool, s *store.Store, modelMap map[string]string) *Gateway {
	if modelMap == nil {
		modelMap = map[string]string{}
	}
	g := &Gateway{cfg: cfg, pool: p, store: s, modelMap: modelMap}
	g.directClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: time.Duration(cfg.Upstream.TimeoutSeconds) * time.Second,
	}
	if u, err := url.Parse(cfg.Upstream.BaseURL); err == nil && u.Scheme != "" && u.Host != "" {
		g.upstreamTarget = u.Scheme + "://" + u.Host
	}
	g.hub = NewHub(g.keysSnapshot)

	p.SetStatusChangeHook(func() { g.hub.NotifyKeys() })
	return g
}

func (g *Gateway) MapModel(model string) string {
	g.modelMu.RLock()
	defer g.modelMu.RUnlock()
	if m, ok := g.modelMap[model]; ok {
		return m
	}
	return model
}

func (g *Gateway) SetModelMap(m map[string]string) {
	g.modelMu.Lock()
	defer g.modelMu.Unlock()
	g.modelMap = m
}

func (g *Gateway) ModelMap() map[string]string {
	g.modelMu.RLock()
	defer g.modelMu.RUnlock()
	out := make(map[string]string, len(g.modelMap))
	for k, v := range g.modelMap {
		out[k] = v
	}
	return out
}

type usagePayload struct {
	PromptTokens        int `json:"prompt_tokens"`
	CompletionTokens    int `json:"completion_tokens"`
	TotalTokens         int `json:"total_tokens"`
	PromptTokensDetails *struct {
		CachedTokens int `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
}

func (u *usagePayload) cachedTokens() int {
	if u == nil || u.PromptTokensDetails == nil {
		return 0
	}
	return u.PromptTokensDetails.CachedTokens
}

type requestLog struct {
	keyID            string
	modelRequested   string
	modelUpstream    string
	status           int
	start            time.Time
	stream           bool
	promptTokens     int
	cachedTokens     int
	completionTokens int
	totalTokens      int
	errMsg           string
}

func (g *Gateway) finishLog(l *requestLog) {
	if g.store == nil {
		return
	}
	entry := &store.Entry{
		TS:               l.start.Format(time.RFC3339Nano),
		KeyID:            l.keyID,
		ModelRequested:   l.modelRequested,
		ModelUpstream:    l.modelUpstream,
		Status:           l.status,
		LatencyMs:        time.Since(l.start).Milliseconds(),
		PromptTokens:     l.promptTokens,
		CachedTokens:     l.cachedTokens,
		CompletionTokens: l.completionTokens,
		TotalTokens:      l.totalTokens,
		Stream:           l.stream,
		Error:            l.errMsg,
	}
	id, err := g.store.Log(entry)
	if err != nil {
		return
	}

	entry.ID = id
	if payload, err := json.Marshal(entry); err == nil {
		g.hub.Broadcast("log", payload)
	}
	g.hub.NotifyKeys()
}

func (g *Gateway) auth(w http.ResponseWriter, r *http.Request) bool {
	if g.cfg.Server.MasterKey == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	if strings.TrimPrefix(auth, "Bearer ") != g.cfg.Server.MasterKey {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error": map[string]any{"message": "invalid master key", "type": "auth_error"},
		})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type upstreamError struct {
	status     int
	body       []byte
	retryAfter time.Duration
	quota      bool
	quotaReset time.Time
}

func classifyUpstream(resp *http.Response, body []byte) upstreamError {
	ue := upstreamError{status: resp.StatusCode, body: body}
	ue.retryAfter = pool.ParseRetryAfter(resp.Header.Get("Retry-After"))

	for _, h := range []string{"x-ratelimit-reset-requests", "x-ratelimit-reset-tokens"} {
		if d := pool.ParseResetDuration(resp.Header.Get(h)); d > 0 {
			ue.quotaReset = time.Now().Add(d)
			break
		}
	}
	if ue.retryAfter > 0 && ue.quotaReset.IsZero() {
		ue.quotaReset = time.Now().Add(ue.retryAfter)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		ue.quota = pool.IsQuotaError(string(body))
	}
	return ue
}

func (g *Gateway) doUpstream(ctx context.Context, key *pool.Key, method, path string, body []byte, stream bool) (*http.Response, *upstreamError, error) {
	relayURL := key.RelayURL()
	if relayURL == "" {
		relayURL = g.cfg.Upstream.RelayURL
	}
	url := g.cfg.Upstream.BaseURL + path
	client := key.Client()
	if relayURL != "" && g.upstreamTarget != "" {
		url = relayURL
		client = g.directClient
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	if relayURL != "" && g.upstreamTarget != "" {
		req.Header.Set("x-relay-target", g.upstreamTarget)
		req.Header.Set("x-relay-path", path)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key.APIKey())

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		ue := classifyUpstream(resp, b)
		return nil, &ue, nil
	}
	return resp, nil, nil
}

func (g *Gateway) maxAttempts() int {
	n := g.cfg.Server.MaxFailovers
	if n <= 0 {
		n = 10
	}
	if keys := len(g.pool.Keys()); n > keys {
		n = keys
	}
	return n
}

func (g *Gateway) passthrough(w http.ResponseWriter, r *http.Request) {
	if !g.auth(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": map[string]any{"message": "method not allowed"}})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"message": "cannot read body"}})
		return
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"message": "invalid JSON body"}})
		return
	}
	modelReq, _ := payload["model"].(string)
	modelUp := g.MapModel(modelReq)
	payload["model"] = modelUp
	body, _ = json.Marshal(payload)
	stream, _ := payload["stream"].(bool)

	lg := &requestLog{modelRequested: modelReq, modelUpstream: modelUp, start: time.Now(), stream: stream}

	res := g.runAttempts(r.Context(), r.Method, r.URL.RequestURI(), body, stream, lg)
	if res.canceled {
		return
	}
	if res.resp != nil {
		lg.keyID = res.key.ID()
		lg.status = res.resp.StatusCode
		if stream {
			g.serveStream(w, r, res.resp, lg)
		} else {
			g.serveJSON(w, res.resp, lg)
		}
		res.key.ReportSuccess()
		res.release()
		res.cancel()
		g.finishLog(lg)
		return
	}
	if res.noKey != nil {
		if res.lastErr != nil {
			if res.lastErr.retryAfter == 0 && res.noKey.RetryAfter > 0 {
				w.Header().Set("Retry-After", fmt.Sprintf("%d", int(res.noKey.RetryAfter.Seconds())))
			}
			proxyErrorBody(w, res.lastErr)
			lg.keyID = ""
			lg.status = res.lastErr.status
			lg.errMsg = strings.TrimSpace(string(res.lastErr.body))
			g.finishLog(lg)
			return
		}
		if res.noKey.RetryAfter > 0 {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", int(res.noKey.RetryAfter.Seconds())))
		}
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error": map[string]any{"message": res.noKey.Error(), "type": "rate_limit_error"},
		})
		lg.keyID = ""
		lg.status = http.StatusTooManyRequests
		lg.errMsg = res.noKey.Error()
		g.finishLog(lg)
		return
	}
	if res.poolErr != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"message": res.poolErr.Error()}})
		lg.keyID = ""
		lg.status = http.StatusServiceUnavailable
		lg.errMsg = res.poolErr.Error()
		g.finishLog(lg)
		return
	}
	if res.lastErr != nil {
		proxyErrorBody(w, res.lastErr)
		return
	}
	writeJSON(w, http.StatusBadGateway, map[string]any{
		"error": map[string]any{"message": "all upstream keys failed: " + res.lastErrMsg, "type": "gateway_error"},
	})
}

type attemptResult struct {
	resp       *http.Response
	key        *pool.Key
	release    func()
	cancel     context.CancelFunc
	noKey      *pool.ErrNoKeyAvailable
	poolErr    error
	lastErr    *upstreamError
	lastErrMsg string
	canceled   bool
}

func (g *Gateway) runAttempts(ctx context.Context, method, path string, body []byte, stream bool, lg *requestLog) attemptResult {
	var lastErr *upstreamError
	var lastErrMsg string
	for attempt := 0; attempt < g.maxAttempts(); attempt++ {
		key, release, err := g.pool.Acquire(ctx)
		if err != nil {
			var noKey *pool.ErrNoKeyAvailable
			if errors.As(err, &noKey) {
				return attemptResult{noKey: noKey, lastErr: lastErr, lastErrMsg: lastErrMsg}
			}
			return attemptResult{poolErr: err, lastErr: lastErr, lastErrMsg: lastErrMsg}
		}

		attemptLog := &requestLog{keyID: key.ID(), modelRequested: lg.modelRequested, modelUpstream: lg.modelUpstream, start: time.Now()}
		actx, cancel := context.WithTimeout(ctx, time.Duration(g.cfg.Upstream.TimeoutSeconds)*time.Second)
		resp, ue, doErr := g.doUpstream(actx, key, method, path, body, stream)
		if doErr != nil {
			cancel()
			if ctx.Err() != nil {
				release()
				attemptLog.status = 499
				attemptLog.errMsg = "client canceled"
				g.finishLog(attemptLog)
				return attemptResult{canceled: true}
			}
			key.ReportFailure(0, 0, false, time.Time{}, doErr.Error())
			release()
			attemptLog.status = http.StatusBadGateway
			attemptLog.errMsg = doErr.Error()
			g.finishLog(attemptLog)
			lastErrMsg = doErr.Error()
			continue
		}
		if ue != nil {
			cancel()
			key.ReportFailure(ue.status, ue.retryAfter, ue.quota, ue.quotaReset, strings.TrimSpace(string(ue.body)))
			release()
			attemptLog.status = ue.status
			attemptLog.errMsg = strings.TrimSpace(string(ue.body))
			g.finishLog(attemptLog)
			lastErr = ue
			continue
		}

		// success — the caller serves the body first, then reports success
		// and cancels: cancelling earlier kills an in-flight SSE stream.
		return attemptResult{resp: resp, key: key, release: release, cancel: cancel}
	}
	return attemptResult{lastErr: lastErr, lastErrMsg: lastErrMsg}
}

func proxyErrorBody(w http.ResponseWriter, ue *upstreamError) {
	status := ue.status
	if status < 400 {
		status = http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "application/json")
	if ue.retryAfter > 0 {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", int(ue.retryAfter.Seconds())))
	}
	w.WriteHeader(status)
	if len(ue.body) > 0 {
		_, _ = w.Write(ue.body)
	} else {
		_, _ = w.Write([]byte(`{"error":{"message":"upstream error"}}`))
	}
}

func (g *Gateway) serveJSON(w http.ResponseWriter, resp *http.Response, lg *requestLog) {
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{"message": "cannot read upstream response"}})
		return
	}
	var parsed struct {
		Usage usagePayload `json:"usage"`
	}
	if json.Unmarshal(b, &parsed) == nil {
		lg.promptTokens = parsed.Usage.PromptTokens
		lg.cachedTokens = parsed.Usage.cachedTokens()
		lg.completionTokens = parsed.Usage.CompletionTokens
		lg.totalTokens = parsed.Usage.TotalTokens
	}
	for k, vs := range resp.Header {
		if strings.EqualFold(k, "Content-Length") {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(b)
}

func (g *Gateway) serveStream(w http.ResponseWriter, r *http.Request, resp *http.Response, lg *requestLog) {
	defer resp.Body.Close()
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"message": "streaming not supported"}})
		return
	}
	for k, vs := range resp.Header {
		if strings.EqualFold(k, "Content-Length") {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	flusher.Flush()

	scanner := bufio.NewScanner(resp.Body)

	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()

		if bytes.HasPrefix(line, []byte("data:")) {
			data := bytes.TrimSpace(line[len("data:"):])
			if len(data) > 0 && !bytes.Equal(data, []byte("[DONE]")) {
				var chunk struct {
					Usage *usagePayload `json:"usage"`
				}
				if json.Unmarshal(data, &chunk) == nil && chunk.Usage != nil {
					lg.promptTokens = chunk.Usage.PromptTokens
					lg.cachedTokens = chunk.Usage.cachedTokens()
					lg.completionTokens = chunk.Usage.CompletionTokens
					lg.totalTokens = chunk.Usage.TotalTokens
				}
			}
		}
		_, _ = w.Write(line)
		_, _ = w.Write([]byte("\n"))
		flusher.Flush()
		if r.Context().Err() != nil {
			return
		}
	}
}

func (g *Gateway) handleModels(w http.ResponseWriter, r *http.Request) {
	if !g.auth(w, r) {
		return
	}
	names := map[string]bool{}
	for req := range g.ModelMap() {
		names[req] = true
	}

	for _, up := range g.ModelMap() {
		names[up] = true
	}
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	list := []model{}
	for name := range names {
		list = append(list, model{ID: name, Object: "model", Created: time.Now().Unix(), OwnedBy: "llm-gateway"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": list})
}
