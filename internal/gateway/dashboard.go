package gateway

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"llm-gateway/internal/pool"
	"llm-gateway/internal/store"
)

//go:embed ui.html
var uiHTML []byte

func (g *Gateway) handleUI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(uiHTML)
}

func (g *Gateway) keysSnapshot() []byte {
	now := time.Now()
	stats := make([]pool.Status, 0, len(g.pool.Keys()))
	for _, k := range g.pool.Keys() {
		st := k.Status(now)
		st.SkipReason = kStatusReason(k, now)
		stats = append(stats, st)
	}
	payload, err := json.Marshal(map[string]any{"keys": stats})
	if err != nil {
		return nil
	}
	return payload
}

func (g *Gateway) statsPayload(hours int) (map[string]any, error) {
	if hours <= 0 {
		hours = 24
	}
	since := time.Now().Add(-time.Duration(hours) * time.Hour)
	st, err := g.store.StatsSince(since)
	if err != nil {
		return nil, err
	}
	byKey, err := g.store.TokensByKey(since)
	if err != nil {
		return nil, err
	}
	for i := range byKey {
		if k := g.pool.Find(byKey[i].KeyID); k != nil {
			byKey[i].KeyName = k.Name()
		}
	}
	if byKey == nil {
		byKey = []store.KeyTokenStat{}
	}
	return map[string]any{
		"total_requests":          st.TotalRequests,
		"total_errors":            st.TotalErrors,
		"total_tokens":            st.TotalTokens,
		"total_prompt_tokens":     st.TotalPromptTokens,
		"total_cached_tokens":     st.TotalCachedTokens,
		"total_completion_tokens": st.TotalCompletionTokens,
		"by_key":                  st.ByKey,
		"by_model":                st.ByModel,
		"by_status":               st.ByStatus,
		"tokens_by_key":           byKey,
	}, nil
}

func (g *Gateway) statsSnapshot(hours int) []byte {
	payload, err := g.statsPayload(hours)
	if err != nil {
		return nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return b
}

func (g *Gateway) handleAPIKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	now := time.Now()
	stats := make([]pool.Status, 0, len(g.pool.Keys()))
	for _, k := range g.pool.Keys() {
		st := k.Status(now)
		st.SkipReason = kStatusReason(k, now)
		stats = append(stats, st)
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": stats})
}

func kStatusReason(k *pool.Key, now time.Time) string {
	st := k.Status(now)
	if !st.Enabled {
		return "disabled"
	}
	if st.CooldownUntil != "" {
		return "cooldown"
	}
	if st.QuotaResetAt != "" {
		return "quota-exhausted"
	}
	for _, rl := range st.RateLimits {
		if rl.Remaining <= 0 {
			return "rate-limited"
		}
	}
	if st.MaxConcurrency > 0 && st.InFlight >= st.MaxConcurrency {
		return "busy"
	}
	return "ready"
}

func (g *Gateway) handleAPIModelMap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"model_map": g.ModelMap()})
}

func parseLogTime(v string, endOfDay bool) (time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{
		time.RFC3339Nano, time.RFC3339,
		"2006-01-02T15:04:05", "2006-01-02T15:04",
		"2006-01-02 15:04:05", "2006-01-02 15:04",
	} {
		if t, err := time.ParseInLocation(layout, v, time.Local); err == nil {
			return t, nil
		}
	}
	if t, err := time.ParseInLocation("2006-01-02", v, time.Local); err == nil {
		if endOfDay {
			return t.Add(24*time.Hour - time.Nanosecond), nil
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("bad time %q", v)
}

func (g *Gateway) handleAPILogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, err := parseLogTime(q.Get("from"), false)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	to, err := parseLogTime(q.Get("to"), true)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	status, _ := strconv.Atoi(q.Get("status"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	offset, _ := strconv.Atoi(q.Get("offset"))
	if offset < 0 {
		offset = 0
	}
	f := store.LogFilter{
		From:   from,
		To:     to,
		KeyID:  q.Get("key_id"),
		Model:  q.Get("model"),
		Status: status,
		Errors: q.Get("errors") == "true",
	}
	total, err := g.store.CountLogs(f)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	entries, err := g.store.Recent(f, limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if entries == nil {
		entries = []store.Entry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"logs": entries, "total": total, "limit": limit, "offset": offset,
	})
}

func (g *Gateway) handleAPIStats(w http.ResponseWriter, r *http.Request) {
	hours, _ := strconv.Atoi(r.URL.Query().Get("hours"))
	payload, err := g.statsPayload(hours)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func (g *Gateway) handleAPIConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"upstream":  g.cfg.Upstream.BaseURL,
		"model_map": g.ModelMap(),
		"listen":    g.cfg.Server.Listen,
	})
}

func (g *Gateway) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/chat/completions", g.passthrough)
	mux.HandleFunc("/v1/embeddings", g.passthrough)
	mux.HandleFunc("/v1/completions", g.passthrough)
	mux.HandleFunc("/v1/models", g.handleModels)

	mux.HandleFunc("/", g.handleUI)
	mux.HandleFunc("/api/keys", g.handleAPIKeys)
	mux.HandleFunc("/api/model-map", g.handleAPIModelMap)
	mux.HandleFunc("/api/logs", g.handleAPILogs)
	mux.HandleFunc("/api/stats", g.handleAPIStats)
	mux.HandleFunc("/api/config", g.handleAPIConfig)
	mux.HandleFunc("/api/events", g.handleAPIEvents)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}
