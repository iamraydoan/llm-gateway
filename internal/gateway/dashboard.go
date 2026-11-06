package gateway

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"llm-gateway/internal/config"
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

func (g *Gateway) keyStatuses(now time.Time) []pool.Status {
	stats := make([]pool.Status, 0, len(g.pool.Keys()))
	for _, k := range g.pool.Keys() {
		st := k.Status(now)
		st.SkipReason = kStatusReason(k, now)
		stats = append(stats, st)
	}
	g.enrichKeyStatuses(stats, now)
	return stats
}

func (g *Gateway) enrichKeyStatuses(stats []pool.Status, now time.Time) {
	if g.store == nil {
		return
	}
	if totals, err := g.store.TokensByKey(time.Time{}); err == nil {
		byID := make(map[string]store.KeyTokenStat, len(totals))
		for _, t := range totals {
			byID[t.KeyID] = t
		}
		for i := range stats {
			if t, ok := byID[stats[i].ID]; ok {
				if succ := t.Requests - t.Errors; succ > stats[i].TotalRequests {
					stats[i].TotalRequests = succ
				}
				if t.Errors > stats[i].TotalErrors {
					stats[i].TotalErrors = t.Errors
				}
			}
		}
	}
	if lastErrs, err := g.store.LastKeyErrors(); err == nil {
		for i := range stats {
			if stats[i].LastError == "" {
				if e, ok := lastErrs[stats[i].ID]; ok {
					stats[i].LastError = e.Error
					if t, err := time.Parse(time.RFC3339Nano, e.TS); err == nil {
						stats[i].LastErrorAt = t.Format(time.RFC3339)
					}
				}
			}
		}
	}
	windows := map[time.Duration]bool{}
	for i := range stats {
		for _, rl := range stats[i].RateLimits {
			if d, err := config.ParseWindowDuration(rl.Window); err == nil && d > 0 {
				windows[d] = true
			}
		}
	}
	for d := range windows {
		counts, err := g.store.CountsByKeySince(now.Add(-d))
		if err != nil {
			continue
		}
		for i := range stats {
			for j := range stats[i].RateLimits {
				wd, err := config.ParseWindowDuration(stats[i].RateLimits[j].Window)
				if err != nil || wd != d {
					continue
				}
				used := stats[i].RateLimits[j].Max - stats[i].RateLimits[j].Remaining
				if c := counts[stats[i].ID]; c > used {
					used = c
				}
				rem := stats[i].RateLimits[j].Max - used
				if rem < 0 {
					rem = 0
				}
				stats[i].RateLimits[j].Remaining = rem
			}
		}
	}
}

func (g *Gateway) keysSnapshot() []byte {
	stats := g.keyStatuses(time.Now())
	payload, err := json.Marshal(map[string]any{"keys": stats})
	if err != nil {
		return nil
	}
	return payload
}

func (g *Gateway) statsPayload(from, to time.Time, hours int) (map[string]any, error) {
	var st store.Stats
	var byKey []store.KeyTokenStat
	var err error
	if !from.IsZero() || !to.IsZero() {
		st, err = g.store.StatsRange(from, to)
		if err != nil {
			return nil, err
		}
		byKey, err = g.store.TokensByKeyRange(from, to)
	} else if hours < 0 {
		st, err = g.store.StatsRange(time.Time{}, time.Time{})
		if err != nil {
			return nil, err
		}
		byKey, err = g.store.TokensByKeyRange(time.Time{}, time.Time{})
	} else {
		if hours <= 0 {
			hours = 24
		}
		since := time.Now().Add(-time.Duration(hours) * time.Hour)
		st, err = g.store.StatsSince(since)
		if err != nil {
			return nil, err
		}
		byKey, err = g.store.TokensByKey(since)
	}
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
	payload, err := g.statsPayload(time.Time{}, time.Time{}, hours)
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
	writeJSON(w, http.StatusOK, map[string]any{"keys": g.keyStatuses(time.Now())})
}

func kStatusReason(k *pool.Key, now time.Time) string {
	st := k.Status(now)
	if !st.Enabled {
		return "disabled"
	}
	if st.QuotaResetAt != "" {
		return "quota-exhausted"
	}
	if st.CooldownUntil != "" {
		return "cooldown"
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
	if h, err := strconv.Atoi(q.Get("hours")); err == nil && h > 0 {
		now := time.Now()
		from, to = now.Add(-time.Duration(h)*time.Hour), now
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
	q := r.URL.Query()
	hours, _ := strconv.Atoi(q.Get("hours"))
	if q.Has("hours") && hours == 0 {
		hours = -1
	}
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
	payload, err := g.statsPayload(from, to, hours)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func (g *Gateway) handleAPIConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"upstream":  g.cfg.Upstream.BaseURL,
		"relay_url": g.cfg.Upstream.RelayURL,
		"model_map": g.ModelMap(),
		"listen":    g.cfg.Server.Listen,
		"backoff": map[string]any{
			"base_seconds": g.cfg.Server.Backoff.BaseSeconds,
			"multiplier":   g.cfg.Server.Backoff.Multiplier,
			"max_seconds":  g.cfg.Server.Backoff.MaxSeconds,
		},
	})
}

func (g *Gateway) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/chat/completions", g.passthrough)
	mux.HandleFunc("/v1/embeddings", g.passthrough)
	mux.HandleFunc("/v1/completions", g.passthrough)
	mux.HandleFunc("/v1/messages", g.claudeMessages)
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
