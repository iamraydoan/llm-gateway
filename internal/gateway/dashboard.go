package gateway

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
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
	if byKey == nil {
		byKey = []store.KeyTokenStat{}
	}
	return map[string]any{
		"total_requests": st.TotalRequests,
		"total_errors":   st.TotalErrors,
		"total_tokens":   st.TotalTokens,
		"by_key":         st.ByKey,
		"by_model":       st.ByModel,
		"by_status":      st.ByStatus,
		"tokens_by_key":  byKey,
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

type keyInput struct {
	ID             string                 `json:"id"`
	Name           string                 `json:"name"`
	APIKey         string                 `json:"api_key"`
	Proxy          string                 `json:"proxy"`
	Priority       int                    `json:"priority"`
	MaxConcurrency int                    `json:"max_concurrency"`
	Enabled        *bool                  `json:"enabled"`
	RateLimits     []config.RateLimitRule `json:"rate_limits"`
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

func slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = slugRe.ReplaceAllString(s, "-")
	return strings.Trim(s, "-")
}

func (in *keyInput) toConfig(existing *config.Key) (config.Key, error) {
	var k config.Key
	if existing != nil {
		k = *existing
	}
	if in.ID != "" {
		k.ID = strings.TrimSpace(in.ID)
	}
	if k.ID == "" {
		if s := slugify(in.Name); s != "" {
			k.ID = s
		} else {
			k.ID = fmt.Sprintf("key-%d", time.Now().Unix())
		}
	}
	if in.Name != "" {
		k.Name = strings.TrimSpace(in.Name)
	}
	if k.Name == "" {
		k.Name = k.ID
	}

	if in.APIKey != "" {
		k.APIKey = in.APIKey
	}
	if k.APIKey == "" {
		return k, fmt.Errorf("api_key is required")
	}
	k.Proxy = strings.TrimSpace(in.Proxy)
	k.Priority = in.Priority
	k.MaxConcurrency = in.MaxConcurrency
	if in.Enabled != nil {
		k.Enabled = *in.Enabled
	} else if existing == nil {
		k.Enabled = true
	}
	k.RateLimits = nil
	for _, rl := range in.RateLimits {
		if rl.Max <= 0 {
			return k, fmt.Errorf("rate limit max must be > 0")
		}
		d, err := config.ParseWindowDuration(rl.Window)
		if err != nil {
			return k, fmt.Errorf("rate limit window %q: %w", rl.Window, err)
		}
		k.RateLimits = append(k.RateLimits, config.RateLimitRule{Max: rl.Max, Window: rl.Window, Duration: d})
	}
	return k, nil
}

func (g *Gateway) handleKeyCreate(w http.ResponseWriter, r *http.Request) {
	var in keyInput
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON body"})
		return
	}
	k, err := in.toConfig(nil)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if g.pool.Find(k.ID) != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "key id already exists: " + k.ID})
		return
	}
	if err := g.store.UpsertKey(k); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := g.syncPool(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	g.hub.NotifyKeys()
	writeJSON(w, http.StatusCreated, map[string]any{"id": k.ID, "name": k.Name})
}

func (g *Gateway) handleKeyUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing id query param"})
		return
	}
	keys, err := g.store.ListKeys()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	var existing *config.Key
	for i := range keys {
		if keys[i].ID == id {
			existing = &keys[i]
			break
		}
	}
	if existing == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "key not found: " + id})
		return
	}
	var in keyInput
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON body"})
		return
	}

	in.ID = id
	k, err := in.toConfig(existing)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := g.store.UpsertKey(k); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := g.syncPool(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	g.hub.NotifyKeys()
	writeJSON(w, http.StatusOK, map[string]any{"id": k.ID, "name": k.Name})
}

func (g *Gateway) handleKeyDelete(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing id query param"})
		return
	}
	deleted, err := g.store.DeleteKey(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if !deleted {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "key not found: " + id})
		return
	}
	if err := g.syncPool(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	g.hub.NotifyKeys()
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "deleted": true})
}

func (g *Gateway) handleAPIKeyToggle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := r.URL.Query().Get("id")
	enabledStr := r.URL.Query().Get("enabled")
	k := g.pool.Find(id)
	if k == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "key not found"})
		return
	}
	enabled, err := strconv.ParseBool(enabledStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "enabled must be true/false"})
		return
	}
	cfg := k.Config()
	cfg.Enabled = enabled
	if err := g.store.UpsertKey(cfg); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := g.syncPool(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	g.hub.NotifyKeys()
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "enabled": enabled})
}

func (g *Gateway) handleAPIKeysRoute(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		g.handleAPIKeys(w, r)
	case http.MethodPost:
		g.handleKeyCreate(w, r)
	case http.MethodPut:
		g.handleKeyUpdate(w, r)
	case http.MethodDelete:
		g.handleKeyDelete(w, r)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func (g *Gateway) handleAPIModelMap(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		m, err := g.store.ListMappings()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"model_map": m})
	case http.MethodPost:
		var in struct {
			Requested string `json:"requested"`
			Upstream  string `json:"upstream"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON body"})
			return
		}
		in.Requested = strings.TrimSpace(in.Requested)
		in.Upstream = strings.TrimSpace(in.Upstream)
		if in.Requested == "" || in.Upstream == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "requested and upstream are required"})
			return
		}
		if err := g.store.UpsertMapping(in.Requested, in.Upstream); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		if err := g.refreshModelMap(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"requested": in.Requested, "upstream": in.Upstream})
	case http.MethodDelete:
		req := r.URL.Query().Get("requested")
		if req == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing requested query param"})
			return
		}
		deleted, err := g.store.DeleteMapping(req)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		if !deleted {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "mapping not found: " + req})
			return
		}
		if err := g.refreshModelMap(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"requested": req, "deleted": true})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}

func (g *Gateway) refreshModelMap() error {
	m, err := g.store.ListMappings()
	if err != nil {
		return err
	}
	g.SetModelMap(m)
	return nil
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
	mux.HandleFunc("/api/keys", g.handleAPIKeysRoute)
	mux.HandleFunc("/api/keys/toggle", g.handleAPIKeyToggle)
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
