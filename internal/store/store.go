package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"llm-gateway/internal/config"

	_ "modernc.org/sqlite"
)

type Entry struct {
	ID               int64  `json:"id"`
	TS               string `json:"ts"`
	KeyID            string `json:"key_id"`
	ModelRequested   string `json:"model_requested"`
	ModelUpstream    string `json:"model_upstream"`
	Status           int    `json:"status"`
	LatencyMs        int64  `json:"latency_ms"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	TotalTokens      int    `json:"total_tokens"`
	Stream           bool   `json:"stream"`
	Error            string `json:"error,omitempty"`
}

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS requests (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			ts TEXT NOT NULL,
			key_id TEXT NOT NULL,
			model_requested TEXT NOT NULL,
			model_upstream TEXT NOT NULL,
			status INTEGER NOT NULL,
			latency_ms INTEGER NOT NULL,
			prompt_tokens INTEGER NOT NULL DEFAULT 0,
			completion_tokens INTEGER NOT NULL DEFAULT 0,
			total_tokens INTEGER NOT NULL DEFAULT 0,
			stream INTEGER NOT NULL DEFAULT 0,
			error TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_requests_ts ON requests(ts DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_requests_key_ts ON requests(key_id, ts DESC)`,
		`CREATE TABLE IF NOT EXISTS keys (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL DEFAULT '',
			api_key TEXT NOT NULL,
			proxy TEXT NOT NULL DEFAULT '',
			priority INTEGER NOT NULL DEFAULT 1,
			max_concurrency INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			rate_limits TEXT NOT NULL DEFAULT '[]',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS model_map (
			requested TEXT PRIMARY KEY,
			upstream TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) UpsertKey(k config.Key) error {
	rl, err := json.Marshal(k.RateLimits)
	if err != nil {
		return err
	}
	now := time.Now().Format(time.RFC3339)
	_, err = s.db.Exec(`INSERT INTO keys
		(id, name, api_key, proxy, priority, max_concurrency, enabled, rate_limits, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name=excluded.name, api_key=excluded.api_key, proxy=excluded.proxy,
			priority=excluded.priority, max_concurrency=excluded.max_concurrency,
			enabled=excluded.enabled, rate_limits=excluded.rate_limits,
			updated_at=excluded.updated_at`,
		k.ID, k.Name, k.APIKey, k.Proxy, k.Priority, k.MaxConcurrency, boolToInt(k.Enabled), string(rl), now, now)
	return err
}

func (s *Store) DeleteKey(id string) (bool, error) {
	res, err := s.db.Exec(`DELETE FROM keys WHERE id = ?`, id)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) ListKeys() ([]config.Key, error) {
	rows, err := s.db.Query(`SELECT id, name, api_key, proxy, priority,
		max_concurrency, enabled, rate_limits FROM keys ORDER BY priority ASC, id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []config.Key
	for rows.Next() {
		var k config.Key
		var enabled int
		var rlJSON string
		if err := rows.Scan(&k.ID, &k.Name, &k.APIKey, &k.Proxy, &k.Priority,
			&k.MaxConcurrency, &enabled, &rlJSON); err != nil {
			return nil, err
		}
		k.Enabled = enabled == 1
		if err := json.Unmarshal([]byte(rlJSON), &k.RateLimits); err != nil {
			return nil, fmt.Errorf("key %q: bad rate_limits: %w", k.ID, err)
		}

		for i := range k.RateLimits {
			d, err := config.ParseWindowDuration(k.RateLimits[i].Window)
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", k.ID, err)
			}
			k.RateLimits[i].Duration = d
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (s *Store) KeyCount() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM keys`).Scan(&n)
	return n, err
}

func (s *Store) UpsertMapping(requested, upstream string) error {
	now := time.Now().Format(time.RFC3339)
	_, err := s.db.Exec(`INSERT INTO model_map (requested, upstream, updated_at)
		VALUES (?, ?, ?) ON CONFLICT(requested) DO UPDATE SET
		upstream=excluded.upstream, updated_at=excluded.updated_at`,
		requested, upstream, now)
	return err
}

func (s *Store) DeleteMapping(requested string) (bool, error) {
	res, err := s.db.Exec(`DELETE FROM model_map WHERE requested = ?`, requested)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) ListMappings() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT requested, upstream FROM model_map ORDER BY requested ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var req, up string
		if err := rows.Scan(&req, &up); err != nil {
			return nil, err
		}
		out[req] = up
	}
	return out, rows.Err()
}

func (s *Store) MappingCount() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM model_map`).Scan(&n)
	return n, err
}

func (s *Store) Log(e *Entry) (int64, error) {
	stream := 0
	if e.Stream {
		stream = 1
	}
	res, err := s.db.Exec(`INSERT INTO requests
		(ts, key_id, model_requested, model_upstream, status, latency_ms,
		 prompt_tokens, completion_tokens, total_tokens, stream, error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.TS, e.KeyID, e.ModelRequested, e.ModelUpstream, e.Status, e.LatencyMs,
		e.PromptTokens, e.CompletionTokens, e.TotalTokens, stream, e.Error)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

type LogFilter struct {
	From   time.Time
	To     time.Time
	KeyID  string
	Model  string
	Status int
	Errors bool
}

func (f LogFilter) where() (string, []any) {
	var conds []string
	var args []any
	if !f.From.IsZero() {
		conds = append(conds, "ts >= ?")
		args = append(args, f.From.Format(time.RFC3339Nano))
	}
	if !f.To.IsZero() {
		conds = append(conds, "ts <= ?")
		args = append(args, f.To.Format(time.RFC3339Nano))
	}
	if f.KeyID != "" {
		conds = append(conds, "key_id = ?")
		args = append(args, f.KeyID)
	}
	if f.Model != "" {
		conds = append(conds, "(model_requested = ? OR model_upstream = ?)")
		args = append(args, f.Model, f.Model)
	}
	if f.Status != 0 {
		conds = append(conds, "status = ?")
		args = append(args, f.Status)
	}
	if f.Errors {
		conds = append(conds, "status >= 400")
	}
	if len(conds) == 0 {
		return "", args
	}
	return "WHERE " + strings.Join(conds, " AND "), args
}

func (s *Store) Recent(f LogFilter, limit, offset int) ([]Entry, error) {
	where, args := f.where()
	args = append(args, limit, offset)
	rows, err := s.db.Query(`SELECT id, ts, key_id, model_requested, model_upstream,
		status, latency_ms, prompt_tokens, completion_tokens, total_tokens, stream, error
		FROM requests `+where+` ORDER BY id DESC LIMIT ? OFFSET ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var e Entry
		var stream int
		if err := rows.Scan(&e.ID, &e.TS, &e.KeyID, &e.ModelRequested, &e.ModelUpstream,
			&e.Status, &e.LatencyMs, &e.PromptTokens, &e.CompletionTokens, &e.TotalTokens, &stream, &e.Error); err != nil {
			return nil, err
		}
		e.Stream = stream == 1
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) CountLogs(f LogFilter) (int, error) {
	where, args := f.where()
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM requests `+where, args...).Scan(&n)
	return n, err
}

type Stats struct {
	TotalRequests int            `json:"total_requests"`
	TotalErrors   int            `json:"total_errors"`
	TotalTokens   int            `json:"total_tokens"`
	ByKey         map[string]int `json:"by_key"`
	ByModel       map[string]int `json:"by_model"`
	ByStatus      map[string]int `json:"by_status"`
}

func (s *Store) StatsSince(since time.Time) (Stats, error) {
	st := Stats{
		ByKey:    map[string]int{},
		ByModel:  map[string]int{},
		ByStatus: map[string]int{},
	}
	row := s.db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(total_tokens), 0) FROM requests WHERE ts >= ?`,
		since.Format(time.RFC3339Nano))
	if err := row.Scan(&st.TotalRequests, &st.TotalErrors, &st.TotalTokens); err != nil {
		return st, fmt.Errorf("stats: %w", err)
	}
	for _, q := range []struct {
		sql string
		dst *map[string]int
	}{
		{`SELECT key_id, COUNT(*) FROM requests WHERE ts >= ? GROUP BY key_id`, &st.ByKey},
		{`SELECT model_requested, COUNT(*) FROM requests WHERE ts >= ? GROUP BY model_requested`, &st.ByModel},
		{`SELECT CAST(status AS TEXT), COUNT(*) FROM requests WHERE ts >= ? GROUP BY status`, &st.ByStatus},
	} {
		rows, err := s.db.Query(q.sql, since.Format(time.RFC3339Nano))
		if err != nil {
			return st, err
		}
		for rows.Next() {
			var k string
			var n int
			if err := rows.Scan(&k, &n); err != nil {
				rows.Close()
				return st, err
			}
			(*q.dst)[k] = n
		}
		rows.Close()
	}
	return st, nil
}

type KeyTokenStat struct {
	KeyID            string `json:"key_id"`
	KeyName          string `json:"key_name"`
	Requests         int    `json:"requests"`
	Errors           int    `json:"errors"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	TotalTokens      int    `json:"total_tokens"`
}

func (s *Store) TokensByKey(since time.Time) ([]KeyTokenStat, error) {
	rows, err := s.db.Query(`SELECT r.key_id, COALESCE(k.name, r.key_id),
		COUNT(*), COALESCE(SUM(CASE WHEN r.status >= 400 THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(r.prompt_tokens), 0), COALESCE(SUM(r.completion_tokens), 0),
		COALESCE(SUM(r.total_tokens), 0)
		FROM requests r LEFT JOIN keys k ON k.id = r.key_id
		WHERE r.ts >= ? GROUP BY r.key_id ORDER BY COUNT(*) DESC`,
		since.Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KeyTokenStat
	for rows.Next() {
		var st KeyTokenStat
		if err := rows.Scan(&st.KeyID, &st.KeyName, &st.Requests, &st.Errors,
			&st.PromptTokens, &st.CompletionTokens, &st.TotalTokens); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s *Store) Close() error { return s.db.Close() }

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
