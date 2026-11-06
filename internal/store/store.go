package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

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
	CachedTokens     int    `json:"cached_tokens"`
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
			cached_tokens INTEGER NOT NULL DEFAULT 0,
			completion_tokens INTEGER NOT NULL DEFAULT 0,
			total_tokens INTEGER NOT NULL DEFAULT 0,
			stream INTEGER NOT NULL DEFAULT 0,
			error TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_requests_ts ON requests(ts DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_requests_key_ts ON requests(key_id, ts DESC)`,
		`CREATE TABLE IF NOT EXISTS key_state (
			key_id TEXT PRIMARY KEY,
			fails INTEGER NOT NULL DEFAULT 0,
			cooldown_until TEXT NOT NULL DEFAULT '',
			quota_reset_at TEXT NOT NULL DEFAULT '',
			last_error TEXT NOT NULL DEFAULT '',
			last_error_at TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS key_rate_hits (
			key_id TEXT NOT NULL,
			window_seconds INTEGER NOT NULL,
			ts TEXT NOT NULL,
			PRIMARY KEY (key_id, window_seconds, ts)
		)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			return nil, err
		}
	}
	if _, err := db.Exec(`ALTER TABLE requests ADD COLUMN cached_tokens INTEGER NOT NULL DEFAULT 0`); err != nil && !strings.Contains(err.Error(), "duplicate column") {
		return nil, err
	}
	return s, nil
}

func (s *Store) Log(e *Entry) (int64, error) {
	stream := 0
	if e.Stream {
		stream = 1
	}
	res, err := s.db.Exec(`INSERT INTO requests
		(ts, key_id, model_requested, model_upstream, status, latency_ms,
		 prompt_tokens, cached_tokens, completion_tokens, total_tokens, stream, error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.TS, e.KeyID, e.ModelRequested, e.ModelUpstream, e.Status, e.LatencyMs,
		e.PromptTokens, e.CachedTokens, e.CompletionTokens, e.TotalTokens, stream, e.Error)
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
		conds = append(conds, "status >= 400 AND status != 499")
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
		status, latency_ms, prompt_tokens, cached_tokens, completion_tokens, total_tokens, stream, error
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
			&e.Status, &e.LatencyMs, &e.PromptTokens, &e.CachedTokens, &e.CompletionTokens, &e.TotalTokens, &stream, &e.Error); err != nil {
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
	TotalRequests         int            `json:"total_requests"`
	TotalErrors           int            `json:"total_errors"`
	TotalTokens           int            `json:"total_tokens"`
	TotalPromptTokens     int            `json:"total_prompt_tokens"`
	TotalCachedTokens     int            `json:"total_cached_tokens"`
	TotalCompletionTokens int            `json:"total_completion_tokens"`
	ByKey                 map[string]int `json:"by_key"`
	ByModel               map[string]int `json:"by_model"`
	ByStatus              map[string]int `json:"by_status"`
}

func rangeWhere(from, to time.Time) (string, []any) {
	var conds []string
	var args []any
	if !from.IsZero() {
		conds = append(conds, "ts >= ?")
		args = append(args, from.Format(time.RFC3339Nano))
	}
	if !to.IsZero() {
		conds = append(conds, "ts <= ?")
		args = append(args, to.Format(time.RFC3339Nano))
	}
	if len(conds) == 0 {
		return "", args
	}
	return "WHERE " + strings.Join(conds, " AND "), args
}

func (s *Store) StatsSince(since time.Time) (Stats, error) {
	return s.StatsRange(since, time.Time{})
}

func (s *Store) StatsRange(from, to time.Time) (Stats, error) {
	st := Stats{
		ByKey:    map[string]int{},
		ByModel:  map[string]int{},
		ByStatus: map[string]int{},
	}
	where, args := rangeWhere(from, to)
	row := s.db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(CASE WHEN status >= 400 AND status != 499 THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(total_tokens), 0), COALESCE(SUM(prompt_tokens), 0),
		COALESCE(SUM(cached_tokens), 0), COALESCE(SUM(completion_tokens), 0) FROM requests `+where,
		args...)
	if err := row.Scan(&st.TotalRequests, &st.TotalErrors, &st.TotalTokens,
		&st.TotalPromptTokens, &st.TotalCachedTokens, &st.TotalCompletionTokens); err != nil {
		return st, fmt.Errorf("stats: %w", err)
	}
	for _, q := range []struct {
		sql string
		dst *map[string]int
	}{
		{`SELECT key_id, COUNT(*) FROM requests ` + where + ` GROUP BY key_id`, &st.ByKey},
		{`SELECT model_requested, COUNT(*) FROM requests ` + where + ` GROUP BY model_requested`, &st.ByModel},
		{`SELECT CAST(status AS TEXT), COUNT(*) FROM requests ` + where + ` GROUP BY status`, &st.ByStatus},
	} {
		rows, err := s.db.Query(q.sql, args...)
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
	CachedTokens     int    `json:"cached_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	TotalTokens      int    `json:"total_tokens"`
}

func (s *Store) TokensByKey(since time.Time) ([]KeyTokenStat, error) {
	return s.TokensByKeyRange(since, time.Time{})
}

func (s *Store) TokensByKeyRange(from, to time.Time) ([]KeyTokenStat, error) {
	where, args := rangeWhere(from, to)
	where = strings.Replace(where, "ts", "r.ts", -1)
	rows, err := s.db.Query(`SELECT r.key_id, r.key_id,
		COUNT(*), COALESCE(SUM(CASE WHEN r.status >= 400 AND r.status != 499 THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(r.prompt_tokens), 0), COALESCE(SUM(r.cached_tokens), 0),
		COALESCE(SUM(r.completion_tokens), 0), COALESCE(SUM(r.total_tokens), 0)
		FROM requests r
		`+where+` GROUP BY r.key_id ORDER BY COUNT(*) DESC`,
		args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KeyTokenStat
	for rows.Next() {
		var st KeyTokenStat
		if err := rows.Scan(&st.KeyID, &st.KeyName, &st.Requests, &st.Errors,
			&st.PromptTokens, &st.CachedTokens, &st.CompletionTokens, &st.TotalTokens); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s *Store) CountsByKeySince(since time.Time) (map[string]int, error) {
	rows, err := s.db.Query(`SELECT key_id, COUNT(*) FROM requests
		WHERE ts >= ? GROUP BY key_id`, since.Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, rows.Err()
}

type LastKeyError struct {
	Error string
	TS    string
}

type KeyState struct {
	KeyID         string
	Fails         int
	CooldownUntil time.Time
	QuotaResetAt  time.Time
	LastError     string
	LastErrorAt   time.Time
}

func encodeTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

func (s *Store) SaveKeyState(st KeyState) error {
	_, err := s.db.Exec(`INSERT INTO key_state
		(key_id, fails, cooldown_until, quota_reset_at, last_error, last_error_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(key_id) DO UPDATE SET
			fails = excluded.fails,
			cooldown_until = excluded.cooldown_until,
			quota_reset_at = excluded.quota_reset_at,
			last_error = excluded.last_error,
			last_error_at = excluded.last_error_at,
			updated_at = excluded.updated_at`,
		st.KeyID, st.Fails, encodeTime(st.CooldownUntil), encodeTime(st.QuotaResetAt),
		st.LastError, encodeTime(st.LastErrorAt), time.Now().Format(time.RFC3339Nano))
	return err
}

func (s *Store) LoadKeyStates() (map[string]KeyState, error) {
	rows, err := s.db.Query(`SELECT key_id, fails, cooldown_until, quota_reset_at, last_error, last_error_at
		FROM key_state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]KeyState{}
	for rows.Next() {
		var st KeyState
		var cooldown, quota, lastErrAt string
		if err := rows.Scan(&st.KeyID, &st.Fails, &cooldown, &quota, &st.LastError, &lastErrAt); err != nil {
			return nil, err
		}
		st.CooldownUntil = parseTime(cooldown)
		st.QuotaResetAt = parseTime(quota)
		st.LastErrorAt = parseTime(lastErrAt)
		out[st.KeyID] = st
	}
	return out, rows.Err()
}

func (s *Store) SaveRateHit(keyID string, windowSeconds int64, ts time.Time) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO key_rate_hits (key_id, window_seconds, ts)
		VALUES (?, ?, ?)`, keyID, windowSeconds, ts.Format(time.RFC3339Nano))
	return err
}

func (s *Store) LoadRateHits(keyID string, windowSeconds int64, since time.Time) ([]time.Time, error) {
	rows, err := s.db.Query(`SELECT ts FROM key_rate_hits
		WHERE key_id = ? AND window_seconds = ? AND ts >= ? ORDER BY ts ASC`,
		keyID, windowSeconds, since.Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []time.Time
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if t := parseTime(raw); !t.IsZero() {
			out = append(out, t)
		}
	}
	return out, rows.Err()
}

func (s *Store) DeleteRateHitsBefore(t time.Time) error {
	_, err := s.db.Exec(`DELETE FROM key_rate_hits WHERE ts < ?`, t.Format(time.RFC3339Nano))
	return err
}

func (s *Store) LastKeyErrors() (map[string]LastKeyError, error) {
	rows, err := s.db.Query(`SELECT key_id, error, ts FROM requests
		WHERE id IN (SELECT MAX(id) FROM requests GROUP BY key_id)
		AND status >= 400 AND status != 499 AND error <> ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]LastKeyError{}
	for rows.Next() {
		var k string
		var e LastKeyError
		if err := rows.Scan(&k, &e.Error, &e.TS); err != nil {
			return nil, err
		}
		out[k] = e
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
