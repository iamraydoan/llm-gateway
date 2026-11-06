package store

import (
	"path/filepath"
	"testing"
	"time"

	"llm-gateway/internal/config"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestKeysCRUD(t *testing.T) {
	s := testStore(t)
	if n, _ := s.KeyCount(); n != 0 {
		t.Fatalf("expected empty keys, got %d", n)
	}
	k := config.Key{ID: "k1", Name: "Main", APIKey: "sk-1", Priority: 1,
		MaxConcurrency: 5, Enabled: true,
		RateLimits: []config.RateLimitRule{{Max: 5, Window: "1m"}}}
	if err := s.UpsertKey(k); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	keys, err := s.ListKeys()
	if err != nil || len(keys) != 1 {
		t.Fatalf("list: %v %d", err, len(keys))
	}
	if keys[0].Name != "Main" || keys[0].RateLimits[0].Duration != time.Minute {
		t.Fatalf("bad roundtrip: %+v", keys[0])
	}

	k.Name = "Renamed"
	k.Enabled = false
	if err := s.UpsertKey(k); err != nil {
		t.Fatalf("upsert2: %v", err)
	}
	keys, _ = s.ListKeys()
	if keys[0].Name != "Renamed" || keys[0].Enabled {
		t.Fatalf("update did not stick: %+v", keys[0])
	}
	ok, err := s.DeleteKey("k1")
	if err != nil || !ok {
		t.Fatalf("delete: %v %v", err, ok)
	}
	if n, _ := s.KeyCount(); n != 0 {
		t.Fatalf("expected 0 keys after delete, got %d", n)
	}
}

func TestMappingsCRUD(t *testing.T) {
	s := testStore(t)
	if err := s.UpsertMapping("cheap", "gpt-4o-mini"); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := s.UpsertMapping("smart", "gpt-4o"); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	m, err := s.ListMappings()
	if err != nil || len(m) != 2 || m["cheap"] != "gpt-4o-mini" {
		t.Fatalf("list: %v %v", err, m)
	}
	ok, err := s.DeleteMapping("cheap")
	if err != nil || !ok {
		t.Fatalf("delete: %v %v", err, ok)
	}
	m, _ = s.ListMappings()
	if len(m) != 1 {
		t.Fatalf("expected 1 mapping left, got %v", m)
	}
}

func TestLogFiltersAndTokensByKey(t *testing.T) {
	s := testStore(t)
	now := time.Now()

	_ = s.UpsertKey(config.Key{ID: "k1", Name: "Alpha", APIKey: "sk-1", Enabled: true})
	_ = s.UpsertKey(config.Key{ID: "k2", Name: "Beta", APIKey: "sk-2", Enabled: true})
	entries := []Entry{
		{TS: now.Add(-2 * time.Hour).Format(time.RFC3339Nano), KeyID: "k1", ModelRequested: "cheap", ModelUpstream: "gpt-4o-mini", Status: 200, TotalTokens: 10},
		{TS: now.Add(-1 * time.Hour).Format(time.RFC3339Nano), KeyID: "k1", ModelRequested: "smart", ModelUpstream: "gpt-4o", Status: 429, TotalTokens: 0, Error: "rate limited"},
		{TS: now.Format(time.RFC3339Nano), KeyID: "k2", ModelRequested: "cheap", ModelUpstream: "gpt-4o-mini", Status: 200, TotalTokens: 25},
	}
	for i := range entries {
		if _, err := s.Log(&entries[i]); err != nil {
			t.Fatalf("log: %v", err)
		}
	}

	got, err := s.Recent(LogFilter{KeyID: "k1"}, 10, 0)
	if err != nil || len(got) != 2 {
		t.Fatalf("key filter: %v %d", err, len(got))
	}

	got, err = s.Recent(LogFilter{From: now.Add(-90 * time.Minute)}, 10, 0)
	if err != nil || len(got) != 2 {
		t.Fatalf("date filter: %v %d", err, len(got))
	}

	got, err = s.Recent(LogFilter{Errors: true}, 10, 0)
	if err != nil || len(got) != 1 || got[0].Status != 429 {
		t.Fatalf("errors filter: %v %+v", err, got)
	}

	n, err := s.CountLogs(LogFilter{})
	if err != nil || n != 3 {
		t.Fatalf("count: %v %d", err, n)
	}
	got, err = s.Recent(LogFilter{}, 2, 0)
	if err != nil || len(got) != 2 {
		t.Fatalf("page 1: %v %d", err, len(got))
	}
	got, err = s.Recent(LogFilter{}, 2, 2)
	if err != nil || len(got) != 1 {
		t.Fatalf("page 2: %v %d", err, len(got))
	}

	stats, err := s.TokensByKey(now.Add(-24 * time.Hour))
	if err != nil || len(stats) != 2 {
		t.Fatalf("tokens by key: %v %+v", err, stats)
	}
	for _, st := range stats {
		switch st.KeyID {
		case "k1":
			if st.KeyName != "Alpha" || st.TotalTokens != 10 || st.Errors != 1 {
				t.Fatalf("k1 stats wrong: %+v", st)
			}
		case "k2":
			if st.KeyName != "Beta" || st.TotalTokens != 25 {
				t.Fatalf("k2 stats wrong: %+v", st)
			}
		}
	}
}
