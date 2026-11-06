package store

import (
	"path/filepath"
	"testing"
	"time"
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

func TestLogFiltersAndTokensByKey(t *testing.T) {
	s := testStore(t)
	now := time.Now()

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
			if st.KeyName != "k1" || st.TotalTokens != 10 || st.Errors != 1 {
				t.Fatalf("k1 stats wrong: %+v", st)
			}
		case "k2":
			if st.KeyName != "k2" || st.TotalTokens != 25 {
				t.Fatalf("k2 stats wrong: %+v", st)
			}
		}
	}
}
