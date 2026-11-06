// llm-gateway: a minimal OpenAI-compatible gateway that routes requests
// through an ordered pool of API keys, each with its own proxy, concurrency
// limit and multi-window rate limits. Keys that hit rate limits are
// blacklisted with exponential backoff; quota exhaustion is tracked until
// its reset time. Every request is logged to SQLite and shown on a
// built-in dashboard.
//
// Keys and model mappings come from config.yaml only (edit the file and
// restart to change them). The SQLite database stores request logs only.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"llm-gateway/internal/config"
	"llm-gateway/internal/gateway"
	"llm-gateway/internal/pool"
	"llm-gateway/internal/store"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	dbPath := cfg.Server.DBPath
	if v := os.Getenv("LLM_GATEWAY_DB"); v != "" {
		dbPath = v
	}
	if dbPath == "" {
		dbPath = filepath.Join(filepath.Dir(*configPath), "gateway.db")
	}
	if abs, err := filepath.Abs(dbPath); err == nil {
		dbPath = abs
	}
	st, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	if len(cfg.Keys) == 0 {
		log.Fatalf("no keys: add some in config.yaml")
	}
	p, err := pool.NewFromKeys(cfg.Keys, time.Duration(cfg.Upstream.TimeoutSeconds)*time.Second)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}

	gw := gateway.New(cfg, p, st, cfg.ModelMap)
	fmt.Printf("llm-gateway listening on %s\n", cfg.Server.Listen)
	fmt.Printf("upstream: %s | keys: %d | dashboard: http://localhost%s/\n",
		cfg.Upstream.BaseURL, len(cfg.Keys), cfg.Server.Listen)
	if err := http.ListenAndServe(cfg.Server.Listen, gw.Routes()); err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(1)
	}
}
