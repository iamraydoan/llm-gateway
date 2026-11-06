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

	if n, err := st.KeyCount(); err != nil {
		log.Fatalf("store: %v", err)
	} else if n == 0 && len(cfg.Keys) > 0 {
		for _, k := range cfg.Keys {
			if err := st.UpsertKey(k); err != nil {
				log.Fatalf("seed key %q: %v", k.ID, err)
			}
		}
		fmt.Printf("seeded %d keys from %s into %s\n", len(cfg.Keys), *configPath, dbPath)
	}
	if n, err := st.MappingCount(); err != nil {
		log.Fatalf("store: %v", err)
	} else if n == 0 && len(cfg.ModelMap) > 0 {
		for req, up := range cfg.ModelMap {
			if err := st.UpsertMapping(req, up); err != nil {
				log.Fatalf("seed mapping %q: %v", req, err)
			}
		}
		fmt.Printf("seeded %d model mappings from %s\n", len(cfg.ModelMap), *configPath)
	}

	keys, err := st.ListKeys()
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	if len(keys) == 0 {
		log.Fatalf("no keys: add some in config.yaml or via the dashboard")
	}
	p, err := pool.NewFromKeys(keys, time.Duration(cfg.Upstream.TimeoutSeconds)*time.Second)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	mappings, err := st.ListMappings()
	if err != nil {
		log.Fatalf("store: %v", err)
	}

	gw := gateway.New(cfg, p, st, mappings)
	fmt.Printf("llm-gateway listening on %s\n", cfg.Server.Listen)
	fmt.Printf("upstream: %s | keys: %d | dashboard: http://localhost%s/\n",
		cfg.Upstream.BaseURL, len(keys), cfg.Server.Listen)
	if err := http.ListenAndServe(cfg.Server.Listen, gw.Routes()); err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(1)
	}
}
