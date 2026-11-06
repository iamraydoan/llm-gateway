package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestValidateRelayURL(t *testing.T) {
	cfg := &Config{
		Server:   Server{Listen: ":8080"},
		Upstream: Upstream{BaseURL: "https://api.openai.com", RelayURL: "https://relay.example.com"},
		Keys: []Key{
			{ID: "a", APIKey: "sk-a", RelayURL: "https://relay.example.com/"},
			{ID: "b", APIKey: "sk-b"},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate = %v", err)
	}
	if cfg.Keys[0].RelayURL != "https://relay.example.com" {
		t.Fatalf("relay_url = %q, want trailing slash trimmed", cfg.Keys[0].RelayURL)
	}
	for _, bad := range []string{"notaurl", "ftp://relay.example.com", "https://"} {
		cfg.Upstream.RelayURL = bad
		if err := cfg.Validate(); err == nil {
			t.Fatalf("validate(%q) = nil, want error", bad)
		}
	}
}

func parseConfig(t *testing.T, text string) *Config {
	t.Helper()
	var cfg Config
	if err := yaml.Unmarshal([]byte(text), &cfg); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	return &cfg
}

func TestKeyDefaults(t *testing.T) {
	cfg := parseConfig(t, `
server:
  listen: ":8080"
upstream:
  base_url: "https://api.openai.com"
key_defaults:
  priority: 1
  max_concurrency: 1
  enabled: true
  rate_limits:
    - max: 2
      window: 1m
    - max: 25
      window: 1h
keys:
  - sk-aaa
  - sk-bbb
  - api_key: sk-ccc
    enabled: false
  - api_key: sk-ddd
    priority: 0
  - api_key: sk-eee
    max_concurrency: 0
`)
	if len(cfg.Keys) != 5 {
		t.Fatalf("keys = %d, want 5", len(cfg.Keys))
	}
	want := func(k Key) {
		if k.Priority != 1 || k.MaxConcurrency != 1 || !k.Enabled {
			t.Fatalf("key %+v: want priority 1, max_concurrency 1, enabled true", k)
		}
		if len(k.RateLimits) != 2 || k.RateLimits[0].Max != 2 || k.RateLimits[0].Duration != time.Minute || k.RateLimits[1].Max != 25 || k.RateLimits[1].Duration != time.Hour {
			t.Fatalf("key %+v: want rate limits 2/1m + 25/1h", k)
		}
	}
	want(cfg.Keys[0])
	want(cfg.Keys[1])
	if cfg.Keys[0].ID != "key-1" || cfg.Keys[0].Name != "key-1" || cfg.Keys[0].APIKey != "sk-aaa" {
		t.Fatalf("key 0 = %+v, want auto id/name key-1 and api_key sk-aaa", cfg.Keys[0])
	}
	if cfg.Keys[2].Enabled {
		t.Fatalf("key 2: enabled = true, want explicit false to win over defaults")
	}
	if cfg.Keys[3].Priority != 0 {
		t.Fatalf("key 3: priority = %d, want explicit 0 to win over defaults", cfg.Keys[3].Priority)
	}
	if cfg.Keys[4].MaxConcurrency != 0 {
		t.Fatalf("key 4: max_concurrency = %d, want explicit 0 (unlimited) to win over defaults", cfg.Keys[4].MaxConcurrency)
	}
}

func TestKeyDefaultsPartial(t *testing.T) {
	cfg := parseConfig(t, `
upstream:
  base_url: "https://api.openai.com"
key_defaults:
  rate_limits:
    - max: 5
      window: 1m
keys:
  - sk-aaa
`)
	k := cfg.Keys[0]
	if len(k.RateLimits) != 1 || k.RateLimits[0].Max != 5 {
		t.Fatalf("key %+v: want rate limit from defaults", k)
	}
	if k.Priority != 0 || k.Enabled {
		t.Fatalf("key %+v: want untouched priority 0 / enabled false (defaults did not set them)", k)
	}
}

func TestKeyDefaultsRejectIdentity(t *testing.T) {
	for _, block := range []string{"api_key: sk-x", "id: d1", "name: d1"} {
		var cfg Config
		text := "upstream:\n  base_url: \"https://api.openai.com\"\nkey_defaults:\n  " + block + "\nkeys:\n  - sk-aaa\n"
		if err := yaml.Unmarshal([]byte(text), &cfg); err != nil {
			t.Fatalf("yaml: %v", err)
		}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("validate with key_defaults %q = nil, want error", block)
		}
	}
}

func TestKeyStringFormEmpty(t *testing.T) {
	var cfg Config
	text := "upstream:\n  base_url: \"https://api.openai.com\"\nkeys:\n  - \"\"\n"
	if err := yaml.Unmarshal([]byte(text), &cfg); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("validate with empty string key = nil, want error")
	}
}

func TestKeyAutoIDSkipsTaken(t *testing.T) {
	cfg := parseConfig(t, `
upstream:
  base_url: "https://api.openai.com"
key_defaults:
  enabled: true
keys:
  - sk-aaa
  - id: key-1
    api_key: sk-bbb
  - sk-ccc
`)
	want := []string{"key-2", "key-1", "key-3"}
	for i, id := range want {
		if cfg.Keys[i].ID != id {
			t.Fatalf("key %d id = %q, want %q", i, cfg.Keys[i].ID, id)
		}
	}
}

func TestKeyDuplicateIDStillErrors(t *testing.T) {
	var cfg Config
	text := "upstream:\n  base_url: \"https://api.openai.com\"\nkeys:\n  - id: key-1\n    api_key: sk-aaa\n  - id: key-1\n    api_key: sk-bbb\n"
	if err := yaml.Unmarshal([]byte(text), &cfg); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("validate with duplicate id = nil, want error")
	}
}

func TestErrorRulesConfig(t *testing.T) {
	cfg := parseConfig(t, `
server:
  listen: ":8080"
upstream:
  base_url: "https://tokenharbor.ai"
  error_rules:
    - path: "error.code"
      value: "server_busy"
      delay_seconds: 5
      cooldown_seconds: 300
keys:
  - "sk-test"
`)
	if len(cfg.Upstream.ErrorRules) != 1 {
		t.Fatalf("error_rules = %v, want 1 rule", cfg.Upstream.ErrorRules)
	}
	r := cfg.Upstream.ErrorRules[0]
	if r.Path != "error.code" || r.Value != "server_busy" || r.DelaySeconds != 5 || r.CooldownSeconds != 300 {
		t.Fatalf("rule = %+v, want server_busy 5s/300s", r)
	}
	for _, bad := range []string{
		`
server:
  listen: ":8080"
upstream:
  base_url: "https://tokenharbor.ai"
  error_rules:
    - path: ""
      value: "server_busy"
      delay_seconds: 5
keys:
  - "sk-test"
`,
		`
server:
  listen: ":8080"
upstream:
  base_url: "https://tokenharbor.ai"
  error_rules:
    - path: "error.code"
      value: "server_busy"
keys:
  - "sk-test"
`,
		`
server:
  listen: ":8080"
upstream:
  base_url: "https://tokenharbor.ai"
  error_rules:
    - path: "error.code"
      value: "server_busy"
      delay_seconds: -1
keys:
  - "sk-test"
`,
	} {
		var cfg Config
		if err := yaml.Unmarshal([]byte(bad), &cfg); err != nil {
			t.Fatalf("yaml: %v", err)
		}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("validate(%q) = nil, want error", bad)
		}
	}
}

func TestBackoffConfig(t *testing.T) {
	cfg := parseConfig(t, `
server:
  listen: ":8080"
  backoff:
    base_seconds: 2
    multiplier: 3
    max_seconds: 30
upstream:
  base_url: "https://api.openai.com"
keys:
  - "sk-test"
`)
	if b := cfg.Server.Backoff; b.BaseSeconds != 2 || b.Multiplier != 3 || b.MaxSeconds != 30 {
		t.Fatalf("backoff = %+v, want base 2 mult 3 max 30", b)
	}
	def := parseConfig(t, `
server:
  listen: ":8080"
upstream:
  base_url: "https://api.openai.com"
keys:
  - "sk-test"
`)
	if b := def.Server.Backoff; b.BaseSeconds != 5 || b.Multiplier != 2 || b.MaxSeconds != 600 {
		t.Fatalf("default backoff = %+v, want base 5 mult 2 max 600", b)
	}
	for _, bad := range []string{
		`
server:
  listen: ":8080"
  backoff:
    base_seconds: 5
    multiplier: 0.5
    max_seconds: 600
keys:
  - "sk-test"
`,
		`
server:
  listen: ":8080"
  backoff:
    base_seconds: 30
    multiplier: 2
    max_seconds: 10
keys:
  - "sk-test"
`,
	} {
		var cfg Config
		if err := yaml.Unmarshal([]byte(bad), &cfg); err != nil {
			t.Fatalf("yaml: %v", err)
		}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("validate(%q) = nil, want error", bad)
		}
	}
}
