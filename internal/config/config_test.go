package config

import "testing"

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
