package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type RateLimitRule struct {
	Max    int    `yaml:"max"`
	Window string `yaml:"window"`

	Duration time.Duration `yaml:"-"`
}

type Key struct {
	ID             string          `yaml:"id"`
	Name           string          `yaml:"name"`
	APIKey         string          `yaml:"api_key"`
	Proxy          string          `yaml:"proxy"`
	RelayURL       string          `yaml:"relay_url"`
	Priority       int             `yaml:"priority"`
	MaxConcurrency int             `yaml:"max_concurrency"`
	Enabled        bool            `yaml:"enabled"`
	RateLimits     []RateLimitRule `yaml:"rate_limits"`

	set map[string]bool `yaml:"-"`
}

func (k *Key) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		k.APIKey = value.Value
		k.set = map[string]bool{"api_key": true}
		return nil
	}
	type plain Key
	var p plain
	if err := value.Decode(&p); err != nil {
		return err
	}
	*k = Key(p)
	k.set = map[string]bool{}
	for i := 0; i+1 < len(value.Content); i += 2 {
		k.set[value.Content[i].Value] = true
	}
	return nil
}

func (k *Key) applyDefaults(d Key) {
	if !k.set["priority"] && d.set["priority"] {
		k.Priority = d.Priority
	}
	if !k.set["max_concurrency"] && d.set["max_concurrency"] {
		k.MaxConcurrency = d.MaxConcurrency
	}
	if !k.set["enabled"] && d.set["enabled"] {
		k.Enabled = d.Enabled
	}
	if !k.set["proxy"] && d.set["proxy"] {
		k.Proxy = d.Proxy
	}
	if !k.set["relay_url"] && d.set["relay_url"] {
		k.RelayURL = d.RelayURL
	}
	if !k.set["rate_limits"] && d.set["rate_limits"] {
		k.RateLimits = append([]RateLimitRule(nil), d.RateLimits...)
	}
}

func (k Key) MaskedAPIKey() string {
	if len(k.APIKey) <= 8 {
		return "****"
	}
	return k.APIKey[:3] + "…****" + k.APIKey[len(k.APIKey)-4:]
}

type Backoff struct {
	BaseSeconds int     `yaml:"base_seconds"`
	Multiplier  float64 `yaml:"multiplier"`
	MaxSeconds  int     `yaml:"max_seconds"`
}

type Server struct {
	Listen       string  `yaml:"listen"`
	MasterKey    string  `yaml:"master_key"`
	DBPath       string  `yaml:"db_path"`
	MaxFailovers int     `yaml:"max_failovers"`
	Backoff      Backoff `yaml:"backoff"`
}

type ErrorRule struct {
	Path            string `yaml:"path"`
	Value           string `yaml:"value"`
	DelaySeconds    int    `yaml:"delay_seconds"`
	CooldownSeconds int    `yaml:"cooldown_seconds"`
}

type Upstream struct {
	BaseURL        string      `yaml:"base_url"`
	TimeoutSeconds int         `yaml:"timeout_seconds"`
	RelayURL       string      `yaml:"relay_url"`
	ErrorRules     []ErrorRule `yaml:"error_rules"`
}

type Config struct {
	Server      Server            `yaml:"server"`
	Upstream    Upstream          `yaml:"upstream"`
	ModelMap    map[string]string `yaml:"model_map"`
	KeyDefaults Key               `yaml:"key_defaults"`
	Keys        []Key             `yaml:"keys"`

	DBPath string `yaml:"-"`
}

func ParseWindowDuration(s string) (time.Duration, error) { return parseWindow(s) }

func parseWindow(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "d") {
		num := strings.TrimSuffix(s, "d")
		var days int
		if _, err := fmt.Sscanf(num, "%d", &days); err != nil {
			return 0, fmt.Errorf("bad window %q: %w", s, err)
		}
		if days <= 0 {
			return 0, fmt.Errorf("bad window %q: must be positive", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func validateRelayURL(field, v string) (string, error) {
	v = strings.TrimSuffix(strings.TrimSpace(v), "/")
	if v == "" {
		return "", nil
	}
	u, err := url.Parse(v)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("config: %s must be an http(s) URL, got %q", field, v)
	}
	return v, nil
}

func (c *Config) Validate() error {
	if c.Server.Listen == "" {
		c.Server.Listen = ":8080"
	}
	if c.Upstream.BaseURL == "" {
		c.Upstream.BaseURL = "https://api.openai.com"
	}
	c.Upstream.BaseURL = strings.TrimSuffix(c.Upstream.BaseURL, "/")
	var err error
	if c.Upstream.RelayURL, err = validateRelayURL("upstream.relay_url", c.Upstream.RelayURL); err != nil {
		return err
	}
	if c.Upstream.TimeoutSeconds <= 0 {
		c.Upstream.TimeoutSeconds = 120
	}
	for i, r := range c.Upstream.ErrorRules {
		if r.Path == "" {
			return fmt.Errorf("config: upstream.error_rules[%d] must set path", i)
		}
		if r.Value == "" {
			return fmt.Errorf("config: upstream.error_rules[%d] must set value", i)
		}
		if r.DelaySeconds < 0 || r.CooldownSeconds < 0 {
			return fmt.Errorf("config: upstream.error_rules[%d] delay_seconds and cooldown_seconds must not be negative", i)
		}
		if r.DelaySeconds == 0 && r.CooldownSeconds == 0 {
			return fmt.Errorf("config: upstream.error_rules[%d] must set delay_seconds or cooldown_seconds", i)
		}
	}
	if c.Server.MaxFailovers <= 0 {
		c.Server.MaxFailovers = 10
	}
	if c.Server.Backoff.BaseSeconds <= 0 {
		c.Server.Backoff.BaseSeconds = 5
	}
	if c.Server.Backoff.Multiplier <= 0 {
		c.Server.Backoff.Multiplier = 2
	}
	if c.Server.Backoff.MaxSeconds <= 0 {
		c.Server.Backoff.MaxSeconds = 600
	}
	if c.Server.Backoff.Multiplier < 1 {
		return fmt.Errorf("config: server.backoff.multiplier must be at least 1, got %v", c.Server.Backoff.Multiplier)
	}
	if c.Server.Backoff.MaxSeconds < c.Server.Backoff.BaseSeconds {
		return fmt.Errorf("config: server.backoff.max_seconds (%d) must be at least base_seconds (%d)", c.Server.Backoff.MaxSeconds, c.Server.Backoff.BaseSeconds)
	}
	for _, f := range []string{"id", "name", "api_key"} {
		if c.KeyDefaults.set[f] {
			return fmt.Errorf("config: key_defaults must not set %q (it is per key)", f)
		}
	}
	for i := range c.Keys {
		c.Keys[i].applyDefaults(c.KeyDefaults)
	}
	if len(c.Keys) == 0 {
		return fmt.Errorf("config: no keys defined")
	}
	seen := map[string]bool{}
	for i := range c.Keys {
		if c.Keys[i].ID != "" {
			if seen[c.Keys[i].ID] {
				return fmt.Errorf("config: duplicate key id %q", c.Keys[i].ID)
			}
			seen[c.Keys[i].ID] = true
		}
	}
	for i := range c.Keys {
		k := &c.Keys[i]
		if k.ID == "" {
			n := 1
			for seen[fmt.Sprintf("key-%d", n)] {
				n++
			}
			k.ID = fmt.Sprintf("key-%d", n)
			seen[k.ID] = true
		}
		if k.Name == "" {
			k.Name = k.ID
		}
		if k.APIKey == "" {
			return fmt.Errorf("config: key %q has empty api_key", k.ID)
		}
		if k.MaxConcurrency < 0 {
			k.MaxConcurrency = 0
		}
		if k.RelayURL, err = validateRelayURL("key "+k.ID+".relay_url", k.RelayURL); err != nil {
			return err
		}
		for j := range k.RateLimits {
			r := &k.RateLimits[j]
			if r.Max <= 0 {
				return fmt.Errorf("config: key %q rate limit max must be > 0", k.ID)
			}
			d, err := parseWindow(r.Window)
			if err != nil {
				return fmt.Errorf("config: key %q rate limit window: %w", k.ID, err)
			}
			r.Duration = d
		}
	}
	return nil
}

func (c *Config) MapModel(model string) string {
	if m, ok := c.ModelMap[model]; ok {
		return m
	}
	return model
}
