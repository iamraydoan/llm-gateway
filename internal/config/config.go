package config

import (
	"fmt"
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
	Priority       int             `yaml:"priority"`
	MaxConcurrency int             `yaml:"max_concurrency"`
	Enabled        bool            `yaml:"enabled"`
	RateLimits     []RateLimitRule `yaml:"rate_limits"`
}

func (k Key) MaskedAPIKey() string {
	if len(k.APIKey) <= 8 {
		return "****"
	}
	return k.APIKey[:3] + "…****" + k.APIKey[len(k.APIKey)-4:]
}

type Server struct {
	Listen       string `yaml:"listen"`
	MasterKey    string `yaml:"master_key"`
	DBPath       string `yaml:"db_path"`
	MaxFailovers int    `yaml:"max_failovers"`
}

type Upstream struct {
	BaseURL        string `yaml:"base_url"`
	TimeoutSeconds int    `yaml:"timeout_seconds"`
}

type Config struct {
	Server   Server            `yaml:"server"`
	Upstream Upstream          `yaml:"upstream"`
	ModelMap map[string]string `yaml:"model_map"`
	Keys     []Key             `yaml:"keys"`

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

func (c *Config) Validate() error {
	if c.Server.Listen == "" {
		c.Server.Listen = ":8080"
	}
	if c.Upstream.BaseURL == "" {
		c.Upstream.BaseURL = "https://api.openai.com"
	}
	c.Upstream.BaseURL = strings.TrimSuffix(c.Upstream.BaseURL, "/")
	if c.Upstream.TimeoutSeconds <= 0 {
		c.Upstream.TimeoutSeconds = 120
	}
	if c.Server.MaxFailovers <= 0 {
		c.Server.MaxFailovers = 10
	}
	if len(c.Keys) == 0 {
		return fmt.Errorf("config: no keys defined")
	}
	seen := map[string]bool{}
	for i := range c.Keys {
		k := &c.Keys[i]
		if k.ID == "" {
			k.ID = fmt.Sprintf("key-%d", i+1)
		}
		if k.Name == "" {
			k.Name = k.ID
		}
		if seen[k.ID] {
			return fmt.Errorf("config: duplicate key id %q", k.ID)
		}
		seen[k.ID] = true
		if k.APIKey == "" {
			return fmt.Errorf("config: key %q has empty api_key", k.ID)
		}
		if k.MaxConcurrency < 0 {
			k.MaxConcurrency = 0
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
