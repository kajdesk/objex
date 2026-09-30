package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Key struct {
	Name      string   `json:"name"`
	AccessKey string   `json:"access_key"`
	SecretKey string   `json:"secret_key"`
	Buckets   []string `json:"buckets,omitempty"`
	ReadOnly  bool     `json:"read_only,omitempty"`
}

func (k Key) CanAccess(bucket string) bool {
	if len(k.Buckets) == 0 {
		return true
	}
	for _, allowed := range k.Buckets {
		if allowed == "*" || allowed == bucket {
			return true
		}
	}
	return false
}

type Config struct {
	Listen         string `json:"listen"`
	DataDir        string `json:"data_dir"`
	Region         string `json:"region"`
	Domain         string `json:"domain,omitempty"`
	Fsync          bool   `json:"fsync"`
	MaxConnections int    `json:"max_connections"`
	Keys           []Key  `json:"keys"`
}

func Default() Config {
	return Config{
		Listen:         "0.0.0.0:9000",
		DataDir:        "./data",
		Region:         "us-east-1",
		Fsync:          true,
		MaxConnections: 4096,
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			if err := json.Unmarshal(data, &cfg); err != nil {
				return Config{}, fmt.Errorf("parse %s: %w", path, err)
			}
		case !errors.Is(err, os.ErrNotExist):
			return Config{}, fmt.Errorf("read %s: %w", path, err)
		}
	}
	applyEnv(&cfg)
	if cfg.Listen == "" || cfg.DataDir == "" || cfg.Region == "" {
		return Config{}, errors.New("listen, data_dir, and region must not be empty")
	}
	if cfg.MaxConnections < 1 {
		return Config{}, errors.New("max_connections must be positive")
	}
	seen := make(map[string]struct{}, len(cfg.Keys))
	for _, key := range cfg.Keys {
		if key.AccessKey == "" || key.SecretKey == "" {
			return Config{}, errors.New("every key needs access_key and secret_key")
		}
		if _, exists := seen[key.AccessKey]; exists {
			return Config{}, fmt.Errorf("duplicate access key %q", key.AccessKey)
		}
		seen[key.AccessKey] = struct{}{}
	}
	return cfg, nil
}

func applyEnv(cfg *Config) {
	set := func(name string, dst *string) {
		if value := os.Getenv(name); value != "" {
			*dst = value
		}
	}
	set("OBJEX_LISTEN", &cfg.Listen)
	set("OBJEX_DATA_DIR", &cfg.DataDir)
	set("OBJEX_REGION", &cfg.Region)
	set("OBJEX_DOMAIN", &cfg.Domain)
	if value := os.Getenv("OBJEX_FSYNC"); value != "" {
		cfg.Fsync = !containsFold([]string{"0", "false", "no", "off"}, value)
	}
	if value := os.Getenv("OBJEX_MAX_CONNECTIONS"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			cfg.MaxConnections = parsed
		}
	}
	if access, secret := os.Getenv("OBJEX_ACCESS_KEY"), os.Getenv("OBJEX_SECRET_KEY"); access != "" && secret != "" {
		filtered := cfg.Keys[:0]
		for _, key := range cfg.Keys {
			if key.AccessKey != access {
				filtered = append(filtered, key)
			}
		}
		cfg.Keys = append(filtered, Key{Name: "env", AccessKey: access, SecretKey: secret})
	}
	cfg.Domain = strings.ToLower(strings.Trim(cfg.Domain, "."))
	cfg.DataDir = filepath.Clean(cfg.DataDir)
}

func containsFold(values []string, value string) bool {
	for _, candidate := range values {
		if strings.EqualFold(candidate, value) {
			return true
		}
	}
	return false
}
