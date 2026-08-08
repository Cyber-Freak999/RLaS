package internal

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the control-plane's environment-driven configuration. It is the
// administrative counterpart to the rate-limiter's config: the two services
// read the same Sentinel vars so the compose wiring can be identical.
type Config struct {
	AdminToken     string
	TimescaleDSN   string
	Redis          RedisConfig
	Port           int
	ShutdownTimeout time.Duration
}

type RedisConfig struct {
	MasterName    string
	SentinelAddrs []string
	Password      string
	URL           string
}

func LoadConfig() Config {
	return Config{
		AdminToken:   os.Getenv("ADMIN_BEARER_TOKEN"),
		TimescaleDSN: os.Getenv("TIMESCALE_DSN"),
		Redis: RedisConfig{
			MasterName:    envOr("REDIS_MASTER_NAME", "mymaster"),
			SentinelAddrs: splitCSV(envOr("REDIS_SENTINEL_ADDRS", "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381")),
			Password:      os.Getenv("REDIS_PASSWORD"),
			URL:           os.Getenv("REDIS_URL"),
		},
		Port:            envInt("PORT", 8081),
		ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
}

// Validate fails fast on the configuration the service cannot run without:
// an admin token (constraint 8) and the analytics DSN. Sentinel defaults are
// always usable, so they are not validated.
func (c Config) Validate() error {
	if c.AdminToken == "" {
		return errors.New("ADMIN_BEARER_TOKEN must be set")
	}
	if c.TimescaleDSN == "" {
		return errors.New("TIMESCALE_DSN must be set")
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
