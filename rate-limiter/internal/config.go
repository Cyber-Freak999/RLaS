package internal

import (
	"os"
	"strconv"
	"strings"
	"time"

	"rlas/redis"
)

// Config carries everything the rate-limiter reads from the environment.
type Config struct {
	Redis           redisclient.ClientConfig
	Port            int
	ShutdownTimeout time.Duration
}

// LoadConfig reads env vars. Defaults match local developer setup; Compose
// overrides them with in-container service names. REDIS_URL is the opt-in for
// managed single-endpoint Redis (no Sentinel to query).
func LoadConfig() Config {
	return Config{
		Redis: redisclient.ClientConfig{
			MasterName:    envOr("REDIS_MASTER_NAME", "mymaster"),
			SentinelAddrs: splitCSV(envOr("REDIS_SENTINEL_ADDRS", "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381")),
			Password:      os.Getenv("REDIS_PASSWORD"),
			URL:           os.Getenv("REDIS_URL"),
		},
		Port:            envInt("PORT", 8080),
		ShutdownTimeout: envDur("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}

func envDur(key string, fallback time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}
