package internal

import (
	"testing"
	"time"
)

func TestLoadConfigDefaults(t *testing.T) {
	for _, k := range []string{"REDIS_MASTER_NAME", "REDIS_SENTINEL_ADDRS", "REDIS_PASSWORD", "PORT", "SHUTDOWN_TIMEOUT"} {
		t.Setenv(k, "")
	}
	cfg := LoadConfig()
	if cfg.Redis.MasterName != "mymaster" {
		t.Fatalf("default master name = %q, want mymaster", cfg.Redis.MasterName)
	}
	if len(cfg.Redis.SentinelAddrs) != 3 {
		t.Fatalf("default sentinel addrs = %v, want 3", cfg.Redis.SentinelAddrs)
	}
	if cfg.Port != 8080 {
		t.Fatalf("default port = %d, want 8080", cfg.Port)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("default shutdown timeout = %s, want 10s", cfg.ShutdownTimeout)
	}
	if cfg.Redis.URL != "" {
		t.Fatalf("default redis url = %q, want empty", cfg.Redis.URL)
	}
}

func TestLoadConfigReadsRedisURL(t *testing.T) {
	t.Setenv("REDIS_URL", "redis://cache.internal:6379/0")
	t.Setenv("REDIS_SENTINEL_ADDRS", "")
	cfg := LoadConfig()
	if cfg.Redis.URL != "redis://cache.internal:6379/0" {
		t.Fatalf("redis url = %q, want redis://cache.internal:6379/0", cfg.Redis.URL)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	t.Setenv("REDIS_MASTER_NAME", "testmaster")
	t.Setenv("REDIS_SENTINEL_ADDRS", "a:26379, b:26380")
	t.Setenv("REDIS_PASSWORD", "pw")
	t.Setenv("PORT", "9000")
	t.Setenv("SHUTDOWN_TIMEOUT", "2s")
	cfg := LoadConfig()
	if cfg.Redis.MasterName != "testmaster" {
		t.Fatalf("master name = %q", cfg.Redis.MasterName)
	}
	if len(cfg.Redis.SentinelAddrs) != 2 || cfg.Redis.SentinelAddrs[1] != "b:26380" {
		t.Fatalf("sentinel addrs = %v", cfg.Redis.SentinelAddrs)
	}
	if cfg.Redis.Password != "pw" {
		t.Fatalf("password = %q", cfg.Redis.Password)
	}
	if cfg.Port != 9000 || cfg.ShutdownTimeout != 2*time.Second {
		t.Fatalf("port=%d timeout=%s", cfg.Port, cfg.ShutdownTimeout)
	}
}
