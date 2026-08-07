package internal

import (
	"testing"
	"time"
)

func TestLoadConfigDefaults(t *testing.T) {
	for _, k := range []string{"REDIS_MASTER_NAME", "REDIS_SENTINEL_ADDRS", "REDIS_PASSWORD", "PORT", "SHUTDOWN_TIMEOUT", "REDIS_POOL_SIZE", "REDIS_MIN_IDLE"} {
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
	if cfg.Redis.PoolSize != defaultRedisPoolSize {
		t.Fatalf("default pool size = %d, want %d", cfg.Redis.PoolSize, defaultRedisPoolSize)
	}
	if cfg.Redis.MinIdleConns != defaultRedisMinIdle {
		t.Fatalf("default min idle = %d, want %d", cfg.Redis.MinIdleConns, defaultRedisMinIdle)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	t.Setenv("REDIS_MASTER_NAME", "testmaster")
	t.Setenv("REDIS_SENTINEL_ADDRS", "a:26379, b:26380")
	t.Setenv("REDIS_PASSWORD", "pw")
	t.Setenv("PORT", "9000")
	t.Setenv("SHUTDOWN_TIMEOUT", "2s")
	t.Setenv("REDIS_POOL_SIZE", "500")
	t.Setenv("REDIS_MIN_IDLE", "50")
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
	if cfg.Redis.PoolSize != 500 {
		t.Fatalf("pool size = %d, want 500", cfg.Redis.PoolSize)
	}
	if cfg.Redis.MinIdleConns != 50 {
		t.Fatalf("min idle = %d, want 50", cfg.Redis.MinIdleConns)
	}
}
