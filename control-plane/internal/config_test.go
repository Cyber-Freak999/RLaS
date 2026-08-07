package internal

import (
	"testing"
	"time"
)

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("ADMIN_BEARER_TOKEN", "")
	t.Setenv("TIMESCALE_DSN", "")
	t.Setenv("REDIS_SENTINEL_ADDRS", "")
	t.Setenv("PORT", "")
	t.Setenv("SHUTDOWN_TIMEOUT", "")

	cfg := LoadConfig()
	if cfg.AdminToken != "" {
		t.Fatalf("AdminToken = %q, want empty", cfg.AdminToken)
	}
	if cfg.TimescaleDSN != "" {
		t.Fatalf("TimescaleDSN = %q, want empty", cfg.TimescaleDSN)
	}
	if cfg.Redis.MasterName != "mymaster" {
		t.Fatalf("MasterName = %q, want mymaster", cfg.Redis.MasterName)
	}
	if len(cfg.Redis.SentinelAddrs) != 3 {
		t.Fatalf("SentinelAddrs len = %d, want 3", len(cfg.Redis.SentinelAddrs))
	}
	if cfg.Port != 8081 {
		t.Fatalf("Port = %d, want 8081", cfg.Port)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want 10s", cfg.ShutdownTimeout)
	}
}

func TestLoadConfigEnvOverrides(t *testing.T) {
	t.Setenv("ADMIN_BEARER_TOKEN", "sekret")
	t.Setenv("TIMESCALE_DSN", "postgres://u:p@db:5432/rlas")
	t.Setenv("REDIS_SENTINEL_ADDRS", "one:1, two:2")
	t.Setenv("REDIS_MASTER_NAME", "prim")
	t.Setenv("PORT", "9999")
	t.Setenv("SHUTDOWN_TIMEOUT", "3s")

	cfg := LoadConfig()
	if cfg.AdminToken != "sekret" {
		t.Fatalf("AdminToken = %q, want sekret", cfg.AdminToken)
	}
	if cfg.TimescaleDSN != "postgres://u:p@db:5432/rlas" {
		t.Fatalf("TimescaleDSN = %q", cfg.TimescaleDSN)
	}
	if cfg.Redis.MasterName != "prim" {
		t.Fatalf("MasterName = %q, want prim", cfg.Redis.MasterName)
	}
	if len(cfg.Redis.SentinelAddrs) != 2 || cfg.Redis.SentinelAddrs[0] != "one:1" || cfg.Redis.SentinelAddrs[1] != "two:2" {
		t.Fatalf("SentinelAddrs = %v", cfg.Redis.SentinelAddrs)
	}
	if cfg.Port != 9999 {
		t.Fatalf("Port = %d, want 9999", cfg.Port)
	}
	if cfg.ShutdownTimeout != 3*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want 3s", cfg.ShutdownTimeout)
	}
}

func TestValidate(t *testing.T) {
	t.Setenv("TIMESCALE_DSN", "postgres://u:p@db:5432/rlas")
	cfg := LoadConfig()
	cfg.AdminToken = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil with empty admin token, want error")
	}
	cfg.AdminToken = "tok"
	cfg.TimescaleDSN = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil with empty timescale DSN, want error")
	}
	cfg.TimescaleDSN = "postgres://u:p@db:5432/rlas"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}
