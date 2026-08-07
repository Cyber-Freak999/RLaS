// Package redisclient is the small shared Redis layer both services use: the
// GCRA Lua script (embedded, so rate-limiter and control-plane can never drift
// from a second copy) and a Sentinel-aware failover client.
//
// AGENTS.md permits a small shared redisclient/types package; this is it. The
// Lua script and its reply contract live in exactly one place.
package redisclient

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

//go:embed gcra.lua
var gcraLua string

// FailoverConfig is the subset of Sentinel connection options the services
// read from the environment. Sentinel-aware only — never a static master
// hostname (constraint 3).
//
// PoolSize and MinIdleConns are the go-redis connection pool tuning for the
// hot path. The rate-limiter serves hundreds of concurrent /check requests,
// each of which needs several sequential Redis round trips; go-redis's default
// pool of 10×GOMAXPROCS (40 on a 4-CPU container) serializes that concurrency
// into queueing latency, so the latency-critical service sizes the pool up.
// 0 leaves go-redis's defaults, which is right for low-concurrency callers
// like control-plane.
type FailoverConfig struct {
	MasterName    string
	SentinelAddrs []string
	Password      string
	PoolSize      int
	MinIdleConns  int
}

// NewFailoverClient builds a Sentinel-aware client pointing at whatever node
// currently holds the master.
func NewFailoverClient(cfg FailoverConfig) *redis.Client {
	opts := &redis.FailoverOptions{
		MasterName:    cfg.MasterName,
		SentinelAddrs: cfg.SentinelAddrs,
		Password:      cfg.Password,
	}
	if cfg.PoolSize > 0 {
		opts.PoolSize = cfg.PoolSize
	}
	if cfg.MinIdleConns > 0 {
		opts.MinIdleConns = cfg.MinIdleConns
	}
	return redis.NewFailoverClient(opts)
}

// Params mirrors the script's ARGV contract (constraint 13: the service
// validates before this is reached; the script still guards defensively).
type Params struct {
	Burst     int64 // bucket capacity in tokens, >= 1
	Rate      int64 // tokens per period, > 0
	PeriodSec int64 // seconds per rate period, > 0
	Cost      int64 // tokens consumed by this request, >= 1
}

// CheckResult is the decoded {allowed, remaining, reset_at_ms, now_ms} reply.
// NowMS lets the service compute Retry-After on the same clock the bucket was
// evaluated, avoiding host/Redis clock skew.
type CheckResult struct {
	Allowed   bool
	Remaining int64
	ResetAtMS int64
	NowMS     int64
}

// GCRA wraps the embedded script. One instance per service is fine — the
// script is already loaded and cached by go-redis after the first run.
type GCRA struct {
	script *redis.Script
}

// NewGCRA returns a GCRA wired to the given client.
func NewGCRA(c redis.Cmdable) *GCRA {
	return &GCRA{script: redis.NewScript(gcraLua)}
}

// Check runs one atomic EVAL. It is the only place the script reply is
// decoded, so a contract change touches one file plus the script itself.
func (g *GCRA) Check(ctx context.Context, c redis.Cmdable, key string, p Params) (CheckResult, error) {
	raw, err := g.script.Run(ctx, c, []string{key}, p.Burst, p.Rate, p.PeriodSec, p.Cost).Result()
	if err != nil {
		return CheckResult{}, err
	}
	items, ok := raw.([]interface{})
	if !ok || len(items) != 4 {
		return CheckResult{}, fmt.Errorf("unexpected GCRA reply %#v", raw)
	}
	allowed := items[0].(int64)
	remaining := items[1].(int64)
	resetAt := items[2].(int64)
	now := items[3].(int64)
	// The script's defensive guard returns now=0; by then the service already
	// validated parameters (constraint 13), so this is a programming error and
	// must not surface as a weird-but-successful response.
	if now <= 0 {
		return CheckResult{}, fmt.Errorf("GCRA rejected parameters (now=%d)", now)
	}
	return CheckResult{
		Allowed:   allowed == 1,
		Remaining: remaining,
		ResetAtMS: resetAt,
		NowMS:     now,
	}, nil
}
