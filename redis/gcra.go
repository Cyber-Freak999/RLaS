// Package redisclient is the small shared Redis layer both services use: the
// GCRA Lua script (embedded, so rate-limiter and control-plane can never drift
// from a second copy) and the client factory (Sentinel failover by default,
// plain single-endpoint for managed providers via REDIS_URL).
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
type FailoverConfig struct {
	MasterName    string
	SentinelAddrs []string
	Password      string
}

// NewFailoverClient builds a Sentinel-aware client pointing at whatever node
// currently holds the master.
func NewFailoverClient(cfg FailoverConfig) *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    cfg.MasterName,
		SentinelAddrs: cfg.SentinelAddrs,
		Password:      cfg.Password,
	})
}

// ClientConfig is the union of connection options both services read from the
// environment. Sentinel is the default topology (constraint 3); URL is the
// explicit opt-in for managed single-endpoint Redis (Render Key Value,
// Upstash, ElastiCache) where there is no Sentinel to query. A non-empty URL
// wins over SentinelAddrs.
type ClientConfig struct {
	MasterName    string
	SentinelAddrs []string
	Password      string
	URL           string
}

// NewClient builds the right go-redis client for the config: a Sentinel-aware
// failover client by default, or a plain client for an explicit REDIS_URL.
// Managed Redis providers don't speak the Sentinel protocol, so portability
// to them (Render's Key Value in the demo deployment) is a config choice, not
// a code change. Sentinel remains the primary topology; REDIS_URL is the
// deviation flagged in AGENTS.md constraint 3.
func NewClient(cfg ClientConfig) (*redis.Client, error) {
	if cfg.URL != "" {
		opt, err := redis.ParseURL(cfg.URL)
		if err != nil {
			return nil, fmt.Errorf("parse REDIS_URL: %w", err)
		}
		return redis.NewClient(opt), nil
	}
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    cfg.MasterName,
		SentinelAddrs: cfg.SentinelAddrs,
		Password:      cfg.Password,
	}), nil
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
