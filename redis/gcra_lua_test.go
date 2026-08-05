// Direct EVAL tests for the GCRA Lua script. These run against the real
// Sentinel-backed Redis (the same connection path /check uses), not against a
// mock or a scriptable interpreter — PRD §8 requires exercising the script
// itself, not just the HTTP layer.
package redisclient_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultMasterName  = "mymaster"
	defaultSentinelAddrs = "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381"
)

var (
	ctx    = context.Background()
	script string
	client *redis.Client
)

func TestMain(m *testing.M) {
	raw, err := os.ReadFile("gcra.lua")
	if err != nil {
		fmt.Fprintf(os.Stderr, "gcra.lua not found: %v\n", err)
		os.Exit(1)
	}
	script = string(raw)

	client = redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    envOr("REDIS_MASTER_NAME", defaultMasterName),
		SentinelAddrs: strings.Split(envOr("REDIS_SENTINEL_ADDRS", defaultSentinelAddrs), ","),
		Password:      os.Getenv("REDIS_PASSWORD"),
	})
	defer client.Close()

	code := m.Run()
	os.Exit(code)
}

// newKey returns a fresh, per-test GCRA state key so tests never collide.
func newKey(t *testing.T) string {
	t.Helper()
	key := fmt.Sprintf("gcra:%s", strings.ReplaceAll(t.Name(), "/", "_"))
	if err := client.Del(ctx, key).Err(); err != nil {
		t.Fatalf("del %s: %v", key, err)
	}
	return key
}

type result struct {
	allowed   int64
	remaining int64
	resetAt   int64
	now       int64
}

// check runs one EVAL round trip with the given parameters and decodes the
// script's {allowed, remaining, reset_at_ms, now_ms} reply.
func check(t *testing.T, key string, burst, rate, periodSec, cost int64) result {
	t.Helper()
	raw, err := client.Eval(ctx, script, []string{key}, burst, rate, periodSec, cost).Result()
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	items, ok := raw.([]interface{})
	if !ok || len(items) != 4 {
		t.Fatalf("unexpected script reply: %#v", raw)
	}
	var r result
	r.allowed = items[0].(int64)
	r.remaining = items[1].(int64)
	r.resetAt = items[2].(int64)
	r.now = items[3].(int64)
	return r
}

func nowMS() int64 {
	return time.Now().UnixMilli()
}

// The reply must carry the script's own now so callers can compute Retry-After
// from the same authoritative clock the bucket was evaluated on — a caller
// clock that skews from Redis would otherwise emit wrong retry windows.
func TestReplyCarriesRedisNow(t *testing.T) {
	key := newKey(t)
	r := check(t, key, 5, 5, 1, 1)
	if r.now <= 0 {
		t.Fatalf("reply must carry the script's now (ms), got %d", r.now)
	}
	skew := r.now - nowMS()
	if skew < -2000 || skew > 2000 {
		t.Fatalf("script now %d drifts from host clock %d by %dms", r.now, nowMS(), skew)
	}
}

func TestFreshBucketAllowsFirst(t *testing.T) {
	key := newKey(t)
	r := check(t, key, 5, 5, 1, 1) // burst 5, 5 req/sec => 200ms per token

	if r.allowed != 1 {
		t.Fatalf("first request should be allowed, got allowed=%d", r.allowed)
	}
	if r.remaining != 4 {
		t.Fatalf("remaining after consuming 1 of burst 5 = 4, got %d", r.remaining)
	}
	// Bucket is full again at now + interval (200ms). Allow a little slack.
	if r.resetAt < nowMS() || r.resetAt > nowMS()+500 {
		t.Fatalf("reset_at %d outside [now, now+500]", r.resetAt)
	}
}

func TestAllowsExactlyBurst(t *testing.T) {
	key := newKey(t)
	for i, wantRemaining := range []int64{4, 3, 2, 1, 0} {
		r := check(t, key, 5, 5, 1, 1)
		if r.allowed != 1 {
			t.Fatalf("request %d should be allowed (burst not exhausted)", i)
		}
		if r.remaining != wantRemaining {
			t.Fatalf("request %d: remaining = %d, want %d", i, r.remaining, wantRemaining)
		}
	}
}

func TestRejectsOverBurst(t *testing.T) {
	key := newKey(t)
	for i := 0; i < 5; i++ {
		check(t, key, 5, 5, 1, 1)
	}
	r := check(t, key, 5, 5, 1, 1) // 6th, burst exhausted
	if r.allowed != 0 {
		t.Fatalf("6th request should be denied, got allowed=%d", r.allowed)
	}
	if r.remaining != 0 {
		t.Fatalf("remaining should be 0 when denied, got %d", r.remaining)
	}
	// Retry-After: when one token (cost 1) is available again = +1 interval.
	wait := r.resetAt - nowMS()
	if wait < 100 || wait > 500 {
		t.Fatalf("reset_at should be roughly one interval (200ms) ahead, got %dms", wait)
	}
}

func TestRefillsAfterInterval(t *testing.T) {
	key := newKey(t)
	if r := check(t, key, 1, 1, 1, 1); r.allowed != 1 {
		t.Fatalf("burst 1 should allow first request")
	}
	if r := check(t, key, 1, 1, 1, 1); r.allowed != 0 {
		t.Fatalf("second immediate request should be denied")
	}
	time.Sleep(1100 * time.Millisecond) // interval is 1000ms
	if r := check(t, key, 1, 1, 1, 1); r.allowed != 1 {
		t.Fatalf("request after a full interval should be allowed again")
	}
}

func TestHonorsCost(t *testing.T) {
	key := newKey(t)
	r := check(t, key, 10, 5, 1, 5) // consume 5 of burst 10
	if r.allowed != 1 {
		t.Fatalf("cost 5 with burst 10 should be allowed")
	}
	if r.remaining != 5 {
		t.Fatalf("remaining after cost 5 = 5, got %d", r.remaining)
	}
}

func TestRejectsCostAboveBurst(t *testing.T) {
	key := newKey(t)
	r := check(t, key, 5, 5, 1, 10)
	if r.allowed != 0 {
		t.Fatalf("cost 10 with burst 5 can never be allowed")
	}
	if r.remaining != 5 {
		t.Fatalf("a denied request must not consume tokens, remaining = %d", r.remaining)
	}
}

func TestDenyDoesNotMutateState(t *testing.T) {
	key := newKey(t)
	if r := check(t, key, 5, 5, 1, 5); r.allowed != 1 || r.remaining != 0 {
		t.Fatalf("drain bucket: allowed=%d remaining=%d", r.allowed, r.remaining)
	}
	if r := check(t, key, 5, 5, 1, 3); r.allowed != 0 {
		t.Fatalf("cost 3 from an empty bucket should be denied")
	}
	if r := check(t, key, 5, 5, 1, 1); r.allowed != 0 {
		t.Fatalf("empty bucket still empty after a denied request")
	}
}

func TestRemainingFloorsFractionalTokens(t *testing.T) {
	key := newKey(t) // burst 10, rate 10 per 10s => 1000ms per token
	if r := check(t, key, 10, 10, 10, 1); r.allowed != 1 || r.remaining != 9 {
		t.Fatalf("start: allowed=%d remaining=%d", r.allowed, r.remaining)
	}
	time.Sleep(500 * time.Millisecond) // half a token refilled: 9.5 available
	r := check(t, key, 10, 10, 10, 1)
	if r.allowed != 1 {
		t.Fatalf("request after half refill should be allowed")
	}
	// 9.5 - 1 consumed leaves 8.5, which floors to 8 — never rounds up to 9.
	if r.remaining != 8 {
		t.Fatalf("remaining should floor to 8, got %d", r.remaining)
	}
}

func TestClientsAreIndependent(t *testing.T) {
	keyA := newKey(t)
	keyB := fmt.Sprintf("%s_b", keyA)
	if err := client.Del(ctx, keyB).Err(); err != nil {
		t.Fatalf("del %s: %v", keyB, err)
	}
	if r := check(t, keyA, 1, 1, 1, 1); r.allowed != 1 {
		t.Fatalf("A first request should be allowed")
	}
	if r := check(t, keyB, 1, 1, 1, 1); r.allowed != 1 {
		t.Fatalf("B must have its own fresh bucket")
	}
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
