// Tests for the Go wrapper around the GCRA script (redisclient package).
// These exercise the same typed Check the rate-limiter service will use, so
// reply parsing and error handling are verified here rather than only through
// HTTP.
package redisclient_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"rlas/redis"

	"github.com/redis/go-redis/v9"
)

var (
	clientCtx   = context.Background()
	testCtxDone = false
)

func newTestClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redisclient.NewFailoverClient(redisclient.FailoverConfig{
		MasterName:    envOr("REDIS_MASTER_NAME", defaultMasterName),
		SentinelAddrs: strings.Split(envOr("REDIS_SENTINEL_ADDRS", defaultSentinelAddrs), ","),
		Password:      os.Getenv("REDIS_PASSWORD"),
	})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newClientKey(t *testing.T) string {
	t.Helper()
	key := fmt.Sprintf("gcra:client_%s", strings.ReplaceAll(t.Name(), "/", "_"))
	if err := newTestClient(t).Del(clientCtx, key).Err(); err != nil {
		t.Fatalf("del %s: %v", key, err)
	}
	return key
}

func TestCheckTypedReply(t *testing.T) {
	c := newTestClient(t)
	g := redisclient.NewGCRA(c)
	key := newClientKey(t)

	r, err := g.Check(clientCtx, c, key, redisclient.Params{Burst: 5, Rate: 5, PeriodSec: 1, Cost: 1})
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !r.Allowed || r.Remaining != 4 {
		t.Fatalf("fresh bucket: allowed=%v remaining=%d, want true/4", r.Allowed, r.Remaining)
	}
	if r.NowMS <= 0 {
		t.Fatalf("reply must carry now_ms, got %d", r.NowMS)
	}
	if r.ResetAtMS < r.NowMS || r.ResetAtMS > r.NowMS+500 {
		t.Fatalf("reset_at %d not within [now, now+500]", r.ResetAtMS)
	}
}

func TestCheckRejectsDefensiveParams(t *testing.T) {
	c := newTestClient(t)
	g := redisclient.NewGCRA(c)
	key := newClientKey(t)

	// A zero/negative parameter trips the script's defensive guard, which the
	// wrapper must surface as an error rather than a silently weird reply.
	_, err := g.Check(clientCtx, c, key, redisclient.Params{Burst: 0, Rate: 5, PeriodSec: 1, Cost: 1})
	if err == nil {
		t.Fatalf("burst 0 should be rejected by the wrapper, got no error")
	}
}
