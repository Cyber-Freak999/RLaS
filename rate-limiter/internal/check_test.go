package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"rlas/redis"
)

// testRedis connects to the Sentinel-backed Redis the compose stack provides
// (the same connection path /check uses). Tests skip when it is unreachable —
// CI runs the pure unit tests, while the Redis-gated suite runs locally and in
// the compose/k6 release gate.
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	c := redisclient.NewFailoverClient(redisclient.FailoverConfig{
		MasterName:    envOr("REDIS_MASTER_NAME", "mymaster"),
		SentinelAddrs: splitCSV(envOr("REDIS_SENTINEL_ADDRS", "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381")),
		Password:      os.Getenv("REDIS_PASSWORD"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		t.Skipf("redis not reachable, skipping: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func seedClient(t *testing.T, c redis.Cmdable, apiKey, clientID string, lim Limits) string {
	t.Helper()
	ctx := context.Background()
	hash := HashAPIKey(apiKey)
	if err := c.HSet(ctx, APIKeysKey, hash, clientID).Err(); err != nil {
		t.Fatalf("seed api_keys: %v", err)
	}
	raw, err := json.Marshal(lim)
	if err != nil {
		t.Fatalf("seed marshal: %v", err)
	}
	if err := c.Set(ctx, limitsKey(clientID), string(raw), 0).Err(); err != nil {
		t.Fatalf("seed limits: %v", err)
	}
	return hash
}

func newTestServer(t *testing.T, c redis.Cmdable, logger *slog.Logger) *httptest.Server {
	t.Helper()
	lim := NewLimiter(LimiterOptions{Redis: c, GCRA: redisclient.NewGCRA(c), Logger: logger})
	ts := httptest.NewServer(Server(lim))
	t.Cleanup(ts.Close)
	return ts
}

func doCheck(t *testing.T, url, apiKey, body string) (*http.Response, map[string]interface{}) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/v1/check", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if apiKey != "" {
		req.Header.Set("X-Api-Key", apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	var out map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func TestCheckAllowsWithinBurst(t *testing.T) {
	c := testRedis(t)
	apiKey := fmt.Sprintf("key-%s", t.Name())
	clientID := fmt.Sprintf("client-%s", t.Name())
	seedClient(t, c, apiKey, clientID, Limits{Rate: 1, Period: "second", Burst: 2})
	srv := newTestServer(t, c, testLogger())

	for i := 0; i < 2; i++ {
		resp, out := doCheck(t, srv.URL, apiKey, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status %d, want 200", i, resp.StatusCode)
		}
		if out["allowed"] != true {
			t.Fatalf("request %d: allowed=%v", i, out["allowed"])
		}
		wantRemaining := 2 - i - 1
		if got := int64(out["remaining"].(float64)); got != int64(wantRemaining) {
			t.Fatalf("request %d: remaining=%d, want %d", i, got, wantRemaining)
		}
		if _, ok := out["reset_at"]; !ok {
			t.Fatalf("response must carry reset_at")
		}
		if resp.Header.Get("X-RateLimit-Degraded") != "" {
			t.Fatalf("healthy path must not carry degraded header")
		}
	}
}

func TestCheckDeniesOverBurstWithRetryAfter(t *testing.T) {
	c := testRedis(t)
	apiKey := fmt.Sprintf("key-%s", t.Name())
	clientID := fmt.Sprintf("client-%s", t.Name())
	seedClient(t, c, apiKey, clientID, Limits{Rate: 1, Period: "second", Burst: 1})
	srv := newTestServer(t, c, testLogger())

	doCheck(t, srv.URL, apiKey, "") // exhaust the single burst token

	resp, out := doCheck(t, srv.URL, apiKey, "")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", resp.StatusCode)
	}
	if out["allowed"] != false || out["remaining"].(float64) != 0 {
		t.Fatalf("denied response body wrong: %v", out)
	}
	if _, ok := out["reset_at"]; !ok {
		t.Fatalf("denied response must carry reset_at")
	}
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		t.Fatalf("denied response must carry Retry-After")
	}
	secs, err := time.ParseDuration(ra + "s")
	if err != nil || secs < time.Second || secs > 2*time.Second {
		t.Fatalf("Retry-After %q out of [1s,2s] for a 1/sec limiter", ra)
	}
}

func TestCheckHonorsCost(t *testing.T) {
	c := testRedis(t)
	apiKey := fmt.Sprintf("key-%s", t.Name())
	clientID := fmt.Sprintf("client-%s", t.Name())
	seedClient(t, c, apiKey, clientID, Limits{Rate: 5, Period: "second", Burst: 10})
	srv := newTestServer(t, c, testLogger())

	resp, out := doCheck(t, srv.URL, apiKey, `{"cost": 5}`)
	if resp.StatusCode != http.StatusOK || out["allowed"] != true {
		t.Fatalf("cost 5 against burst 10 should be allowed: %v", out)
	}
	if got := int64(out["remaining"].(float64)); got != 5 {
		t.Fatalf("remaining after cost 5 = %d, want 5", got)
	}
}

func TestCheckCostDefaultsToOne(t *testing.T) {
	c := testRedis(t)
	apiKey := fmt.Sprintf("key-%s", t.Name())
	clientID := fmt.Sprintf("client-%s", t.Name())
	seedClient(t, c, apiKey, clientID, Limits{Rate: 5, Period: "second", Burst: 1})
	srv := newTestServer(t, c, testLogger())

	resp, _ := doCheck(t, srv.URL, apiKey, "{}")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("empty body must default cost to 1, got %d", resp.StatusCode)
	}
}

func TestCheckRejectsBadInput(t *testing.T) {
	c := testRedis(t)
	srv := newTestServer(t, c, testLogger())

	cases := []struct {
		name   string
		key    string
		body   string
		status int
	}{
		{"missing header", "", "", http.StatusBadRequest},
		{"cost zero", "k", `{"cost": 0}`, http.StatusBadRequest},
		{"cost negative", "k", `{"cost": -3}`, http.StatusBadRequest},
		{"cost non-integer", "k", `{"cost": 1.5}`, http.StatusBadRequest},
		{"cost not a number", "k", `{"cost": "high"}`, http.StatusBadRequest},
		{"broken json", "k", `{"`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		resp, _ := doCheck(t, srv.URL, tc.key, tc.body)
		if resp.StatusCode != tc.status {
			t.Errorf("%s: status %d, want %d", tc.name, resp.StatusCode, tc.status)
		}
	}
}

func TestCheckUnknownKeyUnauthorized(t *testing.T) {
	c := testRedis(t)
	srv := newTestServer(t, c, testLogger())
	resp, _ := doCheck(t, srv.URL, "not-a-real-key", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown key: status %d, want 401", resp.StatusCode)
	}
}

func TestCheckMethodNotAllowed(t *testing.T) {
	c := testRedis(t)
	srv := newTestServer(t, c, testLogger())
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/check", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/check: status %d, want 405", resp.StatusCode)
	}
}

func TestApprovedRequestPushedToStream(t *testing.T) {
	c := testRedis(t)
	apiKey := fmt.Sprintf("key-%s", t.Name())
	clientID := fmt.Sprintf("client-%s", t.Name())
	seedClient(t, c, apiKey, clientID, Limits{Rate: 5, Period: "second", Burst: 5})
	srv := newTestServer(t, c, testLogger())

	resp, _ := doCheck(t, srv.URL, apiKey, `{"cost": 3}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("setup request should succeed")
	}

	msgs, err := c.XRevRangeN(context.Background(), StreamKey, "+", "-", 1).Result()
	if err != nil {
		t.Fatalf("xrevrange: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatalf("approved request must push a stream event")
	}
	if got := msgs[0].Values["client_id"]; got != clientID {
		t.Fatalf("latest event client_id = %v, want %s", got, clientID)
	}
	if got := msgs[0].Values["cost"]; got != "3" {
		t.Fatalf("latest event cost = %v, want 3", got)
	}
}

func TestHealthzOK(t *testing.T) {
	c := testRedis(t)
	srv := newTestServer(t, c, testLogger())
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status %d, want 200", resp.StatusCode)
	}
}

// deadRedis is a FailoverClient whose sentinels are unreachable, with short
// timeouts so the test doesn't hang on dials.
func deadRedis() *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:1"},
		DialTimeout:   300 * time.Millisecond,
		ReadTimeout:   300 * time.Millisecond,
		WriteTimeout:  300 * time.Millisecond,
		PoolTimeout:   300 * time.Millisecond,
		MaxRetries:    0,
	})
}

func TestDegradedServesTrafficAndTrips(t *testing.T) {
	// Seed the fallback cache with what this replica would have learned from
	// successful round trips.
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	dead := deadRedis()
	t.Cleanup(func() { _ = dead.Close() })

	lim := NewLimiter(LimiterOptions{Redis: dead, GCRA: redisclient.NewGCRA(dead), Logger: logger})
	hash := HashAPIKey("degraded-key")
	lim.cache.rememberKey(hash, "degraded-client")
	// Refill is 1 token/hour so the slow (failing) dials never refill the
	// replica-local bucket during the test.
	lim.cache.rememberLimits("degraded-client", Limits{Rate: 1, Period: "hour", Burst: 2})
	srv := httptest.NewServer(Server(lim))
	defer srv.Close()

	// First two fit the cached burst and must succeed in degraded mode; the
	// third exhausts the replica-local bucket and must be a degraded 429.
	for i := 0; i < 2; i++ {
		resp, out := doCheck(t, srv.URL, "degraded-key", "")
		if resp.Header.Get("X-RateLimit-Degraded") != "true" {
			t.Fatalf("request %d: missing degraded header", i)
		}
		if resp.StatusCode != http.StatusOK || out["allowed"] != true {
			t.Fatalf("request %d: degraded traffic must keep succeeding, got %d %v", i, resp.StatusCode, out)
		}
	}
	resp, out := doCheck(t, srv.URL, "degraded-key", "")
	if resp.Header.Get("X-RateLimit-Degraded") != "true" {
		t.Fatalf("request 3: missing degraded header")
	}
	if resp.StatusCode != http.StatusTooManyRequests || out["allowed"] != false {
		t.Fatalf("request 3: expected degraded 429, got %d %v", resp.StatusCode, out)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatalf("degraded 429 must still carry Retry-After")
	}

	// The breaker must have tripped after the 3 consecutive check-path
	// failures (fixed structured event, AGENTS.md).
	logs := buf.String()
	if !strings.Contains(logs, "circuit_breaker_tripped") {
		t.Fatalf("missing circuit_breaker_tripped event, got: %s", logs)
	}
	if !strings.Contains(logs, "redis_unreachable") {
		t.Fatalf("trip event must say why, got: %s", logs)
	}
	if !lim.BreakerOpen() {
		t.Fatalf("breaker must be open after 3 consecutive failures")
	}
}

func TestDegradedUnknownKeyUnauthorized(t *testing.T) {
	dead := deadRedis()
	t.Cleanup(func() { _ = dead.Close() })
	lim := NewLimiter(LimiterOptions{Redis: dead, GCRA: redisclient.NewGCRA(dead), Logger: testLogger()})
	srv := httptest.NewServer(Server(lim))
	defer srv.Close()

	resp, _ := doCheck(t, srv.URL, "never-seen-key", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown key during outage: status %d, want 401", resp.StatusCode)
	}
	if resp.Header.Get("X-RateLimit-Degraded") != "true" {
		t.Fatalf("outage response must carry degraded header")
	}
}

func TestHealthzUnhealthyWhenRedisDown(t *testing.T) {
	dead := deadRedis()
	t.Cleanup(func() { _ = dead.Close() })
	lim := NewLimiter(LimiterOptions{Redis: dead, GCRA: redisclient.NewGCRA(dead), Logger: testLogger()})
	srv := httptest.NewServer(Server(lim))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get healthz: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("healthz with redis down: status %d, want 503 (body %s)", resp.StatusCode, body)
	}
}
