// Direct EVAL tests for the single-round-trip check script. These exercise the
// script itself against the real Sentinel-backed Redis (PRD §8), covering auth,
// limit lookup/validation, GCRA, and the conditional stream push in one atomic
// round trip. All keys are test-scoped (unique per test name) so these can run
// against a live stack without colliding with production traffic.
package redisclient_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// checkKeys are the four KEYS the script receives: the api_keys hash name, the
// client_limits prefix, the gcra prefix, and the approved-request stream.
type checkKeys struct {
	apiKeys  string
	limPref  string
	gcraPref string
	stream   string
}

// checkTestKeys returns fully test-scoped key names so direct EVAL tests never
// touch the live api_keys/client_limits/stream/gcra schema.
func checkTestKeys(t *testing.T) checkKeys {
	t.Helper()
	id := strings.ReplaceAll(t.Name(), "/", "_")
	return checkKeys{
		apiKeys:  "tk:api:" + id,
		limPref:  "tk:lim:" + id + ":",
		gcraPref: "tk:gcra:" + id + ":",
		stream:   "tk:stream:" + id,
	}
}

func loadCheckScript(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("check.lua")
	if err != nil {
		t.Fatalf("check.lua not found: %v", err)
	}
	return string(raw)
}

// seedCheckClient resets the test keys, then maps hash "testhash" -> clientID
// and stores the client's limits JSON. The gcra state key is cleared so a
// re-run starts from a fresh bucket.
func seedCheckClient(t *testing.T, k checkKeys, clientID, limitsJSON string) {
	t.Helper()
	if err := client.Del(ctx, k.stream, k.apiKeys, k.gcraPref+clientID).Err(); err != nil {
		t.Fatalf("reset test keys: %v", err)
	}
	if err := client.HSet(ctx, k.apiKeys, "testhash", clientID).Err(); err != nil {
		t.Fatalf("seed api_keys: %v", err)
	}
	if err := client.Set(ctx, k.limPref+clientID, limitsJSON, 0).Err(); err != nil {
		t.Fatalf("seed limits: %v", err)
	}
}

type checkResult struct {
	status    int64
	remaining int64
	resetAt   int64
	now       int64
	clientID  string
	rate      int64
	period    string
	burst     int64
}

// runCheck performs one EVAL with the given api-key hash, cost, and stream
// MAXLEN, decoding the script's 8-element reply.
func runCheck(t *testing.T, script string, k checkKeys, hash string, cost, maxlen int64) checkResult {
	t.Helper()
	raw, err := client.Eval(ctx, script,
		[]string{k.apiKeys, k.limPref, k.gcraPref, k.stream},
		hash, cost, maxlen).Result()
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	items, ok := raw.([]interface{})
	if !ok || len(items) != 8 {
		t.Fatalf("unexpected script reply: %#v", raw)
	}
	return checkResult{
		status:    items[0].(int64),
		remaining: items[1].(int64),
		resetAt:   items[2].(int64),
		now:       items[3].(int64),
		clientID:  items[4].(string),
		rate:      items[5].(int64),
		period:    items[6].(string),
		burst:     items[7].(int64),
	}
}

func streamLen(t *testing.T, k checkKeys) int64 {
	t.Helper()
	n, err := client.XLen(ctx, k.stream).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	return n
}

func TestCheckAllowsKnownClientAndPushesStreamEvent(t *testing.T) {
	script := loadCheckScript(t)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-a", `{"rate":5,"period":"second","burst":10}`)

	r := runCheck(t, script, k, "testhash", 1, 100000)
	if r.status != 1 {
		t.Fatalf("known client should be allowed, status=%d", r.status)
	}
	if r.remaining != 9 {
		t.Fatalf("remaining after consuming 1 of burst 10 = 9, got %d", r.remaining)
	}
	if r.clientID != "client-a" {
		t.Fatalf("client_id = %q, want client-a", r.clientID)
	}
	if r.rate != 5 || r.period != "second" || r.burst != 10 {
		t.Fatalf("limits echoed wrong: rate=%d period=%q burst=%d", r.rate, r.period, r.burst)
	}
	if r.now <= 0 {
		t.Fatalf("reply must carry the script's now, got %d", r.now)
	}

	if n := streamLen(t, k); n != 1 {
		t.Fatalf("allowed request must push exactly one stream event, got %d", n)
	}
	msgs, err := client.XRevRangeN(ctx, k.stream, "+", "-", 1).Result()
	if err != nil {
		t.Fatalf("xrevrange: %v", err)
	}
	if got := msgs[0].Values["client_id"]; got != "client-a" {
		t.Fatalf("event client_id = %v, want client-a", got)
	}
	if got := msgs[0].Values["cost"]; got != "1" {
		t.Fatalf("event cost = %v, want 1", got)
	}
}

func TestCheckDeniesOverBurst(t *testing.T) {
	script := loadCheckScript(t)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-b", `{"rate":1,"period":"second","burst":1}`)

	if r := runCheck(t, script, k, "testhash", 1, 100000); r.status != 1 {
		t.Fatalf("first request should be allowed, status=%d", r.status)
	}
	r := runCheck(t, script, k, "testhash", 1, 100000)
	if r.status != 0 {
		t.Fatalf("second request should be denied, status=%d", r.status)
	}
	if r.remaining != 0 {
		t.Fatalf("denied response remaining = %d, want 0", r.remaining)
	}
	// Denied: reset_at is when one token (cost 1) is available again, i.e. one
	// full interval (1000ms here) from now.
	wait := r.resetAt - nowMS()
	if wait < 100 || wait > 1500 {
		t.Fatalf("reset_at should be roughly one interval (1000ms) ahead, got %dms", wait)
	}
}

func TestCheckUnauthorizedKey(t *testing.T) {
	script := loadCheckScript(t)
	k := checkTestKeys(t)

	r := runCheck(t, script, k, "unknown-hash", 1, 100000)
	if r.status != -1 {
		t.Fatalf("unknown key should be unauthorized, status=%d", r.status)
	}
	if n := streamLen(t, k); n != 0 {
		t.Fatalf("unauthorized request must not push a stream event, got %d", n)
	}
}

func TestCheckLimitsNotFound(t *testing.T) {
	script := loadCheckScript(t)
	k := checkTestKeys(t)
	if err := client.HSet(ctx, k.apiKeys, "testhash", "client-c").Err(); err != nil {
		t.Fatalf("seed api_keys: %v", err)
	}

	r := runCheck(t, script, k, "testhash", 1, 100000)
	if r.status != -2 {
		t.Fatalf("missing limits should be limits-not-found, status=%d", r.status)
	}
	if r.clientID != "client-c" {
		t.Fatalf("reply must carry the resolved client_id, got %q", r.clientID)
	}
}

func TestCheckRejectsInvalidLimits(t *testing.T) {
	script := loadCheckScript(t)
	k := checkTestKeys(t)

	cases := []struct {
		name string
		lim  string
	}{
		{"zero rate", `{"rate":0,"period":"second","burst":1}`},
		{"zero burst", `{"rate":1,"period":"second","burst":0}`},
		{"unknown period", `{"rate":1,"period":"fortnight","burst":1}`},
		{"unparseable", `not json`},
		{"wrong type", `{"rate":"high","period":"second","burst":1}`},
	}
	for i, tc := range cases {
		id := fmt.Sprintf("client-d%d", i)
		seedCheckClient(t, k, id, tc.lim)
		r := runCheck(t, script, k, "testhash", 1, 100000)
		if r.status != -3 {
			t.Errorf("%s: status=%d, want -3", tc.name, r.status)
		}
	}
}

func TestCheckDeniedDoesNotPushStreamEvent(t *testing.T) {
	script := loadCheckScript(t)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-e", `{"rate":1,"period":"second","burst":1}`)

	runCheck(t, script, k, "testhash", 1, 100000) // allowed: 1 event
	runCheck(t, script, k, "testhash", 1, 100000) // denied: no event
	if n := streamLen(t, k); n != 1 {
		t.Fatalf("stream length = %d, want 1 (only the allowed event)", n)
	}
}

func TestCheckHonorsCost(t *testing.T) {
	script := loadCheckScript(t)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-f", `{"rate":5,"period":"second","burst":10}`)

	r := runCheck(t, script, k, "testhash", 5, 100000)
	if r.status != 1 {
		t.Fatalf("cost 5 of burst 10 should be allowed, status=%d", r.status)
	}
	if r.remaining != 5 {
		t.Fatalf("remaining after cost 5 = %d, want 5", r.remaining)
	}
}

func TestCheckWritesGcraStateKey(t *testing.T) {
	script := loadCheckScript(t)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-g", `{"rate":5,"period":"second","burst":10}`)

	if r := runCheck(t, script, k, "testhash", 1, 100000); r.status != 1 {
		t.Fatalf("setup should be allowed, status=%d", r.status)
	}
	// The TAT must land on the gcra:<client_id> key the control-plane resets by
	// DEL on a limit change — the two must stay in lockstep (constraint 6).
	tat, err := client.Get(ctx, k.gcraPref+"client-g").Result()
	if err != nil {
		t.Fatalf("gcra state key must be written after an allow: %v", err)
	}
	if tat == "" || tat == "0" {
		t.Fatalf("tat = %q, want a real timestamp", tat)
	}
}

func TestCheckRejectsZeroCost(t *testing.T) {
	script := loadCheckScript(t)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-h", `{"rate":5,"period":"second","burst":10}`)

	r := runCheck(t, script, k, "testhash", 0, 100000)
	if r.status != -4 {
		t.Fatalf("cost 0 should be rejected as bad params, status=%d", r.status)
	}
	if n := streamLen(t, k); n != 0 {
		t.Fatalf("bad-params request must not push a stream event, got %d", n)
	}
}
