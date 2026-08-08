// Tests for the Go wrapper around the single-round-trip check script. These
// verify the typed reply decoding and status mapping the rate-limiter service
// will rely on, so the contract is checked here rather than only through HTTP.
package redisclient_test

import (
	"testing"

	"rlas/redis"
)

func TestCheckerAllowsAndDecodesReply(t *testing.T) {
	c := newTestClient(t)
	ch := redisclient.NewChecker(c)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-w", `{"rate":5,"period":"second","burst":10}`)

	reply, err := ch.Check(clientCtx, c, redisclient.CheckKeys{
		APIKeys: k.apiKeys, LimitsPrefix: k.limPref, GcraPrefix: k.gcraPref, Stream: k.stream,
	}, redisclient.CheckParams{APIKeyHash: "testhash", Cost: 1, StreamMaxLen: 100000})
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if reply.Status != redisclient.StatusAllowed {
		t.Fatalf("status = %v, want allowed", reply.Status)
	}
	if reply.Remaining != 9 {
		t.Fatalf("remaining = %d, want 9", reply.Remaining)
	}
	if reply.ClientID != "client-w" {
		t.Fatalf("client_id = %q, want client-w", reply.ClientID)
	}
	if reply.Rate != 5 || reply.Period != "second" || reply.Burst != 10 {
		t.Fatalf("limits wrong: %+v", reply)
	}
	if reply.NowMS <= 0 || reply.ResetAtMS < reply.NowMS || reply.ResetAtMS > reply.NowMS+500 {
		t.Fatalf("reset_at %d not within [now=%d, now+500]", reply.ResetAtMS, reply.NowMS)
	}
	if !reply.StreamOK {
		t.Fatalf("StreamOK = false, want true on a normal allow")
	}
}

func TestCheckerDeniedThenAllowedStatuses(t *testing.T) {
	c := newTestClient(t)
	ch := redisclient.NewChecker(c)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-x", `{"rate":1,"period":"second","burst":1}`)

	params := redisclient.CheckParams{APIKeyHash: "testhash", Cost: 1, StreamMaxLen: 100000}
	keys := redisclient.CheckKeys{
		APIKeys: k.apiKeys, LimitsPrefix: k.limPref, GcraPrefix: k.gcraPref, Stream: k.stream,
	}

	first, err := ch.Check(clientCtx, c, keys, params)
	if err != nil || first.Status != redisclient.StatusAllowed {
		t.Fatalf("first: err=%v status=%v, want allowed", err, first.Status)
	}
	second, err := ch.Check(clientCtx, c, keys, params)
	if err != nil || second.Status != redisclient.StatusDenied {
		t.Fatalf("second: err=%v status=%v, want denied", err, second.Status)
	}
}

func TestCheckerUnauthorizedStatus(t *testing.T) {
	c := newTestClient(t)
	ch := redisclient.NewChecker(c)
	k := checkTestKeys(t)

	reply, err := ch.Check(clientCtx, c, redisclient.CheckKeys{
		APIKeys: k.apiKeys, LimitsPrefix: k.limPref, GcraPrefix: k.gcraPref, Stream: k.stream,
	}, redisclient.CheckParams{APIKeyHash: "unknown-hash", Cost: 1, StreamMaxLen: 100000})
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if reply.Status != redisclient.StatusUnauthorized {
		t.Fatalf("status = %v, want unauthorized", reply.Status)
	}
}

func TestCheckerLimitsInvalidStatus(t *testing.T) {
	c := newTestClient(t)
	ch := redisclient.NewChecker(c)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-y", `{"rate":1,"period":"fortnight","burst":1}`)

	reply, err := ch.Check(clientCtx, c, redisclient.CheckKeys{
		APIKeys: k.apiKeys, LimitsPrefix: k.limPref, GcraPrefix: k.gcraPref, Stream: k.stream,
	}, redisclient.CheckParams{APIKeyHash: "testhash", Cost: 1, StreamMaxLen: 100000})
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if reply.Status != redisclient.StatusLimitsInvalid {
		t.Fatalf("status = %v, want limits invalid", reply.Status)
	}
}

func TestCheckerBadCostStatus(t *testing.T) {
	c := newTestClient(t)
	ch := redisclient.NewChecker(c)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-z", `{"rate":5,"period":"second","burst":10}`)

	reply, err := ch.Check(clientCtx, c, redisclient.CheckKeys{
		APIKeys: k.apiKeys, LimitsPrefix: k.limPref, GcraPrefix: k.gcraPref, Stream: k.stream,
	}, redisclient.CheckParams{APIKeyHash: "testhash", Cost: 0, StreamMaxLen: 100000})
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if reply.Status != redisclient.StatusBadParams {
		t.Fatalf("status = %v, want bad params", reply.Status)
	}
}

func TestCheckerAllowWithFailedStream(t *testing.T) {
	c := newTestClient(t)
	ch := redisclient.NewChecker(c)
	k := checkTestKeys(t)
	seedCheckClient(t, k, "client-aa", `{"rate":5,"period":"second","burst":10}`)
	// A non-stream value where the script expects a stream makes the XADD fail.
	if err := c.Set(clientCtx, k.stream, "not-a-stream", 0).Err(); err != nil {
		t.Fatalf("seed broken stream: %v", err)
	}

	reply, err := ch.Check(clientCtx, c, redisclient.CheckKeys{
		APIKeys: k.apiKeys, LimitsPrefix: k.limPref, GcraPrefix: k.gcraPref, Stream: k.stream,
	}, redisclient.CheckParams{APIKeyHash: "testhash", Cost: 1, StreamMaxLen: 100000})
	if err != nil {
		t.Fatalf("check with broken stream must not error: %v", err)
	}
	if reply.Status != redisclient.StatusAllowed {
		t.Fatalf("status = %v, want allowed despite the failed push", reply.Status)
	}
	if reply.StreamOK {
		t.Fatalf("StreamOK = true, want false when the push failed")
	}
}
