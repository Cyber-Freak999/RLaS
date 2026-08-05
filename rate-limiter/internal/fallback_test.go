package internal

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestBreakerTripsAfterThree(t *testing.T) {
	var buf bytes.Buffer
	b := newCircuitBreaker(slog.New(slog.NewJSONHandler(&buf, nil)), time.Now)

	for i := 0; i < 2; i++ {
		b.onFailure()
		if b.isOpen() {
			t.Fatalf("breaker opened after %d failures, want after 3", i+1)
		}
	}
	b.onFailure()
	if !b.isOpen() {
		t.Fatalf("breaker should open after 3 consecutive failures")
	}
	if !strings.Contains(buf.String(), `"msg":"circuit_breaker_tripped"`) {
		t.Fatalf("missing trip event, got %s", buf.String())
	}
	if !strings.Contains(buf.String(), `"reason":"redis_unreachable"`) {
		t.Fatalf("trip event must carry reason=redis_unreachable, got %s", buf.String())
	}
	if !strings.Contains(buf.String(), `"ts":"`) {
		t.Fatalf("trip event must carry ts, got %s", buf.String())
	}
}

func TestBreakerRecoversAfterOneSuccess(t *testing.T) {
	var buf bytes.Buffer
	b := newCircuitBreaker(slog.New(slog.NewJSONHandler(&buf, nil)), time.Now)
	for i := 0; i < 3; i++ {
		b.onFailure()
	}
	if !b.isOpen() {
		t.Fatalf("precondition: breaker must be open")
	}
	b.onSuccess()
	if b.isOpen() {
		t.Fatalf("one success must recover the breaker")
	}
	if !strings.Contains(buf.String(), `"msg":"circuit_breaker_recovered"`) {
		t.Fatalf("missing recover event, got %s", buf.String())
	}
}

func TestBreakerDoesNotRelogWhileOpen(t *testing.T) {
	var buf bytes.Buffer
	b := newCircuitBreaker(slog.New(slog.NewJSONHandler(&buf, nil)), time.Now)
	for i := 0; i < 6; i++ {
		b.onFailure()
	}
	if n := strings.Count(buf.String(), "circuit_breaker_tripped"); n != 1 {
		t.Fatalf("trip logged %d times while staying open, want 1", n)
	}
}

func TestBucketInitialTake(t *testing.T) {
	b := &bucket{}
	allowed, remaining, resetAt := b.take(0, 5, 1, 2) // burst 5, 1 token/sec
	if !allowed {
		t.Fatalf("initial take with 2 tokens should be allowed")
	}
	if remaining != 3 {
		t.Fatalf("remaining = %d, want 3", remaining)
	}
	// Full again when 2 tokens refill at 1/sec => now + 2000ms.
	if resetAt != 2000 {
		t.Fatalf("resetAt = %d, want 2000", resetAt)
	}
}

func TestBucketRefills(t *testing.T) {
	b := &bucket{}
	b.take(0, 5, 1, 2) // consume 2, remaining 3
	allowed, remaining, _ := b.take(1000, 5, 1, 4)
	// 1s elapsed refills 1 token: 3+1 = 4, cost 4 -> allowed, remaining 0.
	if !allowed || remaining != 0 {
		t.Fatalf("allowed=%v remaining=%d, want true/0", allowed, remaining)
	}
}

func TestBucketDeny(t *testing.T) {
	b := &bucket{}
	b.take(0, 2, 1, 2) // empty
	allowed, remaining, resetAt := b.take(0, 2, 1, 1)
	if allowed {
		t.Fatalf("empty bucket must deny")
	}
	if remaining != 0 {
		t.Fatalf("remaining = %d, want 0", remaining)
	}
	// One token at 1/sec => wait 1000ms.
	if resetAt != 1000 {
		t.Fatalf("resetAt = %d, want 1000", resetAt)
	}
}

func TestFallbackStoreUnknownAndKnown(t *testing.T) {
	f := newFallbackStore()
	if _, ok := f.lookup("nope"); ok {
		t.Fatalf("unknown key must not resolve")
	}
	f.rememberKey("hash", "c1")
	f.rememberLimits("c1", Limits{Rate: 2, Period: "second", Burst: 4})
	if id, ok := f.lookup("hash"); !ok || id != "c1" {
		t.Fatalf("lookup = %q, %v", id, ok)
	}
	allowed, remaining, resetAt, ok := f.take("c1", 0, 2)
	if !ok || !allowed || remaining != 2 {
		t.Fatalf("take = allowed=%v remaining=%d resetAt=%d ok=%v", allowed, remaining, resetAt, ok)
	}
	// Unknown client without limits -> not ok.
	_, _, _, ok = f.take("noclient", 0, 1)
	if ok {
		t.Fatalf("take for client without limits must report not-ok")
	}
}

func TestFallbackStoreBoundedByBurst(t *testing.T) {
	f := newFallbackStore()
	f.rememberKey("h", "c")
	f.rememberLimits("c", Limits{Rate: 1, Period: "hour", Burst: 1})
	// A huge gap of inactivity must not grow the bucket past burst.
	allowed, remaining, _, ok := f.take("c", 60*60*1000*100, 1)
	if !ok || !allowed || remaining != 0 {
		t.Fatalf("allowed=%v remaining=%d, want true/0 (capped at burst)", allowed, remaining)
	}
}
