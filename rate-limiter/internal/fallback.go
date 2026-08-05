package internal

import (
	"log/slog"
	"math"
	"sync"
	"time"
)

// Fail-open fallback (constraint 4). When Redis is unreachable the limiter
// serves responses from a per-replica token bucket instead of erroring; every
// fallback-served response carries X-RateLimit-Degraded: true.
//
// The circuit breaker tracks the outage so the README/chaos verification can
// observe it via fixed structured log events. Serving the fallback starts on
// the first check-path Redis failure (fail-open, never a 503); the breaker
// flips "open" after breakerTripAt consecutive failures and "recovered" after
// one success.

type circuitBreaker struct {
	mu        sync.Mutex
	failures  int
	open      bool
	tripAfter int
	logger    *slog.Logger
	now       func() time.Time
}

func newCircuitBreaker(logger *slog.Logger, now func() time.Time) *circuitBreaker {
	return &circuitBreaker{tripAfter: breakerTripAt, logger: logger, now: now}
}

func (b *circuitBreaker) onFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if !b.open && b.failures >= b.tripAfter {
		b.open = true
		b.failures = 0
		b.logger.Info("circuit_breaker_tripped", "reason", "redis_unreachable",
			"ts", b.now().UTC().Format(time.RFC3339Nano))
	}
}

func (b *circuitBreaker) onSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	if b.open {
		b.open = false
		b.logger.Info("circuit_breaker_recovered", "ts", b.now().UTC().Format(time.RFC3339Nano))
	}
}

func (b *circuitBreaker) isOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}

// fallbackStore caches the key->client mapping and last-known per-client
// limits, refreshed on every successful Redis round trip. While Redis is
// unreachable this is the only source of truth the replica has.
type fallbackStore struct {
	mu          sync.RWMutex
	keyToClient map[string]string
	limits      map[string]Limits
	buckets     map[string]*bucket
}

func newFallbackStore() *fallbackStore {
	return &fallbackStore{
		keyToClient: make(map[string]string),
		limits:      make(map[string]Limits),
		buckets:     make(map[string]*bucket),
	}
}

func (f *fallbackStore) rememberKey(hash, clientID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keyToClient[hash] = clientID
	if _, ok := f.buckets[clientID]; !ok {
		f.buckets[clientID] = &bucket{}
	}
}

func (f *fallbackStore) rememberLimits(clientID string, l Limits) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limits[clientID] = l
}

func (f *fallbackStore) lookup(hash string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	id, ok := f.keyToClient[hash]
	return id, ok
}

// take advances the client's local bucket to nowMS and tries to consume cost
// tokens. Returns the outcome and whether limits were known at all.
func (f *fallbackStore) take(clientID string, nowMS, cost int64) (allowed bool, remaining, resetAtMS int64, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, known := f.limits[clientID]
	if !known {
		return false, 0, nowMS, false
	}
	ratePerSec := float64(l.Rate) / float64(l.PeriodSec())
	allowed, remaining, resetAtMS = f.buckets[clientID].take(nowMS, float64(l.Burst), ratePerSec, cost)
	return allowed, remaining, resetAtMS, true
}

// bucket is one replica-local token bucket. Tokens are refilled continuously
// so a replica that drifted behind a client's burst can still serve them.
type bucket struct {
	tokens     float64
	lastRefill int64 // unix ms of the previous take
	init       bool  // false until the first take seeds a full bucket
}

// take performs the refill-then-consume step. resetAt on allow is when the
// bucket is full again; on deny it is when cost tokens are available. This
// mirrors the GCRA script's reply semantics, but is deliberately approximate:
// it is per-replica state, used only to keep traffic flowing during an outage.
func (b *bucket) take(nowMS int64, capacity, ratePerSec float64, cost int64) (allowed bool, remaining, resetAtMS int64) {
	if !b.init {
		b.tokens = capacity
		b.init = true
	} else if elapsed := nowMS - b.lastRefill; elapsed > 0 {
		refill := (float64(elapsed) / 1000.0) * ratePerSec
		b.tokens = math.Min(capacity, b.tokens+refill)
	}
	b.lastRefill = nowMS

	if b.tokens < float64(cost) {
		need := float64(cost) - b.tokens
		wait := (need / ratePerSec) * 1000.0
		return false, int64(math.Floor(b.tokens)), nowMS + int64(math.Ceil(wait))
	}
	b.tokens -= float64(cost)
	remaining = int64(math.Floor(b.tokens))
	fullIn := ((capacity - b.tokens) / ratePerSec) * 1000.0
	return true, remaining, nowMS + int64(math.Ceil(fullIn))
}
