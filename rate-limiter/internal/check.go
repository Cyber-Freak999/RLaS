package internal

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"rlas/redis"
)

// Limiter is the /v1/check engine. It talks to Redis only for: API-key lookup,
// limit-config read, the GCRA EVAL, and the Streams XADD (constraint 2).
type Limiter struct {
	redis   redis.Cmdable
	gcra    *redisclient.GCRA
	breaker *circuitBreaker
	cache   *fallbackStore
	streams *StreamWriter
	logger  *slog.Logger
	now     func() time.Time
}

type LimiterOptions struct {
	Redis  redis.Cmdable
	GCRA   *redisclient.GCRA
	Logger *slog.Logger
}

func NewLimiter(opts LimiterOptions) *Limiter {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	now := time.Now
	l := &Limiter{
		redis:   opts.Redis,
		gcra:    opts.GCRA,
		breaker: newCircuitBreaker(opts.Logger, now),
		cache:   newFallbackStore(),
		logger:  opts.Logger,
		now:     now,
	}
	l.streams = newStreamWriter(opts.Redis, opts.Logger)
	return l
}

// CheckOutcome is the internal result of one /v1/check. The HTTP handler maps
// it to the documented JSON contract.
type CheckOutcome struct {
	Allowed   bool
	Remaining int64
	ResetAtMS int64
	NowMS     int64
	Degraded  bool
	Err       error
}

// Check runs the happy path against Redis and falls back to the local token
// bucket on any check-path Redis failure.
func (l *Limiter) Check(ctx context.Context, apiKey string, cost int64) CheckOutcome {
	hash := HashAPIKey(apiKey)

	clientID, err := Authenticate(ctx, l.redis, apiKey)
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			l.breaker.onSuccess() // Redis answered; the key is simply unknown.
			return CheckOutcome{Err: ErrUnauthorized}
		}
		l.breaker.onFailure()
		return l.fallback(hash, cost)
	}
	l.cache.rememberKey(hash, clientID)

	lim, err := LoadLimits(ctx, l.redis, clientID)
	if err != nil {
		if errors.Is(err, ErrLimitsNotFound) || errors.Is(err, ErrLimitsInvalid) {
			l.breaker.onSuccess() // Redis answered; stored config is broken.
			return CheckOutcome{Err: err}
		}
		l.breaker.onFailure()
		return l.fallback(hash, cost)
	}
	l.cache.rememberLimits(clientID, lim)

	res, err := l.gcra.Check(ctx, l.redis, gcraKey(clientID), redisclient.Params{
		Burst:     lim.Burst,
		Rate:      lim.Rate,
		PeriodSec: lim.PeriodSec(),
		Cost:      cost,
	})
	if err != nil {
		l.breaker.onFailure()
		return l.fallback(hash, cost)
	}
	l.breaker.onSuccess()

	if res.Allowed {
		// The approved response is committed; never let the analytics push turn
		// it into an error (constraint 5).
		l.streams.Push(ctx, clientID, cost, res.NowMS)
		return CheckOutcome{Allowed: true, Remaining: res.Remaining, ResetAtMS: res.ResetAtMS, NowMS: res.NowMS}
	}
	return CheckOutcome{Allowed: false, Remaining: 0, ResetAtMS: res.ResetAtMS, NowMS: res.NowMS}
}

// fallback serves a degraded response from cached knowledge. A key the replica
// has never seen can't be validated, so it is denied as unauthorized — the
// same answer the happy path would give for an unknown key.
func (l *Limiter) fallback(hash string, cost int64) CheckOutcome {
	clientID, known := l.cache.lookup(hash)
	if !known {
		return CheckOutcome{Err: ErrUnauthorized, Degraded: true}
	}
	nowMS := l.now().UnixMilli()
	allowed, remaining, resetAt, ok := l.cache.take(clientID, nowMS, cost)
	if !ok {
		return CheckOutcome{Err: ErrLimitsNotFound, Degraded: true}
	}
	return CheckOutcome{Allowed: allowed, Remaining: remaining, ResetAtMS: resetAt, NowMS: nowMS, Degraded: true}
}

// Health reports Redis reachability for /healthz (constraint 11).
func (l *Limiter) Health(ctx context.Context) error {
	return l.redis.Ping(ctx).Err()
}

func (l *Limiter) BreakerOpen() bool {
	return l.breaker.isOpen()
}

const maxCheckBody = 1 << 16 // 64 KiB is far beyond any sane check body.

func (l *Limiter) handleCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	apiKey := r.Header.Get("X-Api-Key")
	if apiKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing X-Api-Key"})
		return
	}

	// cost defaults to 1 and supports arbitrary positive integers (constraint
	// 7); anything non-positive or unparseable is rejected with 400.
	cost := int64(1)
	if r.Body != nil && r.Body != http.NoBody {
		body := struct {
			Cost *int64 `json:"cost"`
		}{}
		r.Body = http.MaxBytesReader(w, r.Body, maxCheckBody)
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
			return
		}
		if body.Cost != nil {
			if *body.Cost < 1 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cost must be a positive integer"})
				return
			}
			cost = *body.Cost
		}
	}

	start := time.Now()
	outcome := l.Check(r.Context(), apiKey, cost)
	observeCheck(time.Since(start), outcome)
	setBreakerOpen(l.breaker.isOpen())

	if outcome.Degraded {
		w.Header().Set("X-RateLimit-Degraded", "true")
	}

	switch {
	case errors.Is(outcome.Err, ErrUnauthorized):
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	case errors.Is(outcome.Err, ErrLimitsNotFound):
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "limits not configured"})
		return
	case errors.Is(outcome.Err, ErrLimitsInvalid):
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "limits misconfigured"})
		return
	case outcome.Err != nil:
		l.logger.Error("check_internal_error", "error", outcome.Err.Error())
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	if outcome.Allowed {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"allowed":   true,
			"remaining": outcome.Remaining,
			"reset_at":  outcome.ResetAtMS,
		})
		return
	}

	// Denied: Retry-After is computed on the same clock the bucket used, so
	// host/Redis skew can't stretch or shrink the retry window.
	retryAfter := int64(math.Ceil(float64(outcome.ResetAtMS-outcome.NowMS) / 1000.0))
	if retryAfter < 1 {
		retryAfter = 1
	}
	w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
	writeJSON(w, http.StatusTooManyRequests, map[string]interface{}{
		"allowed":   false,
		"remaining": 0,
		"reset_at":  outcome.ResetAtMS,
	})
}

func (l *Limiter) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := l.Health(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy", "error": "redis unreachable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Server assembles the HTTP surface. Kept separate from the Limiter so tests
// can exercise the handlers via httptest and main can own the listener.
func Server(l *Limiter) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/check", l.handleCheck)
	mux.HandleFunc("/healthz", l.handleHealth)
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}
