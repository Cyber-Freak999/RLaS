package internal

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"rlas/redis"
)

// replicaID is the container/process hostname, resolved once at startup. Each
// replica in the scaled compose service has a unique container hostname, which
// is what lets an observer correlate /v1/check responses with the replica that
// served them (X-Replica header).
var replicaID = func() string {
	if h, err := os.Hostname(); err == nil {
		return h
	}
	return "unknown"
}()

// Limiter is the /v1/check engine. Auth, limit lookup, the GCRA bucket, and
// the Streams XADD all happen inside check.lua, so one /v1/check is exactly
// one Redis round trip (constraint 1) and this service's Redis surface stays
// minimal (constraint 2).
type Limiter struct {
	redis     redis.Cmdable
	checker   *redisclient.Checker
	checkKeys redisclient.CheckKeys
	breaker   *circuitBreaker
	cache     *fallbackStore
	logger    *slog.Logger
	now       func() time.Time
}

type LimiterOptions struct {
	Redis   redis.Cmdable
	Checker *redisclient.Checker
	Logger  *slog.Logger
}

func NewLimiter(opts LimiterOptions) *Limiter {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Checker == nil {
		opts.Checker = redisclient.NewChecker(opts.Redis)
	}
	now := time.Now
	l := &Limiter{
		redis:   opts.Redis,
		checker: opts.Checker,
		// The script receives the schema as KEYS and resolves per-client keys
		// itself; passing them here keeps the schema in exactly one place.
		checkKeys: redisclient.CheckKeys{
			APIKeys:      APIKeysKey,
			LimitsPrefix: limitsKeyPref,
			GcraPrefix:   gcraKeyPref,
			Stream:       StreamKey,
		},
		breaker: newCircuitBreaker(opts.Logger, now),
		cache:   newFallbackStore(),
		logger:  opts.Logger,
		now:     now,
	}
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

// Check runs the happy path against Redis in one atomic EVAL and falls back to
// the local token bucket on any check-path Redis failure.
func (l *Limiter) Check(ctx context.Context, apiKey string, cost int64) CheckOutcome {
	hash := HashAPIKey(apiKey)

	reply, err := l.checker.Check(ctx, l.redis, l.checkKeys, redisclient.CheckParams{
		APIKeyHash:   hash,
		Cost:         cost,
		StreamMaxLen: StreamMaxLen,
	})
	if err != nil {
		// A failed EVAL means Redis didn't answer (network/script error), never
		// a deliberate deny — fail open into the degraded path.
		l.breaker.onFailure()
		return l.fallback(hash, cost)
	}
	l.breaker.onSuccess() // Redis answered; any status is a decision, not an outage.

	switch reply.Status {
	case redisclient.StatusUnauthorized:
		return CheckOutcome{Err: ErrUnauthorized}
	case redisclient.StatusLimitsNotFound:
		return CheckOutcome{Err: ErrLimitsNotFound}
	case redisclient.StatusLimitsInvalid:
		return CheckOutcome{Err: ErrLimitsInvalid}
	case redisclient.StatusBadParams:
		// The HTTP boundary validates cost >= 1, so reaching this is a
		// programming error and must not masquerade as a deny or an outage.
		l.logger.Error("check_bad_params", "cost", cost)
		return CheckOutcome{Err: errors.New("bad check parameters")}
	}

	// Allowed or denied: refresh the fail-open cache from the same round trip
	// that produced the answer. The key is remembered whenever Redis resolved
	// it; limits only when they were present and valid.
	if reply.ClientID != "" {
		l.cache.rememberKey(hash, reply.ClientID)
	}
	if reply.Status == redisclient.StatusAllowed || reply.Status == redisclient.StatusDenied {
		l.cache.rememberLimits(reply.ClientID, Limits{Rate: reply.Rate, Period: reply.Period, Burst: reply.Burst})
	}

	if reply.Status == redisclient.StatusAllowed {
		// The script's analytics push is non-fatal by design (architecture
		// doc: logging never slows or breaks /check). When it failed, the
		// request is still approved and the token still consumed — the only
		// consequence is a missing event for the Streams consumer, which we
		// surface as a structured log line so it can be alerted on.
		if !reply.StreamOK {
			l.logger.Error("stream_xadd_failed", "client_id", reply.ClientID, "cost", cost)
		}
		return CheckOutcome{Allowed: true, Remaining: reply.Remaining, ResetAtMS: reply.ResetAtMS, NowMS: reply.NowMS}
	}
	return CheckOutcome{Allowed: false, Remaining: 0, ResetAtMS: reply.ResetAtMS, NowMS: reply.NowMS}
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

	// Stamp every response with the serving replica's hostname (M4). nginx
	// passes it through, so tests can prove traffic spread across replicas.
	w.Header().Set("X-Replica", replicaID)

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
