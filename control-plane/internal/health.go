package internal

import (
	"context"
	"net/http"
	"time"
)

// Pinger is the seam that makes /healthz unit-testable: production wires real
// Redis and TimescaleDB checkers, tests wire fakes (constraint 11 requires the
// production health check to be real — the fakes only exist to test the
// handler's status logic).
type Pinger interface {
	Ping(ctx context.Context) error
}

// pingerFunc adapts a function to Pinger.
type pingerFunc func(ctx context.Context) error

func (f pingerFunc) Ping(ctx context.Context) error { return f(ctx) }

// healthz reports 200 only when both dependencies answer, else 503. This is
// what compose's service_healthy condition depends on — a health check that
// always returns 200 would make startup ordering silently meaningless
// (constraint 11).
func (a *Admin) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.redisPinger.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "redis unreachable")
		return
	}
	if err := a.dbPinger.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
