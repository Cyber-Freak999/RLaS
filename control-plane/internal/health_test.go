package internal

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/redis/go-redis/v9"
)

// healthAdmin builds an Admin whose dependency pingers are fully injected, so
// the 200/503 branches of /healthz are unit-tested without real dependencies
// (constraint 11). The redis client is never touched on the healthz path.
func healthAdmin(redisPinger, dbPinger Pinger) *Admin {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	return NewAdmin(c, &nopStore{}, redisPinger, dbPinger, "sekret", testLogger())
}

func getHealthz(t *testing.T, a *Admin) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	a.Handler().ServeHTTP(rr, req)
	return rr.Code
}

func TestHealthz200WhenBothDepsUp(t *testing.T) {
	if got := getHealthz(t, healthAdmin(healthyPinger(), healthyPinger())); got != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", got)
	}
}

func TestHealthz503WhenRedisDown(t *testing.T) {
	a := healthAdmin(failingPinger(errors.New("redis unreachable")), healthyPinger())
	if got := getHealthz(t, a); got != http.StatusServiceUnavailable {
		t.Fatalf("healthz = %d, want 503", got)
	}
}

func TestHealthz503WhenDBDown(t *testing.T) {
	a := healthAdmin(healthyPinger(), failingPinger(errors.New("db unreachable")))
	if got := getHealthz(t, a); got != http.StatusServiceUnavailable {
		t.Fatalf("healthz = %d, want 503", got)
	}
}
