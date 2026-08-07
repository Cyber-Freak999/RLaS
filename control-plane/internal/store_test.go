package internal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestStore connects to the TimescaleDB named by TIMESCALE_DSN and applies
// the schema. These tests are TimescaleDB-gated: they skip in CI (no DSN) and
// run in the compose gate, mirroring how the rate-limiter's redis-gated suites
// skip when Redis is unreachable.
func newTestStore(t *testing.T) *PGStore {
	t.Helper()
	dsn := os.Getenv("TIMESCALE_DSN")
	if dsn == "" {
		t.Skip("TIMESCALE_DSN not set; skipping TimescaleDB-gated tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("timescaledb unreachable: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("timescaledb unreachable: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := CreateSchema(ctx, pool); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return NewPGStore(pool)
}

func uniqueEventID(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "evt-" + hex.EncodeToString(b[:])
}

func TestStoreInsertAndRead(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	id := uniqueEventID(t)
	if err := s.InsertApproved(ctx, id, "client-A", 2, 1_700_000_000_123); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var clientID string
	var cost int
	var ts time.Time
	if err := s.pool.QueryRow(ctx,
		`SELECT client_id, cost, ts FROM approved_requests WHERE event_id = $1`, id,
	).Scan(&clientID, &cost, &ts); err != nil {
		t.Fatalf("select: %v", err)
	}
	if clientID != "client-A" || int64(cost) != 2 {
		t.Fatalf("row = %s/%d, want client-A/2", clientID, cost)
	}
	if ts.UnixMilli() != 1_700_000_000_123 {
		t.Fatalf("ts = %v, want 1700000000123 ms", ts)
	}
}

func TestStoreIdempotentOnDuplicateEventID(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	id := uniqueEventID(t)
	if err := s.InsertApproved(ctx, id, "client-B", 1, 1_700_000_000_000); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	// At-least-once redelivery (constraint 5): the same event_id arriving again
	// must not create a second row.
	if err := s.InsertApproved(ctx, id, "client-B", 1, 1_700_000_000_000); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM approved_requests WHERE event_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("duplicate event_id produced %d rows, want 1", n)
	}
}
