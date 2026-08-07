package internal

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the persistence seam the consumer writes through. The pgx
// implementation is the only writer of TimescaleDB in the system (constraint
// 5); tests use in-memory fakes.
type Store interface {
	// InsertApproved writes one approved-request event. eventID is the stream
	// entry ID, tsMS the GCRA check time from the rate-limiter's script. The
	// implementation must be idempotent on eventID (at-least-once delivery).
	InsertApproved(ctx context.Context, eventID, clientID string, cost int64, tsMS int64) error
}

// PGStore writes approved-request events to TimescaleDB.
type PGStore struct {
	pool *pgxpool.Pool
}

func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

// InsertApproved inserts idempotently: ON CONFLICT (event_id) DO NOTHING makes
// a redelivered stream entry (crash between insert and XACK) a no-op rather
// than a duplicate billing row (constraint 5). ts is derived from the check
// time, not the consumer's wall clock, so analytics reflect when the request
// actually happened.
func (s *PGStore) InsertApproved(ctx context.Context, eventID, clientID string, cost int64, tsMS int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO approved_requests (event_id, client_id, cost, ts)
		 VALUES ($1, $2, $3, to_timestamp($4::double precision / 1000.0))
		 ON CONFLICT (event_id) DO NOTHING`,
		eventID, clientID, cost, tsMS)
	return err
}

// Ping reports TimescaleDB connectivity for /healthz (constraint 11).
func (s *PGStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}
