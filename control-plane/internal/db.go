package internal

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CreateSchema applies the analytics schema idempotently at control-plane
// startup. Statements run separately: a single Exec string with multiple
// statements is not valid under pgx's extended query protocol, and the
// hypertable conversion must follow the table creation anyway.
func CreateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	stmts := []string{
		// Managed Postgres (Render's TimescaleDB offering) ships the extension
		// installed but not always enabled in the target database; the local
		// compose image pre-enables it. IF NOT EXISTS makes this a no-op where
		// it's already active and a safe enable everywhere else. It must run
		// before create_hypertable, which lives in the extension.
		`CREATE EXTENSION IF NOT EXISTS timescaledb`,
		// The primary key must include ts, the partitioning column: TimescaleDB
		// rejects any unique index that omits a partitioning column (SQLSTATE
		// TS103). A redelivered stream entry carries the same event_id AND the
		// same ts, so (event_id, ts) still dedupes at-least-once delivery.
		`CREATE TABLE IF NOT EXISTS approved_requests (
			event_id  text NOT NULL,
			client_id text NOT NULL,
			cost      int NOT NULL,
			ts        timestamptz NOT NULL,
			PRIMARY KEY (event_id, ts)
		)`,
		// Time-series hypertable on ts; if_not_exists makes restart-safe.
		`SELECT create_hypertable('approved_requests', 'ts', if_not_exists => TRUE)`,
		`CREATE INDEX IF NOT EXISTS idx_approved_requests_client_ts
		 ON approved_requests (client_id, ts)`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}
