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
		`CREATE TABLE IF NOT EXISTS approved_requests (
			event_id  text PRIMARY KEY,
			client_id text NOT NULL,
			cost      int NOT NULL,
			ts        timestamptz NOT NULL
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
