package internal

import (
	"context"
	"io"
	"log/slog"
)

// testLogger returns a logger that discards everything. Tests that need to
// assert on log events build their own handler (see consumer_test.go).
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// nopStore is the Store fake the admin tests wire in: the admin path never
// touches the analytics store, so a no-op keeps it out of the test's concern.
type nopStore struct{}

func (nopStore) InsertApproved(context.Context, string, string, int64, int64) error {
	return nil
}

// pingerFunc (defined in health.go) adapts a function to the Pinger interface.
// healthyPinger reports a dependency as up; failingPinger as down. healthz
// tests combine them to assert the 503 branches without real dependencies
// (constraint 11 behavior is proven against real deps in the compose gate).
func healthyPinger() Pinger {
	return pingerFunc(func(context.Context) error { return nil })
}

func failingPinger(err error) Pinger {
	return pingerFunc(func(context.Context) error { return err })
}
