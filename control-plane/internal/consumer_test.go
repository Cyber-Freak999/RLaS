package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
)

var errInsertFailed = errors.New("insert failed")

// fakeStore records inserts and can be made to fail to exercise the
// never-XACK-on-transient-failure path of the consumer.
type fakeStore struct {
	mu   sync.Mutex
	rows []storeRow
	fail bool
}

type storeRow struct {
	eventID  string
	clientID string
	cost     int64
	tsMS     int64
}

func (s *fakeStore) InsertApproved(_ context.Context, eventID, clientID string, cost int64, tsMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errInsertFailed
	}
	s.rows = append(s.rows, storeRow{eventID: eventID, clientID: clientID, cost: cost, tsMS: tsMS})
	return nil
}

// recordHandler collects message names so tests can assert on structured-log
// events without a real sink.
type recordHandler struct{ records *[]string }

func (h recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h recordHandler) Handle(_ context.Context, r slog.Record) error {
	*h.records = append(*h.records, r.Message)
	return nil
}
func (h recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recordHandler) WithGroup(string) slog.Handler      { return h }

func captureLogger() (*slog.Logger, *[]string) {
	var records []string
	return slog.New(recordHandler{records: &records}), &records
}

// newTestConsumer creates an isolated stream+group so tests don't collide with
// each other or with a running control-plane. The group is created at "$"
// before any XADD, so entries added afterwards are delivered.
func newTestConsumer(t *testing.T, c *redis.Client, logger *slog.Logger) (*Consumer, *fakeStore, string, string) {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "_")
	stream := "stream:test:" + name
	group := "group:test:" + name
	ctx := context.Background()
	if err := c.XGroupCreateMkStream(ctx, stream, group, "$").Err(); err != nil {
		t.Fatalf("create test group: %v", err)
	}
	store := &fakeStore{}
	cons := NewConsumer(c, store, logger, ConsumerOptions{
		StreamKey:        stream,
		Group:            group,
		LagWarnThreshold: 2,
		RetryBackoff:     10 * time.Millisecond,
	})
	t.Cleanup(func() { _ = c.Del(ctx, stream).Err() })
	return cons, store, stream, group
}

func xadd(t *testing.T, c *redis.Client, stream, clientID string, cost, tsMS int64) {
	t.Helper()
	if _, err := c.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"client_id": clientID, "cost": cost, "ts": tsMS},
	}).Result(); err != nil {
		t.Fatalf("xadd: %v", err)
	}
}

func pendingCount(t *testing.T, c *redis.Client, stream, group string) int64 {
	t.Helper()
	p, err := c.XPending(context.Background(), stream, group).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	return p.Count
}

func TestConsumerProcessesAndAcks(t *testing.T) {
	c := testRedis(t)
	logger, _ := captureLogger()
	cons, store, stream, group := newTestConsumer(t, c, logger)
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		xadd(t, c, stream, "client-A", i, 1000+i)
	}
	if err := cons.runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}

	if len(store.rows) != 3 {
		t.Fatalf("inserted %d rows, want 3", len(store.rows))
	}
	for i, r := range store.rows {
		if r.clientID != "client-A" || r.cost != int64(i+1) || r.tsMS != int64(1001+i) {
			t.Fatalf("row %d = %+v, want client-A cost=%d ts=%d", i, r, i+1, 1001+i)
		}
	}
	// Acked after insert: nothing left pending.
	if n := pendingCount(t, c, stream, group); n != 0 {
		t.Fatalf("pending after ack = %d, want 0", n)
	}
}

func TestConsumerDoesNotAckOnInsertFailureThenRecovers(t *testing.T) {
	c := testRedis(t)
	cons, store, stream, group := newTestConsumer(t, c, testLogger())
	ctx := context.Background()

	xadd(t, c, stream, "client-B", 1, 2000)
	store.fail = true
	if err := cons.runOnce(ctx); err == nil {
		t.Fatal("runOnce with failing store = nil error, want error (no ack)")
	}
	if n := pendingCount(t, c, stream, group); n != 1 {
		t.Fatalf("pending after failed insert = %d, want 1 (must not XACK)", n)
	}

	store.fail = false
	if err := cons.runOnce(ctx); err != nil {
		t.Fatalf("runOnce after recovery: %v", err)
	}
	if len(store.rows) != 1 {
		t.Fatalf("inserted %d rows after recovery, want 1", len(store.rows))
	}
	if n := pendingCount(t, c, stream, group); n != 0 {
		t.Fatalf("pending after recovery = %d, want 0", n)
	}
}

func TestConsumerAcksPoisonEntry(t *testing.T) {
	c := testRedis(t)
	cons, store, stream, group := newTestConsumer(t, c, testLogger())
	ctx := context.Background()

	// Entry with a non-numeric cost: unparseable, must be XACKed and skipped,
	// not retried forever.
	if _, err := c.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"client_id": "client-C", "cost": "not-a-number", "ts": 3000},
	}).Result(); err != nil {
		t.Fatalf("xadd poison: %v", err)
	}

	if err := cons.runOnce(ctx); err != nil {
		t.Fatalf("runOnce on poison = %v, want nil (consumer continues)", err)
	}
	if len(store.rows) != 0 {
		t.Fatalf("poison entry was inserted (%d rows), want 0", len(store.rows))
	}
	if n := pendingCount(t, c, stream, group); n != 0 {
		t.Fatalf("poison entry not acked (pending=%d), want 0", n)
	}
}

func TestConsumerWarnsOnStreamLag(t *testing.T) {
	c := testRedis(t)
	logger, records := captureLogger()
	cons, _, stream, _ := newTestConsumer(t, c, logger)
	ctx := context.Background()

	// Threshold is 2; XADD 3 so XLEN exceeds it on the next cycle.
	for i := int64(1); i <= 3; i++ {
		xadd(t, c, stream, "client-D", 1, 4000+i)
	}
	if err := cons.runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}

	for _, m := range *records {
		if m == "consumer_stream_lag" {
			return
		}
	}
	t.Fatalf("lag warning not logged; records=%v", *records)
}

func TestConsumerPublishesLagMetrics(t *testing.T) {
	c := testRedis(t)
	logger, _ := captureLogger()
	cons, _, stream, _ := newTestConsumer(t, c, logger)
	ctx := context.Background()

	// Clear any value left by an earlier test in this binary, then load the
	// stream with 3 entries. After a fully-drained cycle the entries are still
	// in the stream (XACK does not trim) and the group has 0 pending, so the
	// gauges must read XLEN=3 / XPENDING=0.
	streamEntries.Set(0)
	streamPending.Set(0)
	for i := int64(1); i <= 3; i++ {
		xadd(t, c, stream, "client-F", 1, 5000+i)
	}
	if err := cons.runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	if got := testutil.ToFloat64(streamEntries); got != 3 {
		t.Fatalf("rlas_stream_entries = %v, want 3", got)
	}
	if got := testutil.ToFloat64(streamPending); got != 0 {
		t.Fatalf("rlas_stream_pending = %v, want 0", got)
	}
}

func TestConsumerDoesNotCrashWhenRedisDown(t *testing.T) {
	bad := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	cons := NewConsumer(bad, &fakeStore{}, testLogger(), ConsumerOptions{
		StreamKey: "stream:test:down", Group: "group:test:down",
		RetryBackoff: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("consumer panicked with redis down: %v", r)
			}
			close(done)
		}()
		cons.Run(ctx)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestRunOnceErrorsWhenRedisUnreachable(t *testing.T) {
	bad := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	cons := NewConsumer(bad, &fakeStore{}, testLogger(), ConsumerOptions{
		StreamKey: "stream:test:down", Group: "group:test:down",
	})
	if err := cons.runOnce(context.Background()); err == nil {
		t.Fatal("runOnce against unreachable redis = nil, want error")
	}
}

func TestConsumerParsesStreamEntryFields(t *testing.T) {
	c := testRedis(t)
	cons, store, stream, _ := newTestConsumer(t, c, testLogger())
	ctx := context.Background()

	// The rate-limiter XADDs client_id/cost/ts where ts is the GCRA check time
	// in milliseconds (rate-limiter streams.go). The consumer must round-trip
	// those exact field names.
	if _, err := c.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"client_id": "client-E", "cost": fmt.Sprint(7), "ts": fmt.Sprint(9_999_999_999)},
	}).Result(); err != nil {
		t.Fatalf("xadd: %v", err)
	}
	if err := cons.runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	if len(store.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(store.rows))
	}
	r := store.rows[0]
	if r.clientID != "client-E" || r.cost != 7 || r.tsMS != 9_999_999_999 {
		t.Fatalf("parsed row = %+v, want client-E cost=7 ts=9999999999", r)
	}
}
