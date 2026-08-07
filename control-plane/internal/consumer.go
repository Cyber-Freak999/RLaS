package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Consumer drains stream:approved_requests and writes each event to the Store
// — the only writer of TimescaleDB in the system (constraint 5). Delivery is
// at-least-once: an entry is XACKed only after the store write succeeds, and a
// pending (delivered-but-unacked) entry left by a crash is re-read on the next
// cycle and re-inserted idempotently. Unparseable entries are XACKed and
// logged so one poison message cannot wedge the group.
type Consumer struct {
	client  redis.Cmdable
	store   Store
	logger  *slog.Logger
	stream  string
	group   string
	lagWarn int64
	backoff time.Duration

	groupReady bool
}

// ConsumerOptions configures the consumer; zero values fall back to the
// production defaults below. Tests override the stream/group names and shrink
// the thresholds so behavior is observable without a real workload.
type ConsumerOptions struct {
	StreamKey        string
	Group            string
	LagWarnThreshold int64
	RetryBackoff     time.Duration
}

const (
	defaultStreamKey     = "stream:approved_requests"
	defaultGroup         = "control_plane"
	defaultLagWarn       = 50_000 // constraint 10: warn before the MAXLEN trim starts dropping
	defaultRetryBackoff  = time.Second
	consumerReadBlock    = time.Second
	consumerName         = "control-plane-consumer"
	consumerReadCount    = 100
)

func NewConsumer(client redis.Cmdable, store Store, logger *slog.Logger, opts ConsumerOptions) *Consumer {
	if opts.StreamKey == "" {
		opts.StreamKey = defaultStreamKey
	}
	if opts.Group == "" {
		opts.Group = defaultGroup
	}
	if opts.LagWarnThreshold == 0 {
		opts.LagWarnThreshold = defaultLagWarn
	}
	if opts.RetryBackoff == 0 {
		opts.RetryBackoff = defaultRetryBackoff
	}
	return &Consumer{
		client:  client,
		store:   store,
		logger:  logger,
		stream:  opts.StreamKey,
		group:   opts.Group,
		lagWarn: opts.LagWarnThreshold,
		backoff: opts.RetryBackoff,
	}
}

// Run loops until ctx is done. Blocking reads pace the success path; on any
// error the consumer backs off and retries rather than crashing (constraint:
// the control-plane stays up while Redis or the DB recovers).
func (c *Consumer) Run(ctx context.Context) {
	for {
		if err := c.runOnce(ctx); err != nil {
			c.logger.Error("consumer_cycle_error", "error", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.backoff):
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// runOnce is one consume cycle, split so tests exercise it deterministically.
func (c *Consumer) runOnce(ctx context.Context) error {
	if err := c.ensureGroup(ctx); err != nil {
		return err
	}
	// New messages first, then any pending (unacked) ones — the crash-recovery
	// redelivery path that makes delivery at-least-once.
	if err := c.processStream(ctx, ">"); err != nil {
		return err
	}
	if err := c.processStream(ctx, "0"); err != nil {
		return err
	}
	c.warnIfLagging(ctx)
	return nil
}

// ensureGroup creates the consumer group once. BUSYGROUP is expected on
// restart and is not an error.
func (c *Consumer) ensureGroup(ctx context.Context) error {
	if c.groupReady {
		return nil
	}
	err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "$").Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		c.groupReady = true
		return nil
	}
	return fmt.Errorf("create group: %w", err)
}

// processStream reads one batch from the group. Cursor ">" reads new messages
// (blocking); cursor "0" reads the group's pending list, which is where a
// crash-between-insert-and-XACK leaves entries for redelivery.
func (c *Consumer) processStream(ctx context.Context, cursor string) error {
	args := &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: consumerName,
		Streams:  []string{c.stream, cursor},
		Count:    consumerReadCount,
	}
	if cursor == ">" {
		args.Block = consumerReadBlock
	}
	streams, err := c.client.XReadGroup(ctx, args).Result()
	if errors.Is(err, redis.Nil) || (err == nil && len(streams) == 0) {
		return nil
	}
	if err != nil {
		return err
	}
	return c.processMessages(ctx, streams[0].Messages)
}

// processMessages writes each entry then XACKs it. A failed write returns an
// error without an XACK, so the entry stays pending and is retried. A poison
// entry is XACKed so the group cannot be wedged by one bad message.
func (c *Consumer) processMessages(ctx context.Context, msgs []redis.XMessage) error {
	for _, m := range msgs {
		evt, ok := parseEvent(m)
		if !ok {
			c.logger.Error("consumer_poison_entry", "id", m.ID, "error", "missing or invalid client_id/cost/ts")
			if err := c.client.XAck(ctx, c.stream, c.group, m.ID).Err(); err != nil {
				return fmt.Errorf("ack poison %s: %w", m.ID, err)
			}
			continue
		}
		if err := c.store.InsertApproved(ctx, evt.eventID, evt.clientID, evt.cost, evt.tsMS); err != nil {
			// No XACK: transient failure, leave pending for redelivery.
			return fmt.Errorf("insert %s: %w", evt.eventID, err)
		}
		if err := c.client.XAck(ctx, c.stream, c.group, m.ID).Err(); err != nil {
			return fmt.Errorf("ack %s: %w", m.ID, err)
		}
	}
	return nil
}

// warnIfLagging emits the constraint-10 signal that the consumer is falling
// behind before the MAXLEN trim actually drops events.
func (c *Consumer) warnIfLagging(ctx context.Context) {
	n, err := c.client.XLen(ctx, c.stream).Result()
	if err != nil {
		return
	}
	if n > c.lagWarn {
		c.logger.Warn("consumer_stream_lag", "stream_len", n, "threshold", c.lagWarn)
	}
}

type streamEvent struct {
	eventID  string
	clientID string
	cost     int64
	tsMS     int64
}

// parseEvent extracts the fields the rate-limiter XADDs (streams.go:
// client_id, cost, ts where ts is the GCRA check time in ms). Missing or
// malformed fields mark the entry as poison.
func parseEvent(m redis.XMessage) (streamEvent, bool) {
	clientID, _ := m.Values["client_id"].(string)
	if clientID == "" {
		return streamEvent{}, false
	}
	cost, err := parseInt64(m.Values["cost"])
	if err != nil {
		return streamEvent{}, false
	}
	tsMS, err := parseInt64(m.Values["ts"])
	if err != nil {
		return streamEvent{}, false
	}
	return streamEvent{eventID: m.ID, clientID: clientID, cost: cost, tsMS: tsMS}, true
}

func parseInt64(v interface{}) (int64, error) {
	switch x := v.(type) {
	case string:
		return strconv.ParseInt(x, 10, 64)
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case float64:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("cannot parse %T as int64", v)
	}
}
