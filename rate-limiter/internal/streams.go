package internal

import (
	"context"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// StreamWriter pushes approved-request events onto the Streams transport
// (constraint 5). The response is committed before this is called, so a
// failure here must not change the response — it only means the analytics
// event is delayed, and is logged.
type StreamWriter struct {
	client redis.Cmdable
	key    string
	logger *slog.Logger
}

func newStreamWriter(client redis.Cmdable, logger *slog.Logger) *StreamWriter {
	return &StreamWriter{client: client, key: StreamKey, logger: logger}
}

// Push XADDs one approved event with a bounded, approximate length (constraint
// 10). ts is the check time from the GCRA script, so the consumer can report
// when the request actually happened, not when the event was written.
func (s *StreamWriter) Push(ctx context.Context, clientID string, cost, tsMS int64) {
	_, err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.key,
		MaxLen: StreamMaxLen,
		Approx: true,
		Values: map[string]interface{}{
			"client_id": clientID,
			"cost":      cost,
			"ts":        tsMS,
		},
	}).Result()
	if err != nil {
		s.logger.Error("stream_xadd_failed", "error", err.Error(), "client_id", clientID)
	}
}
