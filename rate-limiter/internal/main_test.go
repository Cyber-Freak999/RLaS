package internal

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

// discardLog silences go-redis's connection-level dial noise, which the
// dead-Redis degraded tests generate on purpose.
type discardLog struct{}

func (discardLog) Printf(context.Context, string, ...interface{}) {}

func TestMain(m *testing.M) {
	redis.SetLogger(discardLog{})
	os.Exit(m.Run())
}
