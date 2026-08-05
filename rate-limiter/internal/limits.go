package internal

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/redis/go-redis/v9"
)

// Sentinel errors returned by the check path. The handler maps them to HTTP
// statuses; the fallback path maps ErrUnauthorized/ErrLimits* to its own
// degraded responses.
var (
	ErrUnauthorized   = errors.New("unauthorized")
	ErrLimitsNotFound = errors.New("limits not found")
	ErrLimitsInvalid  = errors.New("limits invalid")
)

// Limits is the per-client configuration stored by the control-plane as JSON
// at client_limits:{client_id}. The admin boundary (constraint 13) guarantees
// the values before they reach Redis; the rate-limiter re-validates
// defensively because a bad stored value must not reach the Lua script.
type Limits struct {
	Rate   int64  `json:"rate"`
	Period string `json:"period"`
	Burst  int64  `json:"burst"`
}

// PeriodSec maps the fixed period enum to seconds. Returns 0 for anything
// outside second|minute|hour so Valid() rejects it.
func (l Limits) PeriodSec() int64 {
	switch l.Period {
	case "second":
		return 1
	case "minute":
		return 60
	case "hour":
		return 3600
	default:
		return 0
	}
}

// Valid mirrors constraint 13's checks: rate > 0, burst >= 1, period an enum
// value. A zero burst would deny every request, so it is rejected, not
// special-cased.
func (l Limits) Valid() bool {
	return l.Rate > 0 && l.PeriodSec() > 0 && l.Burst >= 1
}

// LoadLimits reads and parses client_limits:{client_id}.
func LoadLimits(ctx context.Context, c redis.Cmdable, clientID string) (Limits, error) {
	raw, err := c.Get(ctx, limitsKey(clientID)).Result()
	if errors.Is(err, redis.Nil) {
		return Limits{}, ErrLimitsNotFound
	}
	if err != nil {
		return Limits{}, err
	}
	var l Limits
	if err := json.Unmarshal([]byte(raw), &l); err != nil {
		return Limits{}, ErrLimitsInvalid
	}
	if !l.Valid() {
		return Limits{}, ErrLimitsInvalid
	}
	return l, nil
}
