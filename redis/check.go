// Checker wraps the single-round-trip check.lua script (auth + limits +
// GCRA + stream push in one atomic EVAL). It lives beside GCRA in the shared
// redisclient package so both services keep a single copy of the script.
package redisclient

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

//go:embed check.lua
var checkLua string

// CheckStatus is the script's reply status. Positive means "consumed/decided
// by GCRA", negative means "could not decide" — the wrapper returns an error
// for nothing here; the rate-limiter maps each status to its HTTP answer.
type CheckStatus int64

const (
	StatusAllowed        CheckStatus = 1
	StatusDenied         CheckStatus = 0
	StatusUnauthorized   CheckStatus = -1
	StatusLimitsNotFound CheckStatus = -2
	StatusLimitsInvalid  CheckStatus = -3
	StatusBadParams      CheckStatus = -4
)

// CheckKeys are the Redis key names passed as KEYS[1..4]. The script builds
// client_limits:{id} and gcra:{id} from the prefixes plus the client_id it
// resolves itself, so callers pass the schema, not concrete per-client keys.
type CheckKeys struct {
	APIKeys      string // hash: sha256(key) -> client_id
	LimitsPrefix string // e.g. "client_limits:"
	GcraPrefix   string // e.g. "gcra:"
	Stream       string // approved-request stream
}

// CheckParams mirror the script's ARGV. APIKeyHash is the SHA-256 digest of
// the presented key — plaintext keys never reach Redis (constraint 8).
type CheckParams struct {
	APIKeyHash   string
	Cost         int64 // >= 1 (boundary-validated, constraint 13)
	StreamMaxLen int64 // XADD MAXLEN target (constraint 10)
}

// CheckReply is the decoded 9-element reply. ClientID/Rate/Period/Burst are
// populated on statuses where the client was resolved; the rate-limiter uses
// them to refresh its fail-open cache on the same round trip. StreamOK is only
// meaningful on StatusAllowed: false means the request was allowed but the
// analytics push failed (the limiter logs stream_xadd_failed and nothing
// else — never a deny or a 500).
type CheckReply struct {
	Status    CheckStatus
	Remaining int64
	ResetAtMS int64
	NowMS     int64
	ClientID  string
	Rate      int64
	Period    string
	Burst     int64
	StreamOK  bool
}

// Checker runs check.lua. Like GCRA, one instance per service is fine — the
// script is loaded and cached by go-redis after the first run.
type Checker struct {
	script *redis.Script
}

// NewChecker returns a Checker wired to the given client.
func NewChecker(c redis.Cmdable) *Checker {
	return &Checker{script: redis.NewScript(checkLua)}
}

// Check runs one atomic EVAL covering auth, limit lookup/validation, the GCRA
// bucket, and the approved-event stream push. It is the only place check.lua's
// reply is decoded, so a contract change touches one file plus the script.
func (ch *Checker) Check(ctx context.Context, c redis.Cmdable, keys CheckKeys, p CheckParams) (CheckReply, error) {
	raw, err := ch.script.Run(ctx, c, []string{keys.APIKeys, keys.LimitsPrefix, keys.GcraPrefix, keys.Stream},
		p.APIKeyHash, p.Cost, p.StreamMaxLen).Result()
	if err != nil {
		return CheckReply{}, err
	}
	items, ok := raw.([]interface{})
	if !ok || len(items) != 9 {
		return CheckReply{}, fmt.Errorf("unexpected check script reply %#v", raw)
	}
	return CheckReply{
		Status:    CheckStatus(items[0].(int64)),
		Remaining: items[1].(int64),
		ResetAtMS: items[2].(int64),
		NowMS:     items[3].(int64),
		ClientID:  items[4].(string),
		Rate:      items[5].(int64),
		Period:    items[6].(string),
		Burst:     items[7].(int64),
		StreamOK:  items[8].(int64) == 1,
	}, nil
}
