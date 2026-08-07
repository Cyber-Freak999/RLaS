package internal

import (
	"crypto/sha256"
	"encoding/hex"
)

// Redis key schema shared with the rate-limiter (AGENTS.md "Redis key
// schema"). The control-plane writes these keys; the rate-limiter reads them,
// so the formats here are the contract — never drift them unilaterally.
const (
	APIKeysKey = "api_keys" // hash: sha256(api_key) -> client_id
)

const (
	limitsKeyPrefix = "client_limits:" // {rate, period, burst} as JSON
	gcraKeyPrefix   = "gcra:"          // TAT integer (owned by the Lua script)
)

func limitsKey(id string) string { return limitsKeyPrefix + id }
func gcraKey(id string) string   { return gcraKeyPrefix + id }

// HashAPIKey returns the sha256 hex digest stored in api_keys. Keys are never
// stored in plaintext (constraint 8).
func HashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Limits is the per-client configuration stored at client_limits:{id}. Field
// names and validation mirror the rate-limiter's Limits so a value written
// here is accepted verbatim by /v1/check (rate-limiter limits.go).
type Limits struct {
	Rate   int64  `json:"rate"`
	Period string `json:"period"`
	Burst  int64  `json:"burst"`
}

// Valid enforces constraint 13 at the admin boundary: rate > 0, burst >= 1,
// period a fixed enum value. The GCRA math assumes positive parameters.
func (l Limits) Valid() bool {
	switch l.Period {
	case "second", "minute", "hour":
	default:
		return false
	}
	return l.Rate > 0 && l.Burst >= 1
}
