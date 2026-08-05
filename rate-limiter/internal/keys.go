package internal

// Redis key schema (see AGENTS.md "Redis key schema").
const (
	APIKeysKey    = "api_keys" // hash: sha256(api_key) -> client_id
	StreamKey     = "stream:approved_requests"
	StreamMaxLen  = 100_000          // XADD MAXLEN ~ 100000 (constraint 10)
	limitsKeyPref = "client_limits:" // {rate, period, burst} as JSON
	gcraKeyPref   = "gcra:"          // TAT integer (owned by the Lua script)
	breakerTripAt = 3                // consecutive check-path failures before trip
)

func limitsKey(id string) string { return limitsKeyPref + id }
func gcraKey(id string) string   { return gcraKeyPref + id }
