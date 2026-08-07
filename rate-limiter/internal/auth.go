package internal

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashAPIKey produces the field key used in the api_keys hash. Keys are never
// stored in plaintext (constraint 8); the digest is the lookup field, and it
// is what the rate-limiter passes into check.lua (auth happens inside the
// script, in the same atomic round trip as the bucket check).
func HashAPIKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}
