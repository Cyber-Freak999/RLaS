package internal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/redis/go-redis/v9"
)

// HashAPIKey produces the field key used in the api_keys hash. Keys are never
// stored in plaintext (constraint 8); the digest is the lookup key.
func HashAPIKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}

// Authenticate resolves an API key to its client ID via the api_keys hash.
// A successful HGET — even one that finds nothing — proves Redis is reachable,
// so the caller can use the outcome as a breaker success signal.
func Authenticate(ctx context.Context, c redis.Cmdable, apiKey string) (string, error) {
	id, err := c.HGet(ctx, APIKeysKey, HashAPIKey(apiKey)).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrUnauthorized
	}
	if err != nil {
		return "", err
	}
	return id, nil
}
