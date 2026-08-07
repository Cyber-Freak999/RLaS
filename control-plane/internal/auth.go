package internal

import (
	"crypto/subtle"
	"net/http"
	"strconv"
)

// withAdminAuth gates the /v1/admin/* routes with a constant-time bearer-token
// comparison (constraint 8: a separate credential from the per-client API keys,
// and never interchangeable with them).
func withAdminAuth(next http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Authorization")
		want := "Bearer " + token
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// writeError emits the canonical JSON error body shared by every admin route.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The message is ours, not caller-controlled; strconv.Quote guarantees
	// well-formed JSON regardless.
	_, _ = w.Write([]byte(`{"error":` + strconv.Quote(msg) + `}`))
}
