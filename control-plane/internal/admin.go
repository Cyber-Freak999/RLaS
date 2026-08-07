package internal

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// Admin is the /v1/admin/* API: client CRUD plus per-client limits. It talks
// only to Redis (constraint 2) — the analytics store is never written from the
// admin path, only the consumer writes TimescaleDB.
type Admin struct {
	client      redis.Cmdable
	store       Store
	redisPinger Pinger
	dbPinger    Pinger
	adminToken  string
	logger      *slog.Logger
}

func NewAdmin(client redis.Cmdable, store Store, redisPinger, dbPinger Pinger, adminToken string, logger *slog.Logger) *Admin {
	return &Admin{
		client:      client,
		store:       store,
		redisPinger: redisPinger,
		dbPinger:    dbPinger,
		adminToken:  adminToken,
		logger:      logger,
	}
}

// Handler mounts every route on one mux (constraint 14: all under /v1).
// /healthz and /metrics are intentionally outside the admin auth wall — the
// health check and scrapers must not need a bearer token.
func (a *Admin) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.healthz)
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/v1/admin/", withAdminAuth(http.HandlerFunc(a.routeAdmin), a.adminToken))
	return mux
}

func (a *Admin) routeAdmin(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == "/v1/admin/clients" && r.Method == http.MethodPost:
		a.createClient(w, r)
	case path == "/v1/admin/clients" && r.Method == http.MethodGet:
		a.listClients(w, r)
	case strings.HasPrefix(path, "/v1/admin/clients/"):
		parts := strings.Split(strings.TrimPrefix(path, "/v1/admin/clients/"), "/")
		id := parts[0]
		if id == "" {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		switch {
		case len(parts) == 1 && r.Method == http.MethodDelete:
			a.deleteClient(w, r, id)
		case len(parts) == 2 && parts[1] == "limits" && r.Method == http.MethodGet:
			a.getLimits(w, r, id)
		case len(parts) == 2 && parts[1] == "limits" && r.Method == http.MethodPut:
			a.putLimits(w, r, id)
		default:
			writeError(w, http.StatusNotFound, "not found")
		}
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

// createClient issues a fresh client ID and API key. The key is shown once and
// stored only as its sha256 hash (constraint 8). No default limits are set —
// a client is not rate-limited until PUT /limits (locked decision).
func (a *Admin) createClient(w http.ResponseWriter, r *http.Request) {
	id, err := randomHex("c-", 16)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot generate client id")
		return
	}
	apiKey, err := randomHex("", 32)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot generate api key")
		return
	}
	if err := a.client.HSet(r.Context(), APIKeysKey, HashAPIKey(apiKey), id).Err(); err != nil {
		a.logger.Error("admin_client_create_failed", "client_id", id, "error", err.Error())
		writeError(w, http.StatusInternalServerError, "redis error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"client_id": id, "api_key": apiKey})
}

// listClients derives the live client set from the api_keys hash values. A
// client "exists" because it owns a key entry, so deletion keeps this list
// accurate.
func (a *Admin) listClients(w http.ResponseWriter, r *http.Request) {
	entries, err := a.client.HGetAll(r.Context(), APIKeysKey).Result()
	if err != nil {
		a.logger.Error("admin_client_list_failed", "error", err.Error())
		writeError(w, http.StatusInternalServerError, "redis error")
		return
	}
	seen := map[string]struct{}{}
	var clients []string
	for _, id := range entries {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			clients = append(clients, id)
		}
	}
	sort.Strings(clients)
	writeJSON(w, http.StatusOK, map[string][]string{"clients": clients})
}

// getLimits returns 404 until a client has limits set (none by default).
func (a *Admin) getLimits(w http.ResponseWriter, r *http.Request, id string) {
	raw, err := a.client.Get(r.Context(), limitsKey(id)).Result()
	if errors.Is(err, redis.Nil) {
		writeError(w, http.StatusNotFound, "limits not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "redis error")
		return
	}
	var l Limits
	if err := json.Unmarshal([]byte(raw), &l); err != nil {
		writeError(w, http.StatusInternalServerError, "corrupt limits")
		return
	}
	writeJSON(w, http.StatusOK, l)
}

// putLimits validates at the boundary (constraint 13) before writing, then
// resets the client's GCRA state — skipping that reset produces mathematically
// undefined wait-time results (constraint 6). The write and the reset happen
// in the same round trip so a crash between them cannot leave stale state.
func (a *Admin) putLimits(w http.ResponseWriter, r *http.Request, id string) {
	var l Limits
	if err := json.NewDecoder(r.Body).Decode(&l); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if !l.Valid() {
		writeError(w, http.StatusBadRequest, "invalid limits: rate>0, burst>=1, period in second|minute|hour")
		return
	}
	raw, err := json.Marshal(l)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "encode error")
		return
	}
	pipe := a.client.TxPipeline()
	pipe.Set(r.Context(), limitsKey(id), raw, 0)
	pipe.Del(r.Context(), gcraKey(id))
	if _, err := pipe.Exec(r.Context()); err != nil {
		a.logger.Error("admin_limits_write_failed", "client_id", id, "error", err.Error())
		writeError(w, http.StatusInternalServerError, "redis error")
		return
	}
	writeJSON(w, http.StatusOK, l)
}

// deleteClient removes every trace of a client: its api_keys hash entries
// (found by HSCAN on value == id, not by an assumed field name), its limits,
// and its GCRA state. Deleting an unknown client is a 404.
func (a *Admin) deleteClient(w http.ResponseWriter, r *http.Request, id string) {
	ctx := r.Context()

	// The only copy of "which keys belong to this client" is the hash, keyed
	// by hashed key value->client id; sweep it rather than trusting a stored
	// plaintext mapping.
	var fields []string
	cursor := uint64(0)
	for {
		entries, next, err := a.client.HScan(ctx, APIKeysKey, cursor, "", 100).Result()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "redis error")
			return
		}
		for i := 0; i+1 < len(entries); i += 2 {
			if entries[i+1] == id {
				fields = append(fields, entries[i])
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if len(fields) == 0 {
		writeError(w, http.StatusNotFound, "client not found")
		return
	}

	pipe := a.client.TxPipeline()
	for _, f := range fields {
		pipe.HDel(ctx, APIKeysKey, f)
	}
	pipe.Del(ctx, limitsKey(id))
	pipe.Del(ctx, gcraKey(id))
	if _, err := pipe.Exec(ctx); err != nil {
		a.logger.Error("admin_client_delete_failed", "client_id", id, "error", err.Error())
		writeError(w, http.StatusInternalServerError, "redis error")
		return
	}
	a.logger.Info("admin_client_deleted", "client_id", id)
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Response is already committed; nothing useful to do but log.
		return
	}
}

func randomHex(prefix string, n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("read random: %w", err)
	}
	return prefix + hex.EncodeToString(b), nil
}
