package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"rlas/redis"
)

// testRedis connects like the compose control-plane does; REDIS_ADDR switches
// to a plain single-node client so CI can run these against a redis service
// container. Tests skip when Redis is unreachable.
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	var c *redis.Client
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		c = redis.NewClient(&redis.Options{Addr: addr})
	} else {
		c = redisclient.NewFailoverClient(redisclient.FailoverConfig{
			MasterName:    envOr("REDIS_MASTER_NAME", "mymaster"),
			SentinelAddrs: splitCSV(envOr("REDIS_SENTINEL_ADDRS", "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381")),
			Password:      os.Getenv("REDIS_PASSWORD"),
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		t.Skipf("redis not reachable, skipping: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newAdmin(t *testing.T, c *redis.Client) *Admin {
	t.Helper()
	return NewAdmin(c, &nopStore{}, healthyPinger(), healthyPinger(), "sekret", testLogger())
}

func doAdminReq(t *testing.T, a *Admin, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	var rd *strings.Reader
	if body == "" {
		rd = strings.NewReader("")
	} else {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rr := httptest.NewRecorder()
	a.Handler().ServeHTTP(rr, req)
	return rr
}

func TestAdminCreateAndList(t *testing.T) {
	c := testRedis(t)
	a := newAdmin(t, c)

	rr := doAdminReq(t, a, http.MethodPost, "/v1/admin/clients", "", "sekret")
	if rr.Code != http.StatusOK {
		t.Fatalf("create status = %d, body %s", rr.Code, rr.Body.String())
	}
	var created struct {
		ClientID string `json:"client_id"`
		APIKey   string `json:"api_key"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.ClientID == "" || created.APIKey == "" {
		t.Fatalf("create response missing fields: %+v", created)
	}

	// The key must be stored hashed, never plaintext (constraint 8).
	ctx := context.Background()
	got, err := c.HGet(ctx, APIKeysKey, HashAPIKey(created.APIKey)).Result()
	if err != nil || got != created.ClientID {
		t.Fatalf("api_keys lookup = %q, %v; want %q", got, err, created.ClientID)
	}
	if n, err := c.HExists(ctx, APIKeysKey, created.APIKey).Result(); err != nil || n {
		t.Fatalf("plaintext key present in api_keys (exists=%v, err=%v)", n, err)
	}

	rr = doAdminReq(t, a, http.MethodGet, "/v1/admin/clients", "", "sekret")
	if rr.Code != http.StatusOK {
		t.Fatalf("list status = %d, body %s", rr.Code, rr.Body.String())
	}
	var listed struct {
		Clients []string `json:"clients"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	found := false
	for _, id := range listed.Clients {
		if id == created.ClientID {
			found = true
		}
	}
	if !found {
		t.Fatalf("client %s missing from list %v", created.ClientID, listed.Clients)
	}
}

func TestAdminLimitsLifecycle(t *testing.T) {
	c := testRedis(t)
	a := newAdmin(t, c)
	ctx := context.Background()

	rr := doAdminReq(t, a, http.MethodPost, "/v1/admin/clients", "", "sekret")
	var created struct {
		ClientID string `json:"client_id"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &created)
	id := created.ClientID

	// No limits yet -> 404.
	rr = doAdminReq(t, a, http.MethodGet, "/v1/admin/clients/"+id+"/limits", "", "sekret")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("get limits before set = %d, want 404", rr.Code)
	}

	// Seed stale GCRA state; a limit change must reset it (constraint 6).
	if err := c.Set(ctx, gcraKey(id), 999999999999, 0).Err(); err != nil {
		t.Fatalf("seed gcra: %v", err)
	}

	rr = doAdminReq(t, a, http.MethodPut, "/v1/admin/clients/"+id+"/limits",
		`{"rate":5,"period":"second","burst":10}`, "sekret")
	if rr.Code != http.StatusOK {
		t.Fatalf("put limits = %d, body %s", rr.Code, rr.Body.String())
	}

	// GCRA state was reset.
	if n, err := c.Exists(ctx, gcraKey(id)).Result(); err != nil || n != 0 {
		t.Fatalf("gcra key still present after limit change (exists=%d, err=%v)", n, err)
	}

	rr = doAdminReq(t, a, http.MethodGet, "/v1/admin/clients/"+id+"/limits", "", "sekret")
	if rr.Code != http.StatusOK {
		t.Fatalf("get limits = %d, body %s", rr.Code, rr.Body.String())
	}
	var lim Limits
	if err := json.Unmarshal(rr.Body.Bytes(), &lim); err != nil {
		t.Fatalf("decode limits: %v", err)
	}
	if lim.Rate != 5 || lim.Period != "second" || lim.Burst != 10 {
		t.Fatalf("limits = %+v, want rate=5 period=second burst=10", lim)
	}
}

func TestAdminPutLimitsValidation(t *testing.T) {
	c := testRedis(t)
	a := newAdmin(t, c)

	rr := doAdminReq(t, a, http.MethodPost, "/v1/admin/clients", "", "sekret")
	var created struct {
		ClientID string `json:"client_id"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &created)
	id := created.ClientID

	cases := map[string]string{
		"rate_zero":     `{"rate":0,"period":"second","burst":1}`,
		"burst_zero":    `{"rate":1,"period":"second","burst":0}`,
		"bad_period":    `{"rate":1,"period":"day","burst":1}`,
		"missing_field": `{"rate":1,"period":"second"}`,
		"not_json":      `not json`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			rr := doAdminReq(t, a, http.MethodPut, "/v1/admin/clients/"+id+"/limits", body, "sekret")
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("put %s = %d, want 400 (body %s)", body, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestAdminDelete(t *testing.T) {
	c := testRedis(t)
	a := newAdmin(t, c)
	ctx := context.Background()

	rr := doAdminReq(t, a, http.MethodPost, "/v1/admin/clients", "", "sekret")
	var created struct {
		ClientID string `json:"client_id"`
		APIKey   string `json:"api_key"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &created)

	if err := c.Set(ctx, limitsKey(created.ClientID), `{"rate":1,"period":"second","burst":1}`, 0).Err(); err != nil {
		t.Fatalf("seed limits: %v", err)
	}
	if err := c.Set(ctx, gcraKey(created.ClientID), 42, 0).Err(); err != nil {
		t.Fatalf("seed gcra: %v", err)
	}

	rr = doAdminReq(t, a, http.MethodDelete, "/v1/admin/clients/"+created.ClientID, "", "sekret")
	if rr.Code != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204", rr.Code)
	}

	if n, err := c.Exists(ctx, limitsKey(created.ClientID)).Result(); err != nil || n != 0 {
		t.Fatalf("limits key remains after delete (exists=%d, err=%v)", n, err)
	}
	if n, err := c.Exists(ctx, gcraKey(created.ClientID)).Result(); err != nil || n != 0 {
		t.Fatalf("gcra key remains after delete (exists=%d, err=%v)", n, err)
	}
	if n, err := c.HExists(ctx, APIKeysKey, HashAPIKey(created.APIKey)).Result(); err != nil || n {
		t.Fatalf("api_keys entry remains after delete (exists=%v, err=%v)", n, err)
	}

	rr = doAdminReq(t, a, http.MethodDelete, "/v1/admin/clients/"+created.ClientID, "", "sekret")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("second delete = %d, want 404", rr.Code)
	}
}

func TestAdminUnauthorized(t *testing.T) {
	c := testRedis(t)
	a := newAdmin(t, c)

	rr := doAdminReq(t, a, http.MethodGet, "/v1/admin/clients", "", "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token status = %d, want 401", rr.Code)
	}
	rr = doAdminReq(t, a, http.MethodGet, "/v1/admin/clients", "", "wrong")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d, want 401", rr.Code)
	}
}

func TestHealthzReflectsDependencies(t *testing.T) {
	c := testRedis(t)
	a := newAdmin(t, c)

	rr := doAdminReq(t, a, http.MethodGet, "/healthz", "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200 (both deps up)", rr.Code)
	}
}
