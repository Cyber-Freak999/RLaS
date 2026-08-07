package internal

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdminAuthRejects(t *testing.T) {
	h := withAdminAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), "sekret")

	cases := map[string]string{
		"missing":    "",
		"wrong_scheme": "Basic c2VrcmV0Og==",
		"wrong_token": "Bearer nope",
	}
	for name, hdr := range cases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/admin/clients", nil)
			if hdr != "" {
				req.Header.Set("Authorization", hdr)
			}
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rr.Code)
			}
		})
	}
}

func TestAdminAuthAllows(t *testing.T) {
	called := false
	h := withAdminAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}), "sekret")

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/clients", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if !called {
		t.Fatal("handler was not invoked for a valid bearer token")
	}
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rr.Code)
	}
}
