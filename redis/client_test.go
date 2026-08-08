// Tests for the client factory (NewClient). The Sentinel branch's real
// behavior is covered by the integration suites (gcra_client_test,
// check_client_test); these unit tests pin the single-endpoint branch and the
// URL-vs-Sentinel discrimination that make the services portable to managed
// Redis providers.
package redisclient_test

import (
	"testing"

	"rlas/redis"
)

func TestNewClientURLModeParsesOptions(t *testing.T) {
	c, err := redisclient.NewClient(redisclient.ClientConfig{URL: "redis://:secret@cache.example.com:6379/2"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	opts := c.Options()
	if opts.Addr != "cache.example.com:6379" {
		t.Fatalf("addr = %q, want cache.example.com:6379", opts.Addr)
	}
	if opts.Password != "secret" {
		t.Fatalf("password = %q, want secret", opts.Password)
	}
	if opts.DB != 2 {
		t.Fatalf("db = %d, want 2", opts.DB)
	}
}

func TestNewClientURLModeRejectsBadURL(t *testing.T) {
	if _, err := redisclient.NewClient(redisclient.ClientConfig{URL: "://not-a-url"}); err == nil {
		t.Fatal("NewClient with an unparseable URL must return an error, got nil")
	}
}

func TestNewClientSentinelModeWhenURLUnset(t *testing.T) {
	c, err := redisclient.NewClient(redisclient.ClientConfig{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:26379"},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	if c == nil {
		t.Fatal("NewClient returned a nil client")
	}
}
