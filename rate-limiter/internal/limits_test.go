package internal

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

func TestLimitsPeriodEnum(t *testing.T) {
	cases := []struct {
		period string
		want   int64
	}{
		{"second", 1},
		{"minute", 60},
		{"hour", 3600},
		{"day", 0},
		{"", 0},
	}
	for _, c := range cases {
		if got := (Limits{Rate: 1, Period: c.period, Burst: 1}).PeriodSec(); got != c.want {
			t.Errorf("PeriodSec(%q) = %d, want %d", c.period, got, c.want)
		}
	}
}

func TestLimitsValid(t *testing.T) {
	cases := []struct {
		name string
		l    Limits
		want bool
	}{
		{"ok", Limits{Rate: 10, Period: "minute", Burst: 5}, true},
		{"zero rate", Limits{Rate: 0, Period: "minute", Burst: 5}, false},
		{"zero burst", Limits{Rate: 10, Period: "minute", Burst: 0}, false},
		{"bad period", Limits{Rate: 10, Period: "day", Burst: 5}, false},
		{"negative rate", Limits{Rate: -1, Period: "minute", Burst: 5}, false},
	}
	for _, c := range cases {
		if got := c.l.Valid(); got != c.want {
			t.Errorf("%s: Valid() = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestHashAPIKeyDeterministicAndHashed(t *testing.T) {
	a := HashAPIKey("secret-key")
	b := HashAPIKey("secret-key")
	if a != b {
		t.Fatalf("hash must be deterministic")
	}
	if strings.Contains(a, "secret-key") {
		t.Fatalf("hash must not leak the plaintext key")
	}
	if len(a) != 64 {
		t.Fatalf("expected a sha256 hex digest (64 chars), got %d", len(a))
	}
}
