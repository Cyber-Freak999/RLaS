package internal

import (
	"os"
	"testing"
)

func TestResolveReplicaIDPrefersRenderInstanceID(t *testing.T) {
	t.Setenv("RENDER_INSTANCE_ID", "instance-abc-123")
	if got := resolveReplicaID(); got != "instance-abc-123" {
		t.Fatalf("resolveReplicaID = %q, want instance-abc-123", got)
	}
}

func TestResolveReplicaIDFallsBackToHostname(t *testing.T) {
	t.Setenv("RENDER_INSTANCE_ID", "")
	host, err := os.Hostname()
	if err != nil {
		t.Fatalf("hostname: %v", err)
	}
	if got := resolveReplicaID(); got != host {
		t.Fatalf("resolveReplicaID = %q, want %q", got, host)
	}
}
