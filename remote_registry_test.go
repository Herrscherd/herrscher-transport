package transport

import (
	"context"
	"strings"
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
)

func memAnnounce(id, addr string) Announcement {
	return Announcement{
		Manifest:   contracts.Manifest{Category: contracts.CategoryMemory},
		GrpcAddr:   addr,
		InstanceID: id,
	}
}

func TestObserveDropsUnusableAddr(t *testing.T) {
	r := NewRemoteRegistry()
	r.Observe(memAnnounce("a", "")) // no host:port
	r.Observe(memAnnounce("b", "not-a-host-port"))
	if got := r.Memories(); len(got) != 0 {
		t.Fatalf("want unusable addresses dropped, got %+v", got)
	}
}

func TestObserveAllowlistFailsClosed(t *testing.T) {
	r := NewRemoteRegistry(WithAddrAllow(func(addr string) bool {
		return strings.HasPrefix(addr, "127.0.0.1:")
	}))
	r.Observe(memAnnounce("trusted", "127.0.0.1:9000"))
	r.Observe(memAnnounce("rogue", "10.0.0.5:9000")) // off-allowlist attacker address

	got := r.Memories()
	if len(got) != 1 || got[0].InstanceID != "trusted" {
		t.Fatalf("allowlist did not fail closed: %+v", got)
	}
}

func TestObserveWithoutAllowlistAcceptsWellFormed(t *testing.T) {
	r := NewRemoteRegistry()
	r.Observe(memAnnounce("a", "10.0.0.5:9000"))
	if got := r.Memories(); len(got) != 1 {
		t.Fatalf("want well-formed address accepted, got %+v", got)
	}
}

func TestDialConnRefusesPlaintextNonLoopback(t *testing.T) {
	_, err := dialConn(context.Background(), RemoteEntry{GrpcAddr: "10.0.0.5:9000"}, nil)
	if err == nil {
		t.Fatal("want refusal of plaintext dial to non-loopback address")
	}
	if !strings.Contains(err.Error(), "non-loopback") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDialConnAllowsPlaintextLoopback(t *testing.T) {
	// grpc.NewClient is lazy, so a loopback plaintext dial succeeds without a
	// live server; we only assert the guard does not reject it.
	for _, addr := range []string{"127.0.0.1:9000", "localhost:9000", "[::1]:9000"} {
		conn, err := dialConn(context.Background(), RemoteEntry{GrpcAddr: addr}, nil)
		if err != nil {
			t.Fatalf("loopback %q rejected: %v", addr, err)
		}
		_ = conn.Close()
	}
}
