package transport

import (
	"context"
	"errors"
	"strings"
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"github.com/nats-io/nats.go"
)

func memAnnounce(id, addr string) Announcement {
	return Announcement{
		Manifest:   contracts.Manifest{Category: contracts.CategoryMemory},
		GrpcAddr:   addr,
		InstanceID: id,
	}
}

func allowAll(string) bool { return true }

func loopbackOnly(addr string) bool { return strings.HasPrefix(addr, "127.0.0.1:") }

func registryEntries(r *RemoteRegistry) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

func TestObserveDropsUnusableAddr(t *testing.T) {
	r := NewRemoteRegistry(WithAddrAllow(allowAll))
	for _, addr := range []string{"", "not-a-host-port"} {
		if err := r.observe(memAnnounce("a", addr)); !errors.Is(err, ErrUnusableAnnouncement) {
			t.Fatalf("address %q: want ErrUnusableAnnouncement, got %v", addr, err)
		}
	}
	if got := r.Memories(); len(got) != 0 {
		t.Fatalf("want unusable addresses dropped, got %+v", got)
	}
}

func TestObserveAllowlistFailsClosed(t *testing.T) {
	r := NewRemoteRegistry(WithAddrAllow(loopbackOnly))
	if err := r.observe(memAnnounce("trusted", "127.0.0.1:9000")); err != nil {
		t.Fatalf("loopback announcement: want recorded, got %v", err)
	}
	if err := r.observe(memAnnounce("rogue", "10.0.0.5:9000")); !errors.Is(err, ErrAddrRefused) {
		t.Fatalf("rogue address: want ErrAddrRefused, got %v", err)
	}
	got := r.Memories()
	if len(got) != 1 || got[0].InstanceID != "trusted" {
		t.Fatalf("allowlist did not fail closed: %+v", got)
	}
}

func TestObserveWithoutAllowlistRefusesEverything(t *testing.T) {
	r := NewRemoteRegistry()
	if err := r.observe(memAnnounce("a", "127.0.0.1:9000")); !errors.Is(err, ErrAddrRefused) {
		t.Fatalf("no allowlist: want ErrAddrRefused, got %v", err)
	}
	if got := r.Memories(); len(got) != 0 {
		t.Fatalf("want no announcement accepted without an allowlist, got %+v", got)
	}
}

func TestObserveNilAllowlistRefusesEverything(t *testing.T) {
	r := NewRemoteRegistry(WithAddrAllow(nil))
	if err := r.observe(memAnnounce("a", "127.0.0.1:9000")); !errors.Is(err, ErrAddrRefused) {
		t.Fatalf("nil allowlist: want ErrAddrRefused, got %v", err)
	}
	if got := r.Memories(); len(got) != 0 {
		t.Fatalf("want a nil allowlist to refuse every announcement, got %+v", got)
	}
}

func TestDialConnRefusesPlaintextNonLoopback(t *testing.T) {
	_, err := dialConn(context.Background(), RemoteEntry{GrpcAddr: "10.0.0.5:9000"}, nil)
	if err == nil {
		t.Fatal("want refusal of plaintext dial to non-loopback address")
	}
	if _, ok := errors.AsType[*PlaintextDialError](err); !ok {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDialConnAllowsPlaintextLoopback(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:9000", "localhost:9000", "[::1]:9000"} {
		conn, err := dialConn(context.Background(), RemoteEntry{GrpcAddr: addr}, nil)
		if err != nil {
			t.Fatalf("loopback %q rejected: %v", addr, err)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestObserveDropsUnqueryableAnnouncements(t *testing.T) {
	cases := []struct {
		name string
		ann  Announcement
	}{
		{name: "empty instance id", ann: Announcement{
			Manifest: contracts.Manifest{Category: contracts.CategoryMemory}, GrpcAddr: "127.0.0.1:9001",
		}},
		{name: "empty category", ann: Announcement{
			InstanceID: "a", GrpcAddr: "127.0.0.1:9001",
		}},
		{name: "port only address", ann: memAnnounce("a", ":50051")},
		{name: "host only address", ann: memAnnounce("a", "127.0.0.1:")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRemoteRegistry(WithAddrAllow(allowAll))
			if err := r.observe(tc.ann); !errors.Is(err, ErrUnusableAnnouncement) {
				t.Fatalf("want ErrUnusableAnnouncement, got %v", err)
			}
			if n := registryEntries(r); n != 0 {
				t.Fatalf("want announcement dropped, registry holds %d entries", n)
			}
		})
	}
}

func TestObserveKeepsDistinctInstancesWithoutCollapsing(t *testing.T) {
	r := NewRemoteRegistry(WithAddrAllow(allowAll))
	for _, addr := range []string{"127.0.0.1:9001", "127.0.0.1:9002"} {
		if err := r.observe(memAnnounce("", addr)); !errors.Is(err, ErrUnusableAnnouncement) {
			t.Fatalf("identity-less announcement: want ErrUnusableAnnouncement, got %v", err)
		}
	}
	if got := r.Memories(); len(got) != 0 {
		t.Fatalf("want identity-less announcements dropped, got %+v", got)
	}
	if err := r.observe(memAnnounce("a", "127.0.0.1:9001")); err != nil {
		t.Fatal(err)
	}
	if err := r.observe(memAnnounce("b", "127.0.0.1:9002")); err != nil {
		t.Fatal(err)
	}
	if got := r.Memories(); len(got) != 2 {
		t.Fatalf("want both identified plugins visible, got %+v", got)
	}
}

func watchRegistry(t *testing.T, nc *nats.Conn, r *RemoteRegistry, key AnnounceKey) <-chan error {
	t.Helper()
	errs := make(chan error, 16)
	if _, err := r.Watch(nc, key, func(err error) { errs <- err }); err != nil {
		t.Fatalf("watch: %v", err)
	}
	return errs
}

func settleAfterMarker(t *testing.T, nc *nats.Conn, rejected <-chan error) []error {
	t.Helper()
	if err := nc.Publish(SubjectAnnounce, []byte("{")); err != nil {
		t.Fatalf("publish marker: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), announceWait)
	defer cancel()
	var before []error
	for {
		select {
		case err := <-rejected:
			if errors.Is(err, ErrMalformedAnnouncement) {
				return before
			}
			before = append(before, err)
		case <-ctx.Done():
			t.Fatalf("subscription did not settle within %s: %v", announceWait, ctx.Err())
		}
	}
}

func TestWatchRecordsAuthenticAnnouncement(t *testing.T) {
	nc := runNATS(t)
	r := NewRemoteRegistry(WithAddrAllow(loopbackOnly))
	rejected := watchRegistry(t, nc, r, testKey(t))
	if err := Announce(nc, memAnnounce("abc", "127.0.0.1:9000"), testKey(t)); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if errs := settleAfterMarker(t, nc, rejected); len(errs) != 0 {
		t.Fatalf("authentic announcement rejected: %v", errs)
	}
	got := r.Memories()
	if len(got) != 1 || got[0].InstanceID != "abc" {
		t.Fatalf("want the authentic announcement recorded, got %+v", got)
	}
}

func TestWatchRefusesAddrOutsideAllowlist(t *testing.T) {
	nc := runNATS(t)
	r := NewRemoteRegistry(WithAddrAllow(loopbackOnly))
	rejected := watchRegistry(t, nc, r, testKey(t))
	if err := Announce(nc, memAnnounce("rogue", "10.0.0.5:9000"), testKey(t)); err != nil {
		t.Fatalf("announce: %v", err)
	}
	errs := settleAfterMarker(t, nc, rejected)
	if len(errs) != 1 || !errors.Is(errs[0], ErrAddrRefused) {
		t.Fatalf("want one ErrAddrRefused, got %v", errs)
	}
	if got := r.Memories(); len(got) != 0 {
		t.Fatalf("rogue address recorded: %+v", got)
	}
}

func TestWatchRefusesForeignKey(t *testing.T) {
	nc := runNATS(t)
	r := NewRemoteRegistry(WithAddrAllow(allowAll))
	rejected := watchRegistry(t, nc, r, testKey(t))
	if err := Announce(nc, memAnnounce("forged", "127.0.0.1:9000"), otherKey(t)); err != nil {
		t.Fatalf("announce: %v", err)
	}
	errs := settleAfterMarker(t, nc, rejected)
	if len(errs) != 1 || !errors.Is(errs[0], ErrUnauthenticatedAnnouncement) {
		t.Fatalf("want one ErrUnauthenticatedAnnouncement, got %v", errs)
	}
	if got := r.Memories(); len(got) != 0 {
		t.Fatalf("forged announcement recorded: %+v", got)
	}
}

func TestWatchAuthenticatesBeforeAllowlist(t *testing.T) {
	nc := runNATS(t)
	calls := 0
	r := NewRemoteRegistry(WithAddrAllow(func(string) bool {
		calls++
		return true
	}))
	rejected := watchRegistry(t, nc, r, testKey(t))
	if err := nc.Publish(SubjectAnnounce, unsignedEnvelope(t, memAnnounce("unsigned", "127.0.0.1:9000"))); err != nil {
		t.Fatalf("publish: %v", err)
	}
	errs := settleAfterMarker(t, nc, rejected)
	if len(errs) != 1 || !errors.Is(errs[0], ErrUnauthenticatedAnnouncement) {
		t.Fatalf("want one ErrUnauthenticatedAnnouncement, got %v", errs)
	}
	if calls != 0 {
		t.Fatalf("allowlist consulted %d times for an unauthenticated message", calls)
	}
}

func TestWatchRefusesNilHookAndZeroKey(t *testing.T) {
	nc := runNATS(t)
	r := NewRemoteRegistry(WithAddrAllow(allowAll))
	if _, err := r.Watch(nc, testKey(t), nil); !errors.Is(err, ErrNilHook) {
		t.Fatalf("nil hook: want ErrNilHook, got %v", err)
	}
	if _, err := r.Watch(nc, AnnounceKey{}, func(error) {}); !errors.Is(err, ErrNoAnnounceKey) {
		t.Fatalf("zero key: want ErrNoAnnounceKey, got %v", err)
	}
}

func TestWatchUnsubscribeStopsDelivery(t *testing.T) {
	nc := runNATS(t)
	r := NewRemoteRegistry(WithAddrAllow(allowAll))
	sub, err := r.Watch(nc, testKey(t), func(error) {})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	if sub.IsValid() {
		t.Fatal("subscription still valid after unsubscribe")
	}
	if err := Announce(nc, memAnnounce("late", "127.0.0.1:9000"), testKey(t)); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := r.Memories(); len(got) != 0 {
		t.Fatalf("announcement delivered after unsubscribe: %+v", got)
	}
}
