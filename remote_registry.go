package transport

import (
	"context"
	"fmt"
	"net"
	"sync"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

// RemoteEntry is one announced remote plugin.
type RemoteEntry struct {
	Manifest   contracts.Manifest
	GrpcAddr   string
	InstanceID string
}

// RemoteRegistry accumulates announcements and offers the same query surface
// as contracts.Registry, keyed by category.
type RemoteRegistry struct {
	mu      sync.RWMutex
	entries map[string]RemoteEntry
	allow   func(addr string) bool
}

// RegistryOption configures a RemoteRegistry at construction.
type RegistryOption func(*RemoteRegistry)

// WithAddrAllow sets the address gate of a RemoteRegistry: Watch records only
// announcements whose GrpcAddr satisfies allow, and refuses the others with
// ErrAddrRefused. The gate is required, since a registry without it refuses every
// announcement. Authentication is a separate gate, applied before this one.
func WithAddrAllow(allow func(addr string) bool) RegistryOption {
	return func(r *RemoteRegistry) { r.allow = allow }
}

// NewRemoteRegistry returns an empty registry of remote plugin instances. It
// fails closed: without WithAddrAllow, or with a nil allow function, every
// announcement is refused.
func NewRemoteRegistry(opts ...RegistryOption) *RemoteRegistry {
	registry := &RemoteRegistry{entries: make(map[string]RemoteEntry)}
	for _, o := range opts {
		o(registry)
	}
	return registry
}

// Watch subscribes r to SubjectAnnounce on conn and records the authentic
// announcements that pass the allowlist. Each message is authenticated under key
// before its address is looked at, so an unsigned message never reaches the
// allowlist. A message that is not recorded goes to onRejected exactly once, with
// ErrUnauthenticatedAnnouncement, ErrStaleAnnouncement, ErrOversizedAnnouncement,
// ErrMalformedAnnouncement, ErrUnusableAnnouncement or ErrAddrRefused. onRejected
// runs on the NATS subscription goroutine, so it must return quickly. It returns
// the subscription, which stops delivery when unsubscribed. It fails with
// ErrNilHook, ErrNoAnnounceKey, or the NATS subscribe error.
func (r *RemoteRegistry) Watch(conn *nats.Conn, key AnnounceKey, onRejected func(error)) (*nats.Subscription, error) {
	return subscribeAnnouncements(conn, key, func(announcement Announcement) {
		if err := r.observe(announcement); err != nil {
			onRejected(err)
		}
	}, onRejected)
}

func (r *RemoteRegistry) observe(announcement Announcement) error {
	if announcement.InstanceID == "" || announcement.Manifest.Category == "" {
		return ErrUnusableAnnouncement
	}
	host, port, err := net.SplitHostPort(announcement.GrpcAddr)
	if err != nil || host == "" || port == "" {
		return ErrUnusableAnnouncement
	}
	if r.allow == nil || !r.allow(announcement.GrpcAddr) {
		return ErrAddrRefused
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[announcement.InstanceID] = announcement.entry()
	return nil
}

func (r *RemoteRegistry) byCategory(category contracts.Category) []RemoteEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []RemoteEntry
	for instanceID := range r.entries {
		if r.entries[instanceID].Manifest.Category == category {
			out = append(out, r.entries[instanceID])
		}
	}
	return out
}

// Memories returns the announced memory plugins.
func (r *RemoteRegistry) Memories() []RemoteEntry { return r.byCategory(contracts.CategoryMemory) }

// Orchestrators returns the announced orchestrator plugins.
func (r *RemoteRegistry) Orchestrators() []RemoteEntry {
	return r.byCategory(contracts.CategoryOrchestrator)
}

// Backends returns the announced backend plugins.
func (r *RemoteRegistry) Backends() []RemoteEntry {
	return r.byCategory(contracts.CategoryBackend)
}

func dialConn(ctx context.Context, entry RemoteEntry, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if creds == nil {
		if err := requireLoopback(entry.GrpcAddr); err != nil {
			return nil, err
		}
		creds = insecure.NewCredentials()
	}
	return grpc.NewClient(entry.GrpcAddr, grpc.WithTransportCredentials(creds))
}

func requireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("transport: invalid gRPC address %q: %w", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return &PlaintextDialError{Addr: addr}
}

// DialMemory builds a contracts.Memory proxy over the entry's gRPC address.
// creds selects transport security (nil = plaintext loopback). It returns the
// context error when ctx is already done, *PlaintextDialError when creds is nil
// and the address is not loopback, and a wrapped address error when the address
// has no port.
func DialMemory(ctx context.Context, e RemoteEntry, creds credentials.TransportCredentials) (contracts.Memory, error) {
	conn, err := dialConn(ctx, e, creds)
	if err != nil {
		return nil, err
	}
	proxy := NewMemoryProxy(pb.NewPluginClient(conn))
	proxy.dialed = conn
	return proxy, nil
}

// DialOrchestrator builds a contracts.Orchestrator proxy over the entry's gRPC
// address. It is the request/response sibling of DialMemory: the orchestrator holds
// no per-turn stream state, so the same generic Plugin service carries it. Its
// errors are the same as DialMemory: the context error, *PlaintextDialError, or a
// wrapped address error.
func DialOrchestrator(ctx context.Context, e RemoteEntry, creds credentials.TransportCredentials) (contracts.Orchestrator, error) {
	conn, err := dialConn(ctx, e, creds)
	if err != nil {
		return nil, err
	}
	proxy := NewOrchestratorProxy(pb.NewPluginClient(conn))
	proxy.dialed = conn
	return proxy, nil
}

// DialBackend builds a contracts.Backend proxy over the entry's gRPC address.
// The backend streams turn events, so the proxy keeps the raw connection (for
// NewStream) rather than the generated unary Plugin client. Its errors are the
// same as DialMemory: the context error, *PlaintextDialError, or a wrapped address
// error.
func DialBackend(ctx context.Context, e RemoteEntry, creds credentials.TransportCredentials) (contracts.Backend, error) {
	conn, err := dialConn(ctx, e, creds)
	if err != nil {
		return nil, err
	}
	proxy := NewBackendProxy(conn)
	proxy.dialed = conn
	return proxy, nil
}
