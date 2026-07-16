package transport

import (
	"context"
	"fmt"
	"net"
	"sync"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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
	entries map[string]RemoteEntry // InstanceID -> entry
	allow   func(addr string) bool // nil = accept any well-formed address
}

// RegistryOption configures a RemoteRegistry at construction.
type RegistryOption func(*RemoteRegistry)

// WithAddrAllow restricts Observe to announcements whose GrpcAddr satisfies allow.
// The NATS announce bus is unauthenticated: without a constraint any participant
// can publish a rogue Announcement and hijack a category, redirecting real turn
// traffic (prompts, recalled memory, replies) to an attacker address. Passing an
// allowlist (known plugin hosts, loopback-only, …) makes Observe drop everything
// else, failing closed.
func WithAddrAllow(allow func(addr string) bool) RegistryOption {
	return func(r *RemoteRegistry) { r.allow = allow }
}

// NewRemoteRegistry returns an empty registry of remote plugin instances.
func NewRemoteRegistry(opts ...RegistryOption) *RemoteRegistry {
	r := &RemoteRegistry{entries: map[string]RemoteEntry{}}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Observe records (or replaces) an announced plugin. Announcements whose GrpcAddr
// is not a usable host:port, or that a configured allowlist rejects, are dropped:
// the address is dialed later with real turn traffic, so an unvalidated one is a
// redirection hazard.
func (r *RemoteRegistry) Observe(a Announcement) {
	if _, _, err := net.SplitHostPort(a.GrpcAddr); err != nil {
		return
	}
	if r.allow != nil && !r.allow(a.GrpcAddr) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[a.InstanceID] = RemoteEntry{Manifest: a.Manifest, GrpcAddr: a.GrpcAddr, InstanceID: a.InstanceID}
}

func (r *RemoteRegistry) byCategory(c contracts.Category) []RemoteEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []RemoteEntry
	for _, e := range r.entries {
		if e.Manifest.Category == c {
			out = append(out, e)
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

// dialConn opens a gRPC connection to the entry. creds selects the transport
// security: nil means plaintext (insecure) — the loopback default — while a
// non-nil credentials.TransportCredentials (from TLSConfig) negotiates (m)TLS.
// Plaintext is refused for any non-loopback address: dialing a remote host in
// cleartext would put prompts/replies/memory on the wire, so this path fails
// closed instead of silently downgrading (configure TLS for remote plugins).
func dialConn(ctx context.Context, e RemoteEntry, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if creds == nil {
		if err := requireLoopback(e.GrpcAddr); err != nil {
			return nil, err
		}
		creds = insecure.NewCredentials()
	}
	return grpc.NewClient(e.GrpcAddr, grpc.WithTransportCredentials(creds))
}

// requireLoopback fails unless addr's host is loopback, guarding the plaintext
// (nil-creds) path from serving a remote address in cleartext.
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
	return fmt.Errorf("transport: refusing plaintext dial to non-loopback address %q; configure TLS for remote plugins", addr)
}

// DialMemory builds a contracts.Memory proxy over the entry's gRPC address.
// creds selects transport security (nil = plaintext loopback).
func DialMemory(ctx context.Context, e RemoteEntry, creds credentials.TransportCredentials) (contracts.Memory, error) {
	conn, err := dialConn(ctx, e, creds)
	if err != nil {
		return nil, err
	}
	p := NewMemoryProxy(pb.NewPluginClient(conn))
	p.conn = conn
	return p, nil
}

// DialOrchestrator builds a contracts.Orchestrator proxy over the entry's gRPC
// address — the request/response sibling of DialMemory (the orchestrator holds
// no per-turn stream state, so the same generic Plugin service carries it).
func DialOrchestrator(ctx context.Context, e RemoteEntry, creds credentials.TransportCredentials) (contracts.Orchestrator, error) {
	conn, err := dialConn(ctx, e, creds)
	if err != nil {
		return nil, err
	}
	p := NewOrchestratorProxy(pb.NewPluginClient(conn))
	p.conn = conn
	return p, nil
}

// DialBackend builds a contracts.Backend proxy over the entry's gRPC address.
// The backend streams turn events, so the proxy keeps the raw connection (for
// NewStream) rather than the generated unary Plugin client.
func DialBackend(ctx context.Context, e RemoteEntry, creds credentials.TransportCredentials) (contracts.Backend, error) {
	conn, err := dialConn(ctx, e, creds)
	if err != nil {
		return nil, err
	}
	p := NewBackendProxy(conn)
	p.conn = conn
	return p, nil
}
