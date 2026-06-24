package transport

import (
	"context"
	"sync"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
	"google.golang.org/grpc"
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
}

func NewRemoteRegistry() *RemoteRegistry {
	return &RemoteRegistry{entries: map[string]RemoteEntry{}}
}

// Observe records (or replaces) an announced plugin.
func (r *RemoteRegistry) Observe(a Announcement) {
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

// DialMemory builds a contracts.Memory proxy over the entry's gRPC address.
func DialMemory(ctx context.Context, e RemoteEntry) (contracts.Memory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(e.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
func DialOrchestrator(ctx context.Context, e RemoteEntry) (contracts.Orchestrator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(e.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p := NewOrchestratorProxy(pb.NewPluginClient(conn))
	p.conn = conn
	return p, nil
}
