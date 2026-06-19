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

// DialMemory builds a contracts.Memory proxy over the entry's gRPC address.
func DialMemory(ctx context.Context, e RemoteEntry) (contracts.Memory, error) {
	conn, err := grpc.NewClient(e.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	_ = ctx
	return NewMemoryProxy(pb.NewPluginClient(conn)), nil
}
