package transport

import (
	"context"
	"io"

	contracts "github.com/Herrscherd/herrscher-contracts"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

// MemoryProxy is a contracts.Memory backed by a remote plugin over gRPC.
type MemoryProxy struct {
	client pb.PluginClient
	dialed io.Closer
}

var _ contracts.Memory = (*MemoryProxy)(nil)

// NewMemoryProxy builds a proxy over an established Plugin client.
func NewMemoryProxy(c pb.PluginClient) *MemoryProxy { return &MemoryProxy{client: c} }

func (p *MemoryProxy) call(ctx context.Context, method string, args ...any) (*pb.ResultEnvelope, error) {
	payload, err := Marshal(args)
	if err != nil {
		return nil, err
	}
	res, err := p.client.Call(ctx, &pb.MethodEnvelope{Port: PortMemory, Method: method, JsonPayload: payload})
	if err != nil {
		return nil, err
	}
	if res.Error != "" {
		return nil, decodeWireError(res.JsonPayload, res.Error)
	}
	return res, nil
}

// Recall returns the subgraph around key, up to depth links away, as the remote memory computes it.
func (p *MemoryProxy) Recall(ctx context.Context, key string, depth int) (contracts.Subgraph, error) {
	res, err := p.call(ctx, "Recall", key, depth)
	if err != nil {
		return contracts.Subgraph{}, err
	}
	var out contracts.Subgraph
	if err := decodeArgs(res.JsonPayload, &out); err != nil {
		return contracts.Subgraph{}, err
	}
	return out, nil
}

// Record stores n in the remote memory.
func (p *MemoryProxy) Record(ctx context.Context, n contracts.Node) error {
	_, err := p.call(ctx, "Record", n)
	return err
}

// Search returns the nodes of the remote memory that match q.
func (p *MemoryProxy) Search(ctx context.Context, q contracts.Query) ([]contracts.Node, error) {
	res, err := p.call(ctx, "Search", q)
	if err != nil {
		return nil, err
	}
	var out []contracts.Node
	if err := decodeArgs(res.JsonPayload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Links records the relation rel from the node from to the node to in the remote memory.
func (p *MemoryProxy) Links(ctx context.Context, from, to, rel string) error {
	_, err := p.call(ctx, "Links", from, to, rel)
	return err
}

// Unlink removes the relation from the node from to the node to in the remote memory.
func (p *MemoryProxy) Unlink(ctx context.Context, from, to string) error {
	_, err := p.call(ctx, "Unlink", from, to)
	return err
}

// Close releases the client-side gRPC connection only. It does not close the
// remote Memory: the plugin-host owns that object and may serve other clients,
// so one bridge shutting down must not tear the shared memory down.
func (p *MemoryProxy) Close() error {
	if p.dialed != nil {
		return p.dialed.Close()
	}
	return nil
}
