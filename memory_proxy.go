package transport

import (
	"context"
	"errors"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
)

// MemoryProxy is a contracts.Memory backed by a remote plugin over gRPC.
type MemoryProxy struct {
	client pb.PluginClient
}

// NewMemoryProxy builds a proxy over an established Plugin client.
func NewMemoryProxy(c pb.PluginClient) *MemoryProxy { return &MemoryProxy{client: c} }

func (p *MemoryProxy) call(ctx context.Context, method string, args ...any) (*pb.ResultEnvelope, error) {
	payload, err := Marshal(args)
	if err != nil {
		return nil, err
	}
	res, err := p.client.Call(ctx, &pb.MethodEnvelope{Port: PortMemory, Method: method, JsonPayload: payload})
	if err != nil {
		return nil, err // transport-level failure (peer down) — clear, typed
	}
	if res.Error != "" {
		return nil, errors.New(res.Error)
	}
	return res, nil
}

func (p *MemoryProxy) Recall(ctx context.Context, key string, depth int) (contracts.Subgraph, error) {
	res, err := p.call(ctx, "Recall", key, depth)
	if err != nil {
		return contracts.Subgraph{}, err
	}
	var out contracts.Subgraph
	tuple := []any{&out}
	if err := Unmarshal(res.JsonPayload, &tuple); err != nil {
		return contracts.Subgraph{}, err
	}
	return out, nil
}

func (p *MemoryProxy) Record(ctx context.Context, n contracts.Node) error {
	_, err := p.call(ctx, "Record", n)
	return err
}

func (p *MemoryProxy) Search(ctx context.Context, q contracts.Query) ([]contracts.Node, error) {
	res, err := p.call(ctx, "Search", q)
	if err != nil {
		return nil, err
	}
	var out []contracts.Node
	tuple := []any{&out}
	if err := Unmarshal(res.JsonPayload, &tuple); err != nil {
		return nil, err
	}
	return out, nil
}

func (p *MemoryProxy) Links(ctx context.Context, from, to, rel string) error {
	_, err := p.call(ctx, "Links", from, to, rel)
	return err
}

func (p *MemoryProxy) Close() error {
	_, err := p.call(context.Background(), "Close")
	return err
}
