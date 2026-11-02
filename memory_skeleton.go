package transport

import (
	"context"

	contracts "github.com/Herrscherd/herrscher-contracts"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

// PortMemory names the memory port on the wire.
const PortMemory = "memory"

const (
	methodRecall = "Recall"
	methodRecord = "Record"
	methodSearch = "Search"
	methodLinks  = "Links"
	methodUnlink = "Unlink"
)

type memoryServer struct {
	pb.UnimplementedPluginServer

	real contracts.Memory
}

// RegisterMemorySkeleton wires a real Memory object behind the generic service.
func RegisterMemorySkeleton(r *PluginRouter, impl contracts.Memory) {
	r.add(PortMemory, &memoryServer{real: impl})
}

func fail(err error) (*pb.ResultEnvelope, error) {
	return &pb.ResultEnvelope{Error: err.Error(), JsonPayload: encodeWireError(err)}, nil
}

func (m *memoryServer) Call(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	if env.Port != PortMemory {
		return fail(&UnknownPortError{Port: env.Port})
	}
	switch env.Method {
	case methodRecall:
		return m.recall(ctx, env)
	case methodRecord:
		return m.record(ctx, env)
	case methodSearch:
		return m.search(ctx, env)
	case methodLinks:
		return m.links(ctx, env)
	case methodUnlink:
		return m.unlink(ctx, env)
	default:
		return fail(&UnknownMethodError{Service: PortMemory, Method: env.Method})
	}
}

func (m *memoryServer) recall(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	var args struct {
		Key   string
		Depth int
	}
	if err := decodeArgs(env.JsonPayload, &args.Key, &args.Depth); err != nil {
		return fail(err)
	}
	sub, err := m.real.Recall(ctx, args.Key, args.Depth)
	if err != nil {
		return fail(err)
	}
	out, err := encodeArgs(sub)
	if err != nil {
		return fail(err)
	}
	return &pb.ResultEnvelope{JsonPayload: out}, nil
}

func (m *memoryServer) record(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	var node contracts.Node
	if err := decodeArgs(env.JsonPayload, &node); err != nil {
		return fail(err)
	}
	if err := m.real.Record(ctx, node); err != nil {
		return fail(err)
	}
	return &pb.ResultEnvelope{}, nil
}

func (m *memoryServer) search(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	var query contracts.Query
	if err := decodeArgs(env.JsonPayload, &query); err != nil {
		return fail(err)
	}
	nodes, err := m.real.Search(ctx, query)
	if err != nil {
		return fail(err)
	}
	out, err := encodeArgs(nodes)
	if err != nil {
		return fail(err)
	}
	return &pb.ResultEnvelope{JsonPayload: out}, nil
}

func (m *memoryServer) links(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	var args struct{ From, To, Rel string }
	if err := decodeArgs(env.JsonPayload, &args.From, &args.To, &args.Rel); err != nil {
		return fail(err)
	}
	if err := m.real.Links(ctx, args.From, args.To, args.Rel); err != nil {
		return fail(err)
	}
	return &pb.ResultEnvelope{}, nil
}

func (m *memoryServer) unlink(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	var args struct{ From, To string }
	if err := decodeArgs(env.JsonPayload, &args.From, &args.To); err != nil {
		return fail(err)
	}
	if err := m.real.Unlink(ctx, args.From, args.To); err != nil {
		return fail(err)
	}
	return &pb.ResultEnvelope{}, nil
}
