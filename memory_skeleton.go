package transport

import (
	"context"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

// PortMemory names the memory port on the wire.
const PortMemory = "memory"

type memoryServer struct {
	pb.UnimplementedPluginServer

	real contracts.Memory
}

// RegisterMemorySkeleton wires a real Memory object behind the generic service.
func RegisterMemorySkeleton(s *grpc.Server, real contracts.Memory) {
	routerFor(s).add(PortMemory, &memoryServer{real: real})
}

func fail(err error) (*pb.ResultEnvelope, error) {
	return &pb.ResultEnvelope{Error: err.Error(), JsonPayload: encodeWireError(err)}, nil
}

func (m *memoryServer) Call(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	if env.Port != PortMemory {
		return fail(&UnknownPortError{Port: env.Port})
	}
	switch env.Method {
	case "Recall":
		return m.recall(ctx, env)
	case "Record":
		return m.record(ctx, env)
	case "Search":
		return m.search(ctx, env)
	case "Links":
		return m.links(ctx, env)
	case "Unlink":
		return m.unlink(ctx, env)
	default:
		return fail(&UnknownMethodError{Service: "memory", Method: env.Method})
	}
}

func (m *memoryServer) recall(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	var a struct {
		Key   string
		Depth int
	}
	if err := decodeArgs(env.JsonPayload, &a.Key, &a.Depth); err != nil {
		return fail(err)
	}
	sub, err := m.real.Recall(ctx, a.Key, a.Depth)
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
	var n contracts.Node
	if err := decodeArgs(env.JsonPayload, &n); err != nil {
		return fail(err)
	}
	if err := m.real.Record(ctx, n); err != nil {
		return fail(err)
	}
	return &pb.ResultEnvelope{}, nil
}

func (m *memoryServer) search(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	var q contracts.Query
	if err := decodeArgs(env.JsonPayload, &q); err != nil {
		return fail(err)
	}
	nodes, err := m.real.Search(ctx, q)
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
	var a struct{ From, To, Rel string }
	if err := decodeArgs(env.JsonPayload, &a.From, &a.To, &a.Rel); err != nil {
		return fail(err)
	}
	if err := m.real.Links(ctx, a.From, a.To, a.Rel); err != nil {
		return fail(err)
	}
	return &pb.ResultEnvelope{}, nil
}

func (m *memoryServer) unlink(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	var a struct{ From, To string }
	if err := decodeArgs(env.JsonPayload, &a.From, &a.To); err != nil {
		return fail(err)
	}
	if err := m.real.Unlink(ctx, a.From, a.To); err != nil {
		return fail(err)
	}
	return &pb.ResultEnvelope{}, nil
}
