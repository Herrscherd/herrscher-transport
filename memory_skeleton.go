package transport

import (
	"context"
	"fmt"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
	"google.golang.org/grpc"
)

const PortMemory = "memory"

type memoryServer struct {
	pb.UnimplementedPluginServer
	real contracts.Memory
}

// RegisterMemorySkeleton wires a real Memory object behind the generic service.
func RegisterMemorySkeleton(s *grpc.Server, real contracts.Memory) {
	pb.RegisterPluginServer(s, &memoryServer{real: real})
}

func fail(err error) (*pb.ResultEnvelope, error) {
	return &pb.ResultEnvelope{Error: err.Error()}, nil
}

func (m *memoryServer) Call(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	if env.Port != PortMemory {
		return fail(fmt.Errorf("transport: unknown port %q", env.Port))
	}
	switch env.Method {
	case "Recall":
		var a struct {
			Key   string
			Depth int
		}
		var tuple []any = []any{&a.Key, &a.Depth}
		if err := Unmarshal(env.JsonPayload, &tuple); err != nil {
			return fail(err)
		}
		sub, err := m.real.Recall(ctx, a.Key, a.Depth)
		if err != nil {
			return fail(err)
		}
		out, err := Marshal([]any{sub})
		if err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{JsonPayload: out}, nil
	case "Record":
		var n contracts.Node
		tuple := []any{&n}
		if err := Unmarshal(env.JsonPayload, &tuple); err != nil {
			return fail(err)
		}
		if err := m.real.Record(ctx, n); err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{}, nil
	case "Search":
		var q contracts.Query
		tuple := []any{&q}
		if err := Unmarshal(env.JsonPayload, &tuple); err != nil {
			return fail(err)
		}
		nodes, err := m.real.Search(ctx, q)
		if err != nil {
			return fail(err)
		}
		out, err := Marshal([]any{nodes})
		if err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{JsonPayload: out}, nil
	case "Links":
		var a struct{ From, To, Rel string }
		tuple := []any{&a.From, &a.To, &a.Rel}
		if err := Unmarshal(env.JsonPayload, &tuple); err != nil {
			return fail(err)
		}
		if err := m.real.Links(ctx, a.From, a.To, a.Rel); err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{}, nil
	case "Close":
		if err := m.real.Close(); err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{}, nil
	default:
		return fail(fmt.Errorf("transport: unknown method memory.%s", env.Method))
	}
}
