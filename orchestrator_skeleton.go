package transport

import (
	"context"
	"fmt"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
	"google.golang.org/grpc"
)

const PortOrchestrator = "orchestrator"

type orchestratorServer struct {
	pb.UnimplementedPluginServer
	real contracts.Orchestrator
}

// RegisterOrchestratorSkeleton wires a real Orchestrator behind the generic
// Plugin service so a remote host can serve the conversation-policy port. The
// surface is request/response (Context/Observe/Consolidate); the orchestrator
// holds no streaming turn state, unlike the backend.
func RegisterOrchestratorSkeleton(s *grpc.Server, real contracts.Orchestrator) {
	pb.RegisterPluginServer(s, &orchestratorServer{real: real})
}

func (o *orchestratorServer) Call(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	if env.Port != PortOrchestrator {
		return fail(fmt.Errorf("transport: unknown port %q", env.Port))
	}
	switch env.Method {
	case "Context":
		out, err := encodeArgs(o.real.Context(ctx))
		if err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{JsonPayload: out}, nil
	case "Observe":
		var a struct {
			Prompt contracts.Prompt
			Reply  string
		}
		if err := decodeArgs(env.JsonPayload, &a.Prompt, &a.Reply); err != nil {
			return fail(err)
		}
		if err := o.real.Observe(ctx, a.Prompt, a.Reply); err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{}, nil
	case "Consolidate":
		if err := o.real.Consolidate(ctx); err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{}, nil
	default:
		return fail(fmt.Errorf("transport: unknown method orchestrator.%s", env.Method))
	}
}
