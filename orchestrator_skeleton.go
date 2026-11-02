package transport

import (
	"context"

	contracts "github.com/Herrscherd/herrscher-contracts"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

// PortOrchestrator names the orchestrator port on the wire.
const PortOrchestrator = "orchestrator"

type orchestratorServer struct {
	pb.UnimplementedPluginServer

	real contracts.Orchestrator
}

// RegisterOrchestratorSkeleton wires a real Orchestrator behind the generic
// Plugin service so a remote host can serve the conversation-policy port. The
// surface is request/response (Context/Observe/Consolidate); the orchestrator
// holds no streaming turn state, unlike the backend.
func RegisterOrchestratorSkeleton(r *PluginRouter, impl contracts.Orchestrator) {
	r.add(PortOrchestrator, &orchestratorServer{real: impl})
}

// Call runs the orchestrator method named by env against the wrapped Orchestrator and returns its encoded result, or the encoded error.
func (o *orchestratorServer) Call(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	if env.Port != PortOrchestrator {
		return fail(&UnknownPortError{Port: env.Port})
	}
	switch env.Method {
	case "Context":
		out, err := encodeArgs(o.real.Context(ctx))
		if err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{JsonPayload: out}, nil
	case "Observe":
		var args struct {
			Prompt contracts.Prompt
			Reply  string
		}
		if err := decodeArgs(env.JsonPayload, &args.Prompt, &args.Reply); err != nil {
			return fail(err)
		}
		if err := o.real.Observe(ctx, args.Prompt, args.Reply); err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{}, nil
	case "Consolidate":
		if err := o.real.Consolidate(ctx); err != nil {
			return fail(err)
		}
		return &pb.ResultEnvelope{}, nil
	default:
		return fail(&UnknownMethodError{Service: "orchestrator", Method: env.Method})
	}
}
