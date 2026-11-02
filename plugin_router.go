package transport

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

// PluginRouter dispatches the generic Plugin service to the skeletons
// registered on it, one port each. Create one per gRPC server with
// NewPluginRouter and pass it to every Register*Skeleton on that server.
type PluginRouter struct {
	pb.UnimplementedPluginServer

	mu    sync.RWMutex
	ports map[string]pb.PluginServer
}

// NewPluginRouter registers the Plugin service on server and returns the router that
// answers it. It returns ErrPluginServiceRegistered when server already serves the
// Plugin service, so a second router needs a second server.
func NewPluginRouter(server *grpc.Server) (*PluginRouter, error) {
	if _, registered := server.GetServiceInfo()[pb.Plugin_ServiceDesc.ServiceName]; registered {
		return nil, ErrPluginServiceRegistered
	}
	router := &PluginRouter{ports: make(map[string]pb.PluginServer)}
	pb.RegisterPluginServer(server, router)
	return router, nil
}

func (r *PluginRouter) add(port string, skeleton pb.PluginServer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, registered := r.ports[port]; registered {
		return fmt.Errorf("%w: %s", ErrPortRegistered, port)
	}
	r.ports[port] = skeleton
	return nil
}

// Call forwards the envelope to the skeleton registered on its port and reports
// an unknown port as a wire error.
func (r *PluginRouter) Call(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	r.mu.RLock()
	skeleton, ok := r.ports[env.Port]
	r.mu.RUnlock()
	if !ok {
		return fail(&UnknownPortError{Port: env.Port})
	}
	return skeleton.Call(ctx, env)
}
