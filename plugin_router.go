package transport

import (
	"context"
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

// NewPluginRouter registers the Plugin service on s and returns the router that
// answers it.
func NewPluginRouter(s *grpc.Server) *PluginRouter {
	r := &PluginRouter{ports: map[string]pb.PluginServer{}}
	pb.RegisterPluginServer(s, r)
	return r
}

func (r *PluginRouter) add(port string, s pb.PluginServer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ports[port] = s
}

// Call forwards the envelope to the skeleton registered on its port and reports
// an unknown port as a wire error.
func (r *PluginRouter) Call(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	r.mu.RLock()
	s, ok := r.ports[env.Port]
	r.mu.RUnlock()
	if !ok {
		return fail(&UnknownPortError{Port: env.Port})
	}
	return s.Call(ctx, env)
}
