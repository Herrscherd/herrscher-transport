package transport

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

type pluginRouter struct {
	pb.UnimplementedPluginServer

	mu    sync.RWMutex
	ports map[string]pb.PluginServer
}

func (r *pluginRouter) add(port string, s pb.PluginServer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ports[port] = s
}

func (r *pluginRouter) Call(ctx context.Context, env *pb.MethodEnvelope) (*pb.ResultEnvelope, error) {
	r.mu.RLock()
	s, ok := r.ports[env.Port]
	r.mu.RUnlock()
	if !ok {
		return fail(&UnknownPortError{Port: env.Port})
	}
	return s.Call(ctx, env)
}

var (
	routersMu sync.Mutex
	routers   = map[*grpc.Server]*pluginRouter{}
)

func routerFor(s *grpc.Server) *pluginRouter {
	routersMu.Lock()
	defer routersMu.Unlock()
	if r, ok := routers[s]; ok {
		return r
	}
	r := &pluginRouter{ports: map[string]pb.PluginServer{}}
	routers[s] = r
	pb.RegisterPluginServer(s, r)
	return r
}
