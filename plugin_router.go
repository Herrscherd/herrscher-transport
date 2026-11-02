package transport

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"weak"

	"google.golang.org/grpc"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

type pluginClaims struct {
	mu      sync.Mutex
	servers map[weak.Pointer[grpc.Server]]struct{}
}

var pluginServerClaims = &pluginClaims{servers: make(map[weak.Pointer[grpc.Server]]struct{})}

func (c *pluginClaims) claim(server *grpc.Server) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := weak.Make(server)
	if _, claimed := c.servers[key]; claimed {
		return ErrPluginServiceRegistered
	}
	c.servers[key] = struct{}{}
	runtime.AddCleanup(server, c.release, key)
	return nil
}

func (c *pluginClaims) release(key weak.Pointer[grpc.Server]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.servers, key)
}

// PluginRouter dispatches the generic Plugin service to the skeletons
// registered on it, one port each. Create one per gRPC server with
// NewPluginRouter and pass it to every Register*Skeleton on that server.
type PluginRouter struct {
	pb.UnimplementedPluginServer

	mu    sync.RWMutex
	ports map[string]pb.PluginServer
}

// NewPluginRouter registers the Plugin service on server and returns the router that
// answers it. It returns ErrPluginServiceRegistered when a router already exists for
// server, so a second router needs a second server. It must be called before
// server.Serve: once the server serves, grpc exits the process on the registration,
// and this error return cannot detect that state.
func NewPluginRouter(server *grpc.Server) (*PluginRouter, error) {
	if err := pluginServerClaims.claim(server); err != nil {
		return nil, err
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
