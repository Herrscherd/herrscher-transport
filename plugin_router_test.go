package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

func TestNewPluginRouterRefusesASecondPluginServiceOnAServer(t *testing.T) {
	server := grpc.NewServer()
	newRouter(t, server)
	if _, err := NewPluginRouter(server); !errors.Is(err, ErrPluginServiceRegistered) {
		t.Fatalf("second Plugin service on one server: want ErrPluginServiceRegistered, got %v", err)
	}
}

func TestConcurrentNewPluginRouterOnOneServerAdmitsOneCaller(t *testing.T) {
	server := grpc.NewServer()
	const callers = 16
	start := make(chan struct{})
	outcomes := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			<-start
			_, err := NewPluginRouter(server)
			outcomes <- err
		})
	}
	close(start)
	wg.Wait()
	close(outcomes)

	var admitted, refused int
	for err := range outcomes {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrPluginServiceRegistered):
			refused++
		default:
			t.Errorf("concurrent NewPluginRouter: unexpected error %v", err)
		}
	}
	if admitted != 1 || refused != callers-1 {
		t.Fatalf("concurrent NewPluginRouter on one server: want 1 admitted and %d refused, got %d and %d", callers-1, admitted, refused)
	}
}

func TestPluginRouterRefusesASecondRegistrationOfAPort(t *testing.T) {
	router := newRouter(t, grpc.NewServer())
	registerMemory(t, router, &fakeMem{})
	if err := RegisterMemorySkeleton(router, &fakeMem{}); !errors.Is(err, ErrPortRegistered) {
		t.Fatalf("second memory registration: want ErrPortRegistered, got %v", err)
	}
	registerOrchestrator(t, router, &stubOrchestrator{})
	if err := RegisterOrchestratorSkeleton(router, &stubOrchestrator{}); !errors.Is(err, ErrPortRegistered) {
		t.Fatalf("second orchestrator registration: want ErrPortRegistered, got %v", err)
	}
}

func TestPluginRouterAnswersEverySkeletonRegisteredOnIt(t *testing.T) {
	mem := &fakeMem{}
	router := newRouter(t, grpc.NewServer())
	registerMemory(t, router, mem)
	registerOrchestrator(t, router, &stubOrchestrator{})

	ctx := context.Background()
	payload, err := encodeArgs(contracts.Node{Key: "facts/router"})
	if err != nil {
		t.Fatalf("encode node: %v", err)
	}
	res, err := router.Call(ctx, &pb.MethodEnvelope{Port: PortMemory, Method: "Record", JsonPayload: payload})
	if err != nil || res.Error != "" {
		t.Fatalf("memory port: err=%v wire=%q", err, res.Error)
	}
	if mem.recorded.Key != "facts/router" {
		t.Fatalf("memory port did not dispatch, got %+v", mem.recorded)
	}

	res, err = router.Call(ctx, &pb.MethodEnvelope{Port: PortOrchestrator, Method: "Context"})
	if err != nil || res.Error != "" {
		t.Fatalf("orchestrator port: err=%v wire=%q", err, res.Error)
	}
	var primed string
	if err := decodeArgs(res.JsonPayload, &primed); err != nil {
		t.Fatalf("decode orchestrator context: %v", err)
	}
	if primed != "primed" {
		t.Fatalf("orchestrator port returned %q", primed)
	}
}

type stubOrchestrator struct{ observed string }

func (o *stubOrchestrator) Context(context.Context) string { return "primed" }
func (o *stubOrchestrator) Observe(_ context.Context, _ contracts.Prompt, reply string) error {
	o.observed = reply
	return nil
}
func (o *stubOrchestrator) Consolidate(context.Context) error { return nil }
func (o *stubOrchestrator) Close() error                      { return nil }

func TestMemoryAndOrchestratorSkeletonsShareOneServer(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	mem := &fakeMem{}
	orch := &stubOrchestrator{}
	router := newRouter(t, s)
	registerMemory(t, router, mem)
	registerOrchestrator(t, router, orch)
	serveInBackground(t, s, lis)
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { closeOrFail(t, conn) })
	client := pb.NewPluginClient(conn)

	if err = NewMemoryProxy(client).Record(context.Background(), contracts.Node{Key: "facts/x"}); err != nil {
		t.Fatalf("memory port on shared server: %v", err)
	}
	if mem.recorded.Key != "facts/x" {
		t.Fatalf("memory port did not dispatch, got %+v", mem.recorded)
	}
	if got := NewOrchestratorProxy(client).Context(context.Background()); got != "primed" {
		t.Fatalf("orchestrator port on shared server returned %q", got)
	}
	res, err := client.Call(context.Background(), &pb.MethodEnvelope{Port: "nope", Method: "Context"})
	if err != nil {
		t.Fatalf("unknown port call: %v", err)
	}
	var remote *remoteError
	if !errors.As(decodeWireError(res.JsonPayload, res.Error), &remote) || remote.message != "transport: unknown port \"nope\"" {
		t.Fatalf("want unknown port rejected, got %q", res.Error)
	}
}
