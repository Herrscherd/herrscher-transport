package transport

import (
	"context"
	"errors"
	"net"
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// fakeOrch records what the proxy forwarded and lets a test force an error.
type fakeOrch struct {
	ctxOut       string
	gotPrompt    contracts.Prompt
	gotReply     string
	observeErr   error
	consolidated int
	closed       int
}

func (f *fakeOrch) Context(context.Context) string { return f.ctxOut }
func (f *fakeOrch) Observe(_ context.Context, p contracts.Prompt, reply string) error {
	f.gotPrompt, f.gotReply = p, reply
	return f.observeErr
}
func (f *fakeOrch) Consolidate(context.Context) error { f.consolidated++; return nil }
func (f *fakeOrch) Close() error                      { f.closed++; return nil }

// serveOrch stands up the orchestrator skeleton over an in-memory gRPC link and
// returns a proxy plus the server/conn so a test can break the transport.
func serveOrch(t *testing.T, real contracts.Orchestrator) (*OrchestratorProxy, *grpc.Server, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	RegisterOrchestratorSkeleton(s, real)
	go func() { _ = s.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	p := NewOrchestratorProxy(pb.NewPluginClient(conn))
	p.conn = conn
	return p, s, conn
}

func TestOrchestratorProxyRoundTrip(t *testing.T) {
	fake := &fakeOrch{ctxOut: "prior context"}
	proxy, s, conn := serveOrch(t, fake)
	t.Cleanup(s.Stop)
	t.Cleanup(func() { _ = conn.Close() })

	if got := proxy.Context(context.Background()); got != "prior context" {
		t.Fatalf("Context round-trip wrong: %q", got)
	}

	if err := proxy.Observe(context.Background(), contracts.Prompt{Author: "alice", Content: "hi"}, "reply text"); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if fake.gotPrompt.Author != "alice" || fake.gotReply != "reply text" {
		t.Fatalf("Observe not propagated: %+v reply=%q", fake.gotPrompt, fake.gotReply)
	}

	if err := proxy.Consolidate(context.Background()); err != nil {
		t.Fatalf("Consolidate: %v", err)
	}
	if fake.consolidated != 1 {
		t.Fatalf("Consolidate not propagated: %d", fake.consolidated)
	}
}

// TestOrchestratorProxyContextDegradesOnTransportError pins the port contract:
// Context must never surface a turn-breaking error — a broken transport yields "".
func TestOrchestratorProxyContextDegradesOnTransportError(t *testing.T) {
	proxy, s, conn := serveOrch(t, &fakeOrch{ctxOut: "ctx"})

	if got := proxy.Context(context.Background()); got != "ctx" {
		t.Fatalf("pre-break Context wrong: %q", got)
	}
	// Break the transport; Context must degrade to "" rather than error out.
	s.Stop()
	_ = conn.Close()
	if got := proxy.Context(context.Background()); got != "" {
		t.Fatalf("Context must degrade to empty on transport failure, got %q", got)
	}
}

// TestOrchestratorProxyObserveSurfacesError asserts Observe propagates the
// remote's error (best-effort; the host logs it but does not break the loop).
func TestOrchestratorProxyObserveSurfacesError(t *testing.T) {
	proxy, s, conn := serveOrch(t, &fakeOrch{observeErr: errors.New("boom")})
	t.Cleanup(s.Stop)
	t.Cleanup(func() { _ = conn.Close() })

	if err := proxy.Observe(context.Background(), contracts.Prompt{}, "r"); err == nil {
		t.Fatal("expected Observe to surface the remote error")
	}
}

// TestOrchestratorProxyCloseReleasesConnOnly guards the topology: closing the
// proxy releases only its connection and never closes the remote orchestrator
// (its host owns that lifecycle).
func TestOrchestratorProxyCloseReleasesConnOnly(t *testing.T) {
	fake := &fakeOrch{}
	proxy, s, _ := serveOrch(t, fake)
	t.Cleanup(s.Stop)

	if err := proxy.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if fake.closed != 0 {
		t.Fatalf("proxy Close must not close the remote orchestrator; closed=%d", fake.closed)
	}
}
