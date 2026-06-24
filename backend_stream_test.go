package transport

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// fakeBackend emits a fixed event sequence then returns a reply/error, recording
// the prompt it saw and whether it was closed.
type fakeBackend struct {
	events    []contracts.BackendEvent
	reply     string
	err       error
	gotPrompt contracts.Prompt
	closed    int
}

func (f *fakeBackend) Respond(_ context.Context, p contracts.Prompt, onEvent func(contracts.BackendEvent)) (string, error) {
	f.gotPrompt = p
	for _, e := range f.events {
		onEvent(e)
	}
	return f.reply, f.err
}
func (f *fakeBackend) Close() error { f.closed++; return nil }

// serveBackend stands up a backend skeleton (or a custom streaming server) over
// an in-memory gRPC link and returns a proxy plus the server/conn so a test can
// break the transport.
func serveBackend(t *testing.T, register func(*grpc.Server)) (*BackendProxy, *grpc.Server, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	register(s)
	go func() { _ = s.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	p := NewBackendProxy(conn)
	p.conn = conn
	return p, s, conn
}

func TestBackendStreamRoundTripsEventsThenReply(t *testing.T) {
	want := []contracts.BackendEvent{
		{Kind: "tool", Tool: "Read", Detail: "file.go"},
		{Kind: "text", Detail: "thinking…"},
		{Kind: "result", Cost: 0.42},
	}
	fake := &fakeBackend{events: want, reply: "final"}
	proxy, s, conn := serveBackend(t, func(s *grpc.Server) { RegisterBackendSkeleton(s, fake) })
	t.Cleanup(s.Stop)
	t.Cleanup(func() { _ = conn.Close() })

	var got []contracts.BackendEvent
	reply, err := proxy.Respond(context.Background(),
		contracts.Prompt{Author: "alice", Content: "hi"},
		func(e contracts.BackendEvent) { got = append(got, e) })
	if err != nil {
		t.Fatalf("Respond: %v", err)
	}
	if reply != "final" {
		t.Fatalf("reply = %q, want %q", reply, "final")
	}
	if fake.gotPrompt.Author != "alice" || fake.gotPrompt.Content != "hi" {
		t.Fatalf("prompt not forwarded: %+v", fake.gotPrompt)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d = %+v, want %+v (ordering must be preserved)", i, got[i], want[i])
		}
	}
}

func TestBackendStreamSurfacesRespondError(t *testing.T) {
	fake := &fakeBackend{
		events: []contracts.BackendEvent{{Kind: "text", Detail: "partial"}},
		err:    errors.New("boom"),
	}
	proxy, s, conn := serveBackend(t, func(s *grpc.Server) { RegisterBackendSkeleton(s, fake) })
	t.Cleanup(s.Stop)
	t.Cleanup(func() { _ = conn.Close() })

	var got int
	_, err := proxy.Respond(context.Background(), contracts.Prompt{},
		func(contracts.BackendEvent) { got++ })
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected the remote Respond error to surface, got %v", err)
	}
	if got != 1 {
		t.Fatalf("the pre-error event should still arrive in order, got %d", got)
	}
}

// droppingServer sends one event then ends the stream WITHOUT a Done frame,
// modelling a backend that crashes / drops mid-turn.
type droppingServer struct{}

func (droppingServer) respond(_ *pb.MethodEnvelope, stream grpc.ServerStream) error {
	payload, err := Marshal(backendFrame{Event: &contracts.BackendEvent{Kind: "text", Detail: "partial"}})
	if err != nil {
		return err
	}
	if err := stream.SendMsg(&pb.ResultEnvelope{JsonPayload: payload}); err != nil {
		return err
	}
	return nil // no Done frame → the client sees EOF before reply{done}
}

func TestBackendStreamDropMidTurnIsHangup(t *testing.T) {
	proxy, s, conn := serveBackend(t, func(s *grpc.Server) {
		s.RegisterService(&backendStreamServiceDesc, droppingServer{})
	})
	t.Cleanup(s.Stop)
	t.Cleanup(func() { _ = conn.Close() })

	var got int
	reply, err := proxy.Respond(context.Background(), contracts.Prompt{},
		func(contracts.BackendEvent) { got++ })
	if err == nil {
		t.Fatal("a stream ending before reply{done} must surface an error (hangup), got nil")
	}
	if !strings.Contains(err.Error(), "before reply{done}") {
		t.Fatalf("expected a hangup error, got %v", err)
	}
	if reply != "" {
		t.Fatalf("no reply should be returned on a mid-turn drop, got %q", reply)
	}
	if got != 1 {
		t.Fatalf("the event before the drop should still arrive, got %d", got)
	}
}

func TestBackendProxyCloseReleasesConnOnly(t *testing.T) {
	fake := &fakeBackend{}
	proxy, s, _ := serveBackend(t, func(s *grpc.Server) { RegisterBackendSkeleton(s, fake) })
	t.Cleanup(s.Stop)

	if err := proxy.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if fake.closed != 0 {
		t.Fatalf("proxy Close must not close the remote backend; closed=%d", fake.closed)
	}
}
