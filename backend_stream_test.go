package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

type fakeBackend struct {
	events    []contracts.BackendEvent
	reply     string
	err       error
	gotPrompt contracts.Prompt
	closed    int
}

func (f *fakeBackend) Respond(_ context.Context, p contracts.Prompt, onEvent func(contracts.BackendEvent)) (string, error) {
	f.gotPrompt = p
	for i := range f.events {
		onEvent(f.events[i])
	}
	return f.reply, f.err
}
func (f *fakeBackend) Close() error { f.closed++; return nil }

func serveBackend(t *testing.T, register func(*grpc.Server)) (*BackendProxy, *grpc.Server, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	register(s)
	serveInBackground(t, s, lis)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	p := NewBackendProxy(conn)
	p.dialed = conn
	return p, s, conn
}

func TestBackendStreamRoundTripsEventsThenReply(t *testing.T) {
	want := []contracts.BackendEvent{
		{Kind: "tool", Tool: "Read", Detail: "file.go"},
		{Kind: "text", Detail: "thinking…"},
		{Kind: "result", Cost: 0.42},
		{Kind: "todo", Todos: []contracts.TodoItem{{Text: "écrire les tests", State: "done"}}, Subagent: &contracts.Subagent{ID: "s1", Name: "relecteur", Kind: "review", State: "running"}},
	}
	fake := &fakeBackend{events: want, reply: "final"}
	proxy, s, conn := serveBackend(t, func(s *grpc.Server) { RegisterBackendSkeleton(s, fake) })
	t.Cleanup(s.Stop)
	t.Cleanup(func() { closeOrFail(t, conn) })

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
		if !sameEvent(got[i], want[i]) {
			t.Fatalf("event %d = %+v, want %+v (ordering must be preserved)", i, got[i], want[i])
		}
	}
}

func sameEvent(a, b contracts.BackendEvent) bool {
	left, errLeft := json.Marshal(a)
	right, errRight := json.Marshal(b)
	return errLeft == nil && errRight == nil && bytes.Equal(left, right)
}

func TestBackendStreamSurfacesRespondError(t *testing.T) {
	fake := &fakeBackend{
		events: []contracts.BackendEvent{{Kind: "text", Detail: "partial"}},
		err:    errors.New("boom"),
	}
	proxy, s, conn := serveBackend(t, func(s *grpc.Server) { RegisterBackendSkeleton(s, fake) })
	t.Cleanup(s.Stop)
	t.Cleanup(func() { closeOrFail(t, conn) })

	var got int
	_, err := proxy.Respond(context.Background(), contracts.Prompt{},
		func(contracts.BackendEvent) { got++ })
	var remote *remoteError
	if err == nil || !errors.As(err, &remote) || remote.message != "boom" {
		t.Fatalf("expected the remote Respond error to surface, got %v", err)
	}
	if got != 1 {
		t.Fatalf("the pre-error event should still arrive in order, got %d", got)
	}
}

type droppingServer struct{}

func (droppingServer) respond(_ *pb.MethodEnvelope, stream grpc.ServerStream) error {
	payload, err := Marshal(backendFrame{Event: &contracts.BackendEvent{Kind: "text", Detail: "partial"}})
	if err != nil {
		return err
	}
	if err := stream.SendMsg(&pb.ResultEnvelope{JsonPayload: payload}); err != nil {
		return err
	}
	return nil
}

func TestBackendStreamDropMidTurnIsHangup(t *testing.T) {
	proxy, s, conn := serveBackend(t, func(s *grpc.Server) {
		desc := backendStreamServiceDesc()
		s.RegisterService(&desc, droppingServer{})
	})
	t.Cleanup(s.Stop)
	t.Cleanup(func() { closeOrFail(t, conn) })

	var got int
	reply, err := proxy.Respond(context.Background(), contracts.Prompt{},
		func(contracts.BackendEvent) { got++ })
	if err == nil {
		t.Fatal("a stream ending before reply{done} must surface an error (hangup), got nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
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

type recvNilStream struct{ grpc.ServerStream }

func (recvNilStream) RecvMsg(any) error { return nil }

func TestBackendRespondHandlerRefusesServerWithoutRespond(t *testing.T) {
	if err := backendRespondHandler(struct{}{}, recvNilStream{}); !errors.Is(err, errBackendStreamNotResponder) {
		t.Fatalf("a server without respond must be refused with errBackendStreamNotResponder, got %v", err)
	}
}

type concurrentBackend struct{ events int }

func (b *concurrentBackend) Respond(_ context.Context, _ contracts.Prompt, onEvent func(contracts.BackendEvent)) (string, error) {
	var wg sync.WaitGroup
	for range b.events {
		wg.Add(1)
		go func() {
			defer wg.Done()
			onEvent(contracts.BackendEvent{Kind: "text", Detail: "chunk"})
		}()
	}
	wg.Wait()
	return "done", nil
}
func (b *concurrentBackend) Close() error { return nil }

func TestBackendSkeletonToleratesConcurrentOnEvent(t *testing.T) {
	proxy, s, conn := serveBackend(t, func(s *grpc.Server) {
		RegisterBackendSkeleton(s, &concurrentBackend{events: 32})
	})
	t.Cleanup(s.Stop)
	t.Cleanup(func() { closeOrFail(t, conn) })

	var seen int
	reply, err := proxy.Respond(context.Background(), contracts.Prompt{Content: "hi"}, func(contracts.BackendEvent) { seen++ })
	if err != nil {
		t.Fatalf("respond: %v", err)
	}
	if reply != "done" {
		t.Fatalf("reply = %q", reply)
	}
	if seen != 32 {
		t.Fatalf("want 32 events forwarded, got %d", seen)
	}
}
