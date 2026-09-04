package transport

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestDecodeArgsRejectsArityMismatch(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		dests   int
		wantErr bool
	}{
		{name: "exact", payload: `["k",3]`, dests: 2},
		{name: "missing trailing arg", payload: `["k"]`, dests: 2, wantErr: true},
		{name: "extra arg", payload: `["a","b","c"]`, dests: 2, wantErr: true},
		{name: "empty array for two", payload: `[]`, dests: 2, wantErr: true},
		{name: "null payload", payload: `null`, dests: 1, wantErr: true},
		{name: "not an array", payload: `{"key":"k"}`, dests: 1, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key, depth, extra := "", 0, ""
			dst := []any{&key, &depth, &extra}[:tc.dests]
			err := decodeArgs([]byte(tc.payload), dst...)
			if tc.wantErr != (err != nil) {
				t.Fatalf("decodeArgs(%s, %d dests) error = %v, wantErr %v", tc.payload, tc.dests, err, tc.wantErr)
			}
		})
	}
}

func TestMemorySkeletonRejectsShortArgTuple(t *testing.T) {
	fake := &fakeMem{}
	srv := &memoryServer{real: fake}
	args, _ := Marshal([]any{"facts/x"})
	res, err := srv.Call(context.Background(),
		&pb.MethodEnvelope{Port: PortMemory, Method: "Recall", JsonPayload: args})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if res.Error == "" {
		t.Fatal("want a short Recall tuple rejected, got a silent depth-0 recall")
	}
}

type budgetMem struct{ fakeMem }

func (b *budgetMem) Record(_ context.Context, n contracts.Node) error {
	return &contracts.BudgetError{Key: n.Key, Runes: 900, Limit: 400}
}

func TestTypedErrorSurvivesTheWire(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	RegisterMemorySkeleton(s, &budgetMem{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	err = NewMemoryProxy(pb.NewPluginClient(conn)).Record(context.Background(), contracts.Node{Key: "facts/x"})
	var budget *contracts.BudgetError
	if !errors.As(err, &budget) {
		t.Fatalf("want *contracts.BudgetError across the proxy, got %T (%v)", err, err)
	}
	if budget.Key != "facts/x" || budget.Runes != 900 || budget.Limit != 400 {
		t.Fatalf("typed fields lost: %+v", budget)
	}
}

func TestUntypedErrorKeepsFlatMessage(t *testing.T) {
	res, _ := fail(errors.New("transport: unknown port \"nope\""))
	got := decodeWireError(res.JsonPayload, res.Error)
	if got.Error() != "transport: unknown port \"nope\"" {
		t.Fatalf("flat message lost: %v", got)
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
	RegisterMemorySkeleton(s, mem)
	RegisterOrchestratorSkeleton(s, orch)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewPluginClient(conn)

	if err := NewMemoryProxy(client).Record(context.Background(), contracts.Node{Key: "facts/x"}); err != nil {
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
	if !strings.Contains(res.Error, "unknown port") {
		t.Fatalf("want unknown port rejected, got %q", res.Error)
	}
}

type concurrentBackend struct{ events int }

func (b *concurrentBackend) Respond(_ context.Context, _ contracts.Prompt, onEvent func(contracts.BackendEvent)) (string, error) {
	var wg sync.WaitGroup
	for i := 0; i < b.events; i++ {
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
	t.Cleanup(func() { _ = conn.Close() })

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

func TestObserveDropsUnqueryableAnnouncements(t *testing.T) {
	cases := []struct {
		name string
		ann  Announcement
	}{
		{name: "empty instance id", ann: Announcement{
			Manifest: contracts.Manifest{Category: contracts.CategoryMemory}, GrpcAddr: "127.0.0.1:9001"}},
		{name: "empty category", ann: Announcement{
			InstanceID: "a", GrpcAddr: "127.0.0.1:9001"}},
		{name: "port only address", ann: memAnnounce("a", ":50051")},
		{name: "host only address", ann: memAnnounce("a", "127.0.0.1:")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRemoteRegistry()
			r.Observe(tc.ann)
			r.mu.RLock()
			n := len(r.entries)
			r.mu.RUnlock()
			if n != 0 {
				t.Fatalf("want announcement dropped, registry holds %d entries", n)
			}
		})
	}
}

func TestObserveKeepsDistinctInstancesWithoutCollapsing(t *testing.T) {
	r := NewRemoteRegistry()
	r.Observe(memAnnounce("", "127.0.0.1:9001"))
	r.Observe(memAnnounce("", "127.0.0.1:9002"))
	if got := r.Memories(); len(got) != 0 {
		t.Fatalf("want identity-less announcements dropped, got %+v", got)
	}
	r.Observe(memAnnounce("a", "127.0.0.1:9001"))
	r.Observe(memAnnounce("b", "127.0.0.1:9002"))
	if got := r.Memories(); len(got) != 2 {
		t.Fatalf("want both identified plugins visible, got %+v", got)
	}
}

func TestWatchAnnouncementsReportsDecodeErrors(t *testing.T) {
	nc := runNATS(t)
	failures := make(chan error, 1)
	if err := WatchAnnouncementsFunc(nc, func(Announcement) {}, func(err error) { failures <- err }); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if err := nc.Publish(SubjectAnnounce, []byte("{not json")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case err := <-failures:
		if !strings.Contains(err.Error(), "decode announcement") {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("malformed announcement dropped with no diagnostic")
	}
}
