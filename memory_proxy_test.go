package transport

import (
	"context"
	"net"
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func dialSkeleton(t *testing.T, real contracts.Memory) pb.PluginClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	RegisterMemorySkeleton(s, real)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewPluginClient(conn)
}

func TestMemoryProxyRoundTrip(t *testing.T) {
	fake := &fakeMem{}
	proxy := &MemoryProxy{client: dialSkeleton(t, fake)}

	if err := proxy.Record(context.Background(), contracts.Node{Key: "sessions/x", Title: "X"}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if fake.recorded.Key != "sessions/x" {
		t.Fatalf("Record not propagated: %+v", fake.recorded)
	}

	sub, err := proxy.Recall(context.Background(), "sessions/x", 1)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if sub.Root.Key != "sessions/x" {
		t.Fatalf("Recall round-trip wrong: %+v", sub.Root)
	}

	hits, err := proxy.Search(context.Background(), contracts.Query{Text: "x"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].Key != "hit" {
		t.Fatalf("Search round-trip wrong: %+v", hits)
	}
}

// TestProxyCloseKeepsSharedRealAndPeersAlive guards the plugin-host topology:
// one server (one real Memory) behind two client connections. Closing one proxy
// must release only its own connection — it must not close the shared real, and
// the other proxy must stay usable.
func TestProxyCloseKeepsSharedRealAndPeersAlive(t *testing.T) {
	fake := &fakeMem{}
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	RegisterMemorySkeleton(s, fake)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	dial := func() *MemoryProxy {
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		p := NewMemoryProxy(pb.NewPluginClient(conn))
		p.conn = conn
		return p
	}

	a, b := dial(), dial()
	if err := a.Close(); err != nil {
		t.Fatalf("close A: %v", err)
	}
	if fake.closed != 0 {
		t.Fatalf("client Close must not close the shared real memory; closed=%d", fake.closed)
	}
	if err := b.Record(context.Background(), contracts.Node{Key: "k"}); err != nil {
		t.Fatalf("peer B must stay usable after A closed: %v", err)
	}
}
