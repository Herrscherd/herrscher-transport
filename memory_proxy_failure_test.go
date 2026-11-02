package transport

import (
	"context"
	"testing"
	"time"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

func TestProxyErrorsWhenPeerDown(t *testing.T) {
	lis := listenLoopback(t)
	s := grpc.NewServer()
	RegisterMemorySkeleton(NewPluginRouter(s), &fakeMem{})
	serveInBackground(t, s, lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer closeOrFail(t, conn)
	proxy := NewMemoryProxy(pb.NewPluginClient(conn))

	s.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := proxy.Record(ctx, contracts.Node{Key: "k"}); err == nil {
		t.Fatal("expected error when peer is down, got nil")
	}
}

func TestDialMemoryCloseReleasesConn(t *testing.T) {
	lis := listenLoopback(t)
	s := grpc.NewServer()
	RegisterMemorySkeleton(NewPluginRouter(s), &fakeMem{})
	serveInBackground(t, s, lis)
	t.Cleanup(s.Stop)

	mem, err := DialMemory(context.Background(), RemoteEntry{GrpcAddr: lis.Addr().String()}, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := mem.Record(context.Background(), contracts.Node{Key: "k"}); err != nil {
		t.Fatalf("record before close: %v", err)
	}
	if err := mem.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// After Close the local conn is gone: a further call must fail, not succeed.
	if err := mem.Record(context.Background(), contracts.Node{Key: "k2"}); err == nil {
		t.Fatal("expected error after Close released the connection, got nil")
	}
}
