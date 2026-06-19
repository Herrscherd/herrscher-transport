package transport

import (
	"context"
	"net"
	"testing"
	"time"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestProxyErrorsWhenPeerDown(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	s := grpc.NewServer()
	RegisterMemorySkeleton(s, &fakeMem{})
	go func() { _ = s.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	proxy := NewMemoryProxy(pb.NewPluginClient(conn))

	s.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := proxy.Record(ctx, contracts.Node{Key: "k"}); err == nil {
		t.Fatal("expected error when peer is down, got nil")
	}
}

func TestDialMemoryCloseReleasesConn(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	s := grpc.NewServer()
	RegisterMemorySkeleton(s, &fakeMem{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	mem, err := DialMemory(context.Background(), RemoteEntry{GrpcAddr: lis.Addr().String()})
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
