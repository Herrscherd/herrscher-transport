package transport

import (
	"context"
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

func TestPluginRouterAnswersEverySkeletonRegisteredOnIt(t *testing.T) {
	mem := &fakeMem{}
	router := NewPluginRouter(grpc.NewServer())
	RegisterMemorySkeleton(router, mem)
	RegisterOrchestratorSkeleton(router, &stubOrchestrator{})

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
