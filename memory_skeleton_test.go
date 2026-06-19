package transport

import (
	"context"
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
)

type fakeMem struct {
	recorded contracts.Node
	recall   contracts.Subgraph
}

func (f *fakeMem) Recall(_ context.Context, key string, depth int) (contracts.Subgraph, error) {
	f.recall.Root = contracts.Node{Key: key}
	return f.recall, nil
}
func (f *fakeMem) Record(_ context.Context, n contracts.Node) error { f.recorded = n; return nil }
func (f *fakeMem) Search(_ context.Context, q contracts.Query) ([]contracts.Node, error) {
	return []contracts.Node{{Key: "hit"}}, nil
}
func (f *fakeMem) Links(_ context.Context, from, to, rel string) error { return nil }
func (f *fakeMem) Close() error                                        { return nil }

func TestMemorySkeletonRecord(t *testing.T) {
	fake := &fakeMem{}
	srv := &memoryServer{real: fake}
	args, _ := Marshal([]any{contracts.Node{Key: "sessions/x", Title: "X"}})
	res, err := srv.Call(context.Background(),
		&pb.MethodEnvelope{Port: "memory", Method: "Record", JsonPayload: args})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if res.Error != "" {
		t.Fatalf("unexpected error: %s", res.Error)
	}
	if fake.recorded.Key != "sessions/x" {
		t.Fatalf("Record not dispatched, got %+v", fake.recorded)
	}
}
