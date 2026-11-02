package transport

import (
	"context"
	"errors"
	"net"
	"testing"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

type budgetMem struct{ fakeMem }

func (b *budgetMem) Record(_ context.Context, n contracts.Node) error {
	return &contracts.BudgetError{Key: n.Key, Runes: 900, Limit: 400}
}

func TestTypedErrorSurvivesTheWire(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	registerMemory(t, NewPluginRouter(s), &budgetMem{})
	serveInBackground(t, s, lis)
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { closeOrFail(t, conn) })

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
	res, err := fail(errors.New("transport: unknown port \"nope\""))
	if err != nil {
		t.Fatal(err)
	}
	got := decodeWireError(res.JsonPayload, res.Error)
	var remote *remoteError
	if !errors.As(got, &remote) || remote.message != "transport: unknown port \"nope\"" {
		t.Fatalf("flat message lost: %v", got)
	}
}
