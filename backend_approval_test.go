package transport

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

const approvalTestTimeout = 5 * time.Second

var errNoAskerOnTurn = errors.New("no approval asker on the turn")

type askResult struct {
	ID     string
	Answer contracts.ApprovalAnswer
}

type askingBackend struct {
	requests []contracts.ApprovalRequest
	out      chan askResult
}

func newAskingBackend(requests ...contracts.ApprovalRequest) *askingBackend {
	return &askingBackend{requests: requests, out: make(chan askResult, len(requests))}
}

func (b *askingBackend) Respond(ctx context.Context, _ contracts.Prompt, _ func(contracts.BackendEvent)) (string, error) {
	asker, ok := contracts.ApprovalAskerFrom(ctx)
	if !ok {
		return "", errNoAskerOnTurn
	}
	var asks sync.WaitGroup
	for _, req := range b.requests {
		asks.Add(1)
		go func() {
			defer asks.Done()
			b.out <- askResult{ID: req.ID, Answer: asker(ctx, req)}
		}()
	}
	asks.Wait()
	return "done", nil
}

func (b *askingBackend) Close() error { return nil }

func serveApproval(t *testing.T, register func(*grpc.Server)) *BackendProxy {
	t.Helper()
	proxy, s, conn := serveBackend(t, register)
	t.Cleanup(s.Stop)
	t.Cleanup(func() { closeOrFail(t, conn) })
	return proxy
}

func serveAsking(t *testing.T, backend contracts.Backend) *BackendProxy {
	t.Helper()
	return serveApproval(t, func(s *grpc.Server) { RegisterBackendSkeleton(s, backend) })
}

func receiveResult(t *testing.T, out <-chan askResult) askResult {
	t.Helper()
	select {
	case result := <-out:
		return result
	case <-time.After(approvalTestTimeout):
		t.Fatal("the backend received no approval answer")
	}
	return askResult{}
}

func answering(decision contracts.ApprovalDecision, reason string) contracts.ApprovalAsker {
	return func(_ context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
		return contracts.ApprovalAnswer{ID: req.ID, Decision: decision, Reason: reason}
	}
}

func answerEnvelope(t *testing.T, answer contracts.ApprovalAnswer) *pb.MethodEnvelope {
	t.Helper()
	return &pb.MethodEnvelope{Port: PortBackend, Method: approvalAnswerMethod, JsonPayload: mustMarshal(t, answer)}
}

func TestApprovalAllowIsRelayed(t *testing.T) {
	backend := newAskingBackend(contracts.ApprovalRequest{ID: "a1", Tool: "Bash", Subject: "ls"})
	proxy := serveAsking(t, backend)

	ctx := contracts.WithApprovalAsker(context.Background(), answering(contracts.DecisionAllow, "ok"))
	reply, err := proxy.RespondWithApprovals(ctx, contracts.Prompt{Content: "hi"}, nil)
	if err != nil {
		t.Fatalf("RespondWithApprovals: %v", err)
	}
	if reply != "done" {
		t.Fatalf("reply = %q, want %q", reply, "done")
	}
	got := receiveResult(t, backend.out)
	if got.Answer.ID != "a1" || !got.Answer.Allowed() || got.Answer.Reason != "ok" {
		t.Fatalf("backend got %+v, want allow with reason ok for a1", got.Answer)
	}
}

func TestApprovalDenyIsRelayed(t *testing.T) {
	backend := newAskingBackend(contracts.ApprovalRequest{ID: "d1", Tool: "Bash", Subject: "rm"})
	proxy := serveAsking(t, backend)

	ctx := contracts.WithApprovalAsker(context.Background(), answering(contracts.DecisionDeny, "non"))
	if _, err := proxy.RespondWithApprovals(ctx, contracts.Prompt{}, nil); err != nil {
		t.Fatalf("RespondWithApprovals: %v", err)
	}
	got := receiveResult(t, backend.out)
	if got.Answer.ID != "d1" || got.Answer.Allowed() || got.Answer.Reason != "non" {
		t.Fatalf("backend got %+v, want deny with reason non for d1", got.Answer)
	}
}

func TestApprovalClientWithoutAskerIsDenied(t *testing.T) {
	backend := newAskingBackend(contracts.ApprovalRequest{ID: "n1", Tool: "Bash", Subject: "ls"})
	proxy := serveAsking(t, backend)

	reply, err := proxy.RespondWithApprovals(context.Background(), contracts.Prompt{}, nil)
	if err != nil {
		t.Fatalf("RespondWithApprovals: %v", err)
	}
	if reply != "done" {
		t.Fatalf("reply = %q, want %q: the turn must finish when the client has no asker", reply, "done")
	}
	got := receiveResult(t, backend.out)
	if got.Answer.Allowed() {
		t.Fatalf("a client without an asker must deny, got %+v", got.Answer)
	}
}

func TestApprovalCancelledTurnIsDenied(t *testing.T) {
	backend := newAskingBackend(contracts.ApprovalRequest{ID: "c1", Tool: "Bash", Subject: "ls"})
	proxy := serveAsking(t, backend)

	asking := make(chan struct{})
	asker := func(askCtx context.Context, _ contracts.ApprovalRequest) contracts.ApprovalAnswer {
		close(asking)
		<-askCtx.Done()
		return contracts.ApprovalAnswer{ID: "c1", Decision: contracts.DecisionDeny}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-asking
		cancel()
	}()

	if _, err := proxy.RespondWithApprovals(contracts.WithApprovalAsker(ctx, asker), contracts.Prompt{}, nil); err == nil {
		t.Fatal("a cancelled turn must return an error")
	}
	got := receiveResult(t, backend.out)
	if got.Answer.Allowed() {
		t.Fatalf("a request cut by cancellation must be denied, got %+v", got.Answer)
	}
}

func TestApprovalBrokenStreamIsDenied(t *testing.T) {
	backend := newAskingBackend(contracts.ApprovalRequest{ID: "b1", Tool: "Bash", Subject: "ls"})
	proxy, s, conn := serveBackend(t, func(s *grpc.Server) { RegisterBackendSkeleton(s, backend) })
	t.Cleanup(s.Stop)

	asking := make(chan struct{})
	asker := func(askCtx context.Context, _ contracts.ApprovalRequest) contracts.ApprovalAnswer {
		close(asking)
		<-askCtx.Done()
		return contracts.ApprovalAnswer{ID: "b1", Decision: contracts.DecisionDeny}
	}
	go func() {
		<-asking
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()

	if _, err := proxy.RespondWithApprovals(contracts.WithApprovalAsker(context.Background(), asker), contracts.Prompt{}, nil); err == nil {
		t.Fatal("a broken stream must return an error")
	}
	got := receiveResult(t, backend.out)
	if got.Answer.Allowed() {
		t.Fatalf("a request cut by a broken stream must be denied, got %+v", got.Answer)
	}
}

func TestApprovalAnswerWithUnknownIDIsRejected(t *testing.T) {
	desk := newApprovalDesk(nil)
	waiter, err := desk.open("a1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := desk.accept(answerEnvelope(t, contracts.ApprovalAnswer{ID: "nope", Decision: contracts.DecisionAllow})); !errors.Is(err, ErrUnknownApproval) {
		t.Fatalf("an answer with an unknown ID must be rejected with ErrUnknownApproval, got %v", err)
	}
	if err := desk.accept(answerEnvelope(t, contracts.ApprovalAnswer{ID: "a1", Decision: contracts.DecisionAllow})); err != nil {
		t.Fatalf("the answer to the pending request must be accepted: %v", err)
	}
	if got := <-waiter; !got.Allowed() {
		t.Fatalf("the pending request got %+v, want allow", got)
	}
	if err := desk.accept(answerEnvelope(t, contracts.ApprovalAnswer{ID: "a1", Decision: contracts.DecisionAllow})); !errors.Is(err, ErrUnknownApproval) {
		t.Fatalf("a second answer to the same request must be rejected with ErrUnknownApproval, got %v", err)
	}
}

func TestApprovalConcurrentRequestsAreCorrelated(t *testing.T) {
	const count = 8
	requests := make([]contracts.ApprovalRequest, 0, count)
	decisions := make(map[string]contracts.ApprovalDecision, count)
	delays := make(map[string]time.Duration, count)
	for index := range count {
		id := fmt.Sprintf("req-%d", index)
		decision := contracts.DecisionAllow
		if index%2 == 1 {
			decision = contracts.DecisionDeny
		}
		requests = append(requests, contracts.ApprovalRequest{ID: id, Tool: "Edit", Subject: id})
		decisions[id] = decision
		delays[id] = time.Duration(count-index) * time.Millisecond
	}
	backend := newAskingBackend(requests...)
	proxy := serveAsking(t, backend)

	asker := func(ctx context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
		select {
		case <-time.After(delays[req.ID]):
		case <-ctx.Done():
		}
		return contracts.ApprovalAnswer{ID: req.ID, Decision: decisions[req.ID], Reason: req.ID}
	}
	if _, err := proxy.RespondWithApprovals(contracts.WithApprovalAsker(context.Background(), asker), contracts.Prompt{}, nil); err != nil {
		t.Fatalf("RespondWithApprovals: %v", err)
	}

	seen := make(map[string]bool, count)
	for range count {
		got := receiveResult(t, backend.out)
		if got.Answer.ID != got.ID {
			t.Fatalf("request %q received the answer of %q", got.ID, got.Answer.ID)
		}
		if got.Answer.Decision != decisions[got.ID] || got.Answer.Reason != got.ID {
			t.Fatalf("request %q got %+v, want decision %q", got.ID, got.Answer, decisions[got.ID])
		}
		seen[got.ID] = true
	}
	if len(seen) != count {
		t.Fatalf("%d distinct requests answered, want %d", len(seen), count)
	}
}

func TestRespondWithApprovalsOnLegacyServerIsUnsupported(t *testing.T) {
	fake := &fakeBackend{events: []contracts.BackendEvent{{Kind: "text", Detail: "x"}}, reply: "legacy"}
	proxy := serveApproval(t, func(s *grpc.Server) {
		desc := backendServiceDescFor(backendRespondStream())
		s.RegisterService(&desc, &backendServer{real: fake})
	})

	_, err := proxy.RespondWithApprovals(context.Background(), contracts.Prompt{}, nil)
	if !errors.Is(err, ErrApprovalStreamUnsupported) {
		t.Fatalf("a server without the approval stream must yield ErrApprovalStreamUnsupported, got %v", err)
	}
	reply, err := proxy.Respond(context.Background(), contracts.Prompt{}, nil)
	if err != nil || reply != "legacy" {
		t.Fatalf("Respond must still serve the legacy server: reply %q, err %v", reply, err)
	}
}

type rawBackendServer struct {
	frames []backendFrame
}

func (s rawBackendServer) respond(_ *pb.MethodEnvelope, _ grpc.ServerStream) error {
	return nil
}

func (s rawBackendServer) respondWithApprovals(_ *pb.MethodEnvelope, stream grpc.ServerStream) error {
	for _, frame := range s.frames {
		payload, err := Marshal(frame)
		if err != nil {
			return err
		}
		if err := stream.SendMsg(&pb.ResultEnvelope{JsonPayload: payload}); err != nil {
			return err
		}
	}
	return nil
}

func TestApprovalRequestOverBoundIsRefused(t *testing.T) {
	oversized := backendFrame{Approval: &contracts.ApprovalRequest{
		ID:      "big",
		Tool:    "Bash",
		Subject: strings.Repeat("s", maxApprovalBytes),
	}}
	proxy := serveApproval(t, func(s *grpc.Server) {
		desc := backendStreamServiceDesc()
		s.RegisterService(&desc, rawBackendServer{frames: []backendFrame{oversized}})
	})

	ctx := contracts.WithApprovalAsker(context.Background(), answering(contracts.DecisionAllow, ""))
	if _, err := proxy.RespondWithApprovals(ctx, contracts.Prompt{}, nil); !errors.Is(err, ErrApprovalTooLarge) {
		t.Fatalf("an oversized request must be refused with ErrApprovalTooLarge, got %v", err)
	}
}

func TestApprovalAnswerOverBoundIsRefusedByServer(t *testing.T) {
	desk := newApprovalDesk(nil)
	envelope := &pb.MethodEnvelope{Port: PortBackend, Method: approvalAnswerMethod, JsonPayload: make([]byte, maxApprovalBytes+1)}
	if err := desk.accept(envelope); !errors.Is(err, ErrApprovalTooLarge) {
		t.Fatalf("an oversized answer must be refused with ErrApprovalTooLarge, got %v", err)
	}
}

func TestApprovalDeskBoundsRequestsInFlight(t *testing.T) {
	desk := newApprovalDesk(nil)
	for index := range maxInflightApprovals {
		if _, err := desk.open(fmt.Sprintf("r%d", index)); err != nil {
			t.Fatalf("request %d within the bound was refused: %v", index, err)
		}
	}
	if _, err := desk.open("one-too-many"); !errors.Is(err, ErrTooManyApprovals) {
		t.Fatalf("request beyond the bound must be refused with ErrTooManyApprovals, got %v", err)
	}
}

func TestApprovalTurnsLeaveNoGoroutine(t *testing.T) {
	backend := newAskingBackend(contracts.ApprovalRequest{ID: "g1", Tool: "Bash", Subject: "ls"})
	proxy := serveAsking(t, backend)
	ctx := contracts.WithApprovalAsker(context.Background(), answering(contracts.DecisionAllow, ""))
	runTurn := func() {
		if _, err := proxy.RespondWithApprovals(ctx, contracts.Prompt{}, nil); err != nil {
			t.Fatalf("RespondWithApprovals: %v", err)
		}
		receiveResult(t, backend.out)
	}

	runTurn()
	before := runtime.NumGoroutine()
	for range 5 {
		runTurn()
	}
	deadline := time.Now().Add(approvalTestTimeout)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines grew from %d to %d after approval turns", before, after)
	}
}
