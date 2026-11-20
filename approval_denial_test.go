package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

var errSendRefused = errors.New("send refused")

type scriptedServerStream struct {
	grpc.ServerStream

	sendErr error
	recvErr error
}

func (s *scriptedServerStream) Context() context.Context { return context.Background() }

func (s *scriptedServerStream) SendMsg(any) error { return s.sendErr }

func (s *scriptedServerStream) RecvMsg(any) error { return s.recvErr }

type recordingClientStream struct {
	grpc.ClientStream

	sent []*pb.MethodEnvelope
}

func (s *recordingClientStream) SendMsg(msg any) error {
	envelope, ok := msg.(*pb.MethodEnvelope)
	if !ok {
		return fmt.Errorf("unexpected message %T", msg)
	}
	s.sent = append(s.sent, envelope)
	return nil
}

func healthyStream() *scriptedServerStream {
	return &scriptedServerStream{}
}

func requireDenial(t *testing.T, answer contracts.ApprovalAnswer, want DenialReason) {
	t.Helper()
	if answer.Allowed() || answer.Decision != contracts.DecisionDeny {
		t.Fatalf("answer = %+v, want a denial", answer)
	}
	if answer.Reason != want.Key() {
		t.Fatalf("Reason = %q, want %q", answer.Reason, want.Key())
	}
}

func TestDenialAskWithoutIDIsInvalid(t *testing.T) {
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	requireDenial(t, desk.ask(context.Background(), contracts.ApprovalRequest{Tool: "Bash"}), DenialInvalid)
}

func TestDenialAskOversizedRequestIsTooLarge(t *testing.T) {
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	request := contracts.ApprovalRequest{ID: "huge", Tool: "Bash", Subject: strings.Repeat("s", maxApprovalBytes)}
	requireDenial(t, desk.ask(context.Background(), request), DenialTooLarge)
}

func TestDenialAskBeyondInflightBoundIsTooMany(t *testing.T) {
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	for index := range maxInflightApprovals {
		if _, err := desk.open(fmt.Sprintf("r%d", index)); err != nil {
			t.Fatalf("request %d within the bound was refused: %v", index, err)
		}
	}
	requireDenial(t, desk.ask(context.Background(), contracts.ApprovalRequest{ID: "extra", Tool: "Bash"}), DenialTooMany)
}

func TestDenialAskRepeatedIDIsInvalid(t *testing.T) {
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	if _, err := desk.open("dup"); err != nil {
		t.Fatalf("open: %v", err)
	}
	requireDenial(t, desk.ask(context.Background(), contracts.ApprovalRequest{ID: "dup", Tool: "Bash"}), DenialInvalid)
}

func TestDenialAskOnClosedSinkIsStreamClosed(t *testing.T) {
	sink := &frameSink{stream: healthyStream()}
	sink.close()
	desk := newApprovalDesk(sink, func() {})
	requireDenial(t, desk.ask(context.Background(), contracts.ApprovalRequest{ID: "closed", Tool: "Bash"}), DenialStreamClosed)
}

func TestDenialAskRefusedSendIsSendFailed(t *testing.T) {
	stream := &scriptedServerStream{sendErr: errSendRefused}
	desk := newApprovalDesk(&frameSink{stream: stream}, func() {})
	requireDenial(t, desk.ask(context.Background(), contracts.ApprovalRequest{ID: "unsent", Tool: "Bash"}), DenialSendFailed)
}

func TestDenialAskAfterAnswersEndIsStreamClosed(t *testing.T) {
	stream := &scriptedServerStream{recvErr: io.EOF}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	desk := newApprovalDesk(&frameSink{stream: stream}, cancel)
	desk.readAnswers(stream)
	requireDenial(t, desk.ask(ctx, contracts.ApprovalRequest{ID: "ended", Tool: "Bash"}), DenialStreamClosed)
}

func TestDenialAskOnCancelledTurnIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	requireDenial(t, desk.ask(ctx, contracts.ApprovalRequest{ID: "cut", Tool: "Bash"}), DenialCancelled)
}

func TestDenialAskPastDeadlineIsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	requireDenial(t, desk.ask(ctx, contracts.ApprovalRequest{ID: "late", Tool: "Bash"}), DenialDeadline)
}

func TestDenialDecideWithoutAskerIsNoAsker(t *testing.T) {
	turn := newApprovalTurn(func() {}, nil)
	requireDenial(t, turn.decide(context.Background(), contracts.ApprovalRequest{ID: "n1", Tool: "Bash"}), DenialNoAsker)
}

func TestDenialDecideAnswerWithOtherIDIsInvalid(t *testing.T) {
	asker := func(_ context.Context, _ contracts.ApprovalRequest) contracts.ApprovalAnswer {
		return contracts.ApprovalAnswer{ID: "other", Decision: contracts.DecisionAllow}
	}
	turn := newApprovalTurn(func() {}, asker)
	requireDenial(t, turn.decide(context.Background(), contracts.ApprovalRequest{ID: "r1", Tool: "Bash"}), DenialInvalid)
}

func TestDenialDecideCancelledByAskerIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	asker := func(_ context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
		cancel()
		return contracts.ApprovalAnswer{ID: req.ID, Decision: contracts.DecisionAllow}
	}
	turn := newApprovalTurn(cancel, asker)
	requireDenial(t, turn.decide(ctx, contracts.ApprovalRequest{ID: "r2", Tool: "Bash"}), DenialCancelled)
}

func TestDenialDecideAnswerAfterDeadlineIsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	asker := func(askCtx context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
		<-askCtx.Done()
		return contracts.ApprovalAnswer{ID: req.ID, Decision: contracts.DecisionAllow}
	}
	turn := newApprovalTurn(cancel, asker)
	requireDenial(t, turn.decide(ctx, contracts.ApprovalRequest{ID: "r3", Tool: "Bash"}), DenialDeadline)
}

func TestDenialReplyOversizedAnswerIsTooLarge(t *testing.T) {
	oversizedReason := strings.Repeat("r", maxApprovalBytes)
	asker := func(_ context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
		return contracts.ApprovalAnswer{ID: req.ID, Decision: contracts.DecisionAllow, Reason: oversizedReason}
	}
	turn := newApprovalTurn(func() {}, asker)
	stream := &recordingClientStream{}
	turn.stream = stream
	turn.reply(context.Background(), contracts.ApprovalRequest{ID: "big", Tool: "Bash"})
	if len(stream.sent) != 1 {
		t.Fatalf("sent %d answers, want 1", len(stream.sent))
	}
	var answer contracts.ApprovalAnswer
	if err := Unmarshal(stream.sent[0].JsonPayload, &answer); err != nil {
		t.Fatalf("decode the answer sent back: %v", err)
	}
	requireDenial(t, answer, DenialTooLarge)
}

func TestDenialNoAskerReachesBackend(t *testing.T) {
	backend := newAskingBackend(contracts.ApprovalRequest{ID: "n1", Tool: "Bash", Subject: "ls"})
	proxy := serveAsking(t, backend)

	if _, err := proxy.RespondWithApprovals(context.Background(), contracts.Prompt{}, nil); err != nil {
		t.Fatalf("RespondWithApprovals: %v", err)
	}
	got := receiveResult(t, backend.out)
	requireDenial(t, got.Answer, DenialNoAsker)
}

func TestDenialReasonKeysAreDistinctCatalogKeys(t *testing.T) {
	seen := make(map[string]bool)
	for _, reason := range DenialReasons() {
		key := reason.Key()
		if seen[key] || !strings.HasPrefix(key, "approval.refused.") || strings.Contains(key, "unknown") {
			t.Fatalf("denial reason %d has key %q, which is repeated or outside the set", reason, key)
		}
		seen[key] = true
	}
	if len(seen) != 8 {
		t.Fatalf("%d distinct keys, want 8", len(seen))
	}
}
