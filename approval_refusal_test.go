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

type inboxServerStream struct {
	grpc.ServerStream

	inbox []*pb.MethodEnvelope
}

func (s *inboxServerStream) Context() context.Context { return context.Background() }

func (s *inboxServerStream) SendMsg(any) error { return nil }

func (s *inboxServerStream) RecvMsg(msg any) error {
	if len(s.inbox) == 0 {
		return io.EOF
	}
	envelope, ok := msg.(*pb.MethodEnvelope)
	if !ok {
		return fmt.Errorf("unexpected message %T", msg)
	}
	next := s.inbox[0]
	s.inbox = s.inbox[1:]
	envelope.Port = next.Port
	envelope.Method = next.Method
	envelope.JsonPayload = next.JsonPayload
	return nil
}

func healthyStream() *scriptedServerStream {
	return &scriptedServerStream{}
}

func requireRefusal(t *testing.T, answer contracts.ApprovalAnswer, want contracts.ApprovalRefusal) {
	t.Helper()
	if answer.Allowed() || answer.Decision != contracts.DecisionDeny {
		t.Fatalf("answer = %+v, want a refusal", answer)
	}
	if answer.Refusal != want {
		t.Fatalf("Refusal = %q, want %q", answer.Refusal, want)
	}
	if answer.Reason != "" {
		t.Fatalf("Reason = %q, want empty for a refusal the transport makes", answer.Reason)
	}
}

func TestRefusalAskWithoutIDIsInvalid(t *testing.T) {
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	requireRefusal(t, desk.ask(context.Background(), contracts.ApprovalRequest{Tool: "Bash"}), contracts.RefusalInvalid)
}

func TestRefusalAskOversizedRequestIsTooLarge(t *testing.T) {
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	request := contracts.ApprovalRequest{ID: "huge", Tool: "Bash", Subject: strings.Repeat("s", maxApprovalBytes)}
	requireRefusal(t, desk.ask(context.Background(), request), contracts.RefusalTooLarge)
}

func TestRefusalAskBeyondInflightBoundIsTooMany(t *testing.T) {
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	for index := range maxInflightApprovals {
		if _, err := desk.open(fmt.Sprintf("r%d", index)); err != nil {
			t.Fatalf("request %d within the bound was refused: %v", index, err)
		}
	}
	requireRefusal(t, desk.ask(context.Background(), contracts.ApprovalRequest{ID: "extra", Tool: "Bash"}), contracts.RefusalTooMany)
}

func TestRefusalAskRepeatedIDIsInvalid(t *testing.T) {
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	if _, err := desk.open("dup"); err != nil {
		t.Fatalf("open: %v", err)
	}
	requireRefusal(t, desk.ask(context.Background(), contracts.ApprovalRequest{ID: "dup", Tool: "Bash"}), contracts.RefusalInvalid)
}

func TestRefusalAskOnClosedSinkIsStreamClosed(t *testing.T) {
	sink := &frameSink{stream: healthyStream()}
	sink.close()
	desk := newApprovalDesk(sink, func() {})
	requireRefusal(t, desk.ask(context.Background(), contracts.ApprovalRequest{ID: "closed", Tool: "Bash"}), contracts.RefusalStreamClosed)
}

func TestRefusalAskRefusedSendIsSendFailed(t *testing.T) {
	stream := &scriptedServerStream{sendErr: errSendRefused}
	desk := newApprovalDesk(&frameSink{stream: stream}, func() {})
	requireRefusal(t, desk.ask(context.Background(), contracts.ApprovalRequest{ID: "unsent", Tool: "Bash"}), contracts.RefusalSendFailed)
}

func TestRefusalAskAfterAnswersEndIsStreamClosed(t *testing.T) {
	stream := &scriptedServerStream{recvErr: io.EOF}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	desk := newApprovalDesk(&frameSink{stream: stream}, cancel)
	desk.readAnswers(stream)
	requireRefusal(t, desk.ask(ctx, contracts.ApprovalRequest{ID: "ended", Tool: "Bash"}), contracts.RefusalStreamClosed)
}

func TestRefusalAskOnCancelledTurnIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	requireRefusal(t, desk.ask(ctx, contracts.ApprovalRequest{ID: "cut", Tool: "Bash"}), contracts.RefusalCancelled)
}

func TestRefusalAskPastDeadlineIsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	requireRefusal(t, desk.ask(ctx, contracts.ApprovalRequest{ID: "late", Tool: "Bash"}), contracts.RefusalDeadline)
}

func TestRefusalAskAfterUnclassifiedFailureIsUnclassified(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	desk := newApprovalDesk(&frameSink{stream: healthyStream()}, func() {})
	desk.fail(errors.New("unforeseen failure"))
	requireRefusal(t, desk.ask(ctx, contracts.ApprovalRequest{ID: "odd", Tool: "Bash"}), contracts.RefusalUnclassified)
}

func TestRefusalDecideWithoutAskerIsNoAsker(t *testing.T) {
	turn := newApprovalTurn(func() {}, nil)
	requireRefusal(t, turn.decide(context.Background(), contracts.ApprovalRequest{ID: "n1", Tool: "Bash"}), contracts.RefusalNoAsker)
}

func TestRefusalDecideAnswerWithOtherIDIsInvalid(t *testing.T) {
	asker := func(_ context.Context, _ contracts.ApprovalRequest) contracts.ApprovalAnswer {
		return contracts.ApprovalAnswer{ID: "other", Decision: contracts.DecisionAllow}
	}
	turn := newApprovalTurn(func() {}, asker)
	requireRefusal(t, turn.decide(context.Background(), contracts.ApprovalRequest{ID: "r1", Tool: "Bash"}), contracts.RefusalInvalid)
}

func TestRefusalDecideAnswerCarryingRefusalIsInvalid(t *testing.T) {
	asker := func(_ context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
		return contracts.ApprovalAnswer{ID: req.ID, Decision: contracts.DecisionAllow, Refusal: contracts.RefusalTooMany}
	}
	turn := newApprovalTurn(func() {}, asker)
	requireRefusal(t, turn.decide(context.Background(), contracts.ApprovalRequest{ID: "r6", Tool: "Bash"}), contracts.RefusalInvalid)
}

func TestRefusalDecideCancelledByAskerIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	asker := func(_ context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
		cancel()
		return contracts.ApprovalAnswer{ID: req.ID, Decision: contracts.DecisionAllow}
	}
	turn := newApprovalTurn(cancel, asker)
	requireRefusal(t, turn.decide(ctx, contracts.ApprovalRequest{ID: "r2", Tool: "Bash"}), contracts.RefusalCancelled)
}

func TestRefusalDecideAnswerAfterDeadlineIsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	asker := func(askCtx context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
		<-askCtx.Done()
		return contracts.ApprovalAnswer{ID: req.ID, Decision: contracts.DecisionAllow}
	}
	turn := newApprovalTurn(cancel, asker)
	requireRefusal(t, turn.decide(ctx, contracts.ApprovalRequest{ID: "r3", Tool: "Bash"}), contracts.RefusalDeadline)
}

func TestRefusalReplyOversizedAnswerIsTooLarge(t *testing.T) {
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
	requireRefusal(t, answer, contracts.RefusalTooLarge)
}

func TestRefusalNoAskerReachesBackend(t *testing.T) {
	backend := newAskingBackend(contracts.ApprovalRequest{ID: "n1", Tool: "Bash", Subject: "ls"})
	proxy := serveAsking(t, backend)

	if _, err := proxy.RespondWithApprovals(context.Background(), contracts.Prompt{}, nil); err != nil {
		t.Fatalf("RespondWithApprovals: %v", err)
	}
	got := receiveResult(t, backend.out)
	requireRefusal(t, got.Answer, contracts.RefusalNoAsker)
}

func TestRefusedAnswerCarriesEveryCode(t *testing.T) {
	codes := []contracts.ApprovalRefusal{
		contracts.RefusalInvalid, contracts.RefusalTooLarge, contracts.RefusalTooMany,
		contracts.RefusalNoAsker, contracts.RefusalCancelled, contracts.RefusalDeadline,
		contracts.RefusalStreamClosed, contracts.RefusalSendFailed, contracts.RefusalUnclassified,
	}
	for _, code := range codes {
		t.Run(string(code), func(t *testing.T) {
			answer, err := refusedAnswer("r1", code)
			if err != nil {
				t.Fatalf("refusedAnswer(%q): %v", code, err)
			}
			requireRefusal(t, answer, code)
		})
	}
}

func TestRefusalWithoutCodeFailsClosedAndIsSurfaced(t *testing.T) {
	cancelled := false
	desk := newApprovalDesk(nil, func() { cancelled = true })
	answer := desk.refuse("r1", "")
	if answer.Allowed() || answer.Decision != contracts.DecisionDeny || answer.Refusal != "" {
		t.Fatalf("a refusal without a code must still deny, got %+v", answer)
	}
	if desk.failure() == nil {
		t.Fatal("a refusal that cannot be written must be recorded on the desk")
	}
	if !cancelled {
		t.Fatal("a refusal that cannot be written must end the turn")
	}
}

func TestRefusalUnknownIDOnTheWireIsInvalid(t *testing.T) {
	stream := &inboxServerStream{inbox: []*pb.MethodEnvelope{
		answerEnvelope(t, contracts.ApprovalAnswer{ID: "nope", Decision: contracts.DecisionAllow}),
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	desk := newApprovalDesk(&frameSink{stream: stream}, cancel)
	go desk.readAnswers(stream)
	requireRefusal(t, desk.ask(ctx, contracts.ApprovalRequest{ID: "pending", Tool: "Bash"}), contracts.RefusalInvalid)
}

func TestRefusalWrongMethodOnTheWireIsInvalid(t *testing.T) {
	stream := &inboxServerStream{inbox: []*pb.MethodEnvelope{
		{Port: PortBackend, Method: "bogus"},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	desk := newApprovalDesk(&frameSink{stream: stream}, cancel)
	go desk.readAnswers(stream)
	requireRefusal(t, desk.ask(ctx, contracts.ApprovalRequest{ID: "pending", Tool: "Bash"}), contracts.RefusalInvalid)
}

func TestRefusalOversizedAnswerOnTheWireIsTooLarge(t *testing.T) {
	stream := &inboxServerStream{inbox: []*pb.MethodEnvelope{
		{Port: PortBackend, Method: approvalAnswerMethod, JsonPayload: make([]byte, maxApprovalBytes+1)},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	desk := newApprovalDesk(&frameSink{stream: stream}, cancel)
	go desk.readAnswers(stream)
	requireRefusal(t, desk.ask(ctx, contracts.ApprovalRequest{ID: "pending", Tool: "Bash"}), contracts.RefusalTooLarge)
}

func TestRefusalEverySourceOfTheCauseIsClassified(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want contracts.ApprovalRefusal
	}{
		{"invalid", ErrInvalidApproval, contracts.RefusalInvalid},
		{"unknown ID", ErrUnknownApproval, contracts.RefusalInvalid},
		{"too large", ErrApprovalTooLarge, contracts.RefusalTooLarge},
		{"too many", ErrTooManyApprovals, contracts.RefusalTooMany},
		{"deadline", context.DeadlineExceeded, contracts.RefusalDeadline},
		{"cancelled", context.Canceled, contracts.RefusalCancelled},
		{"stream closed", errStreamClosed, contracts.RefusalStreamClosed},
		{"send failed", errSendFailed, contracts.RefusalSendFailed},
		{"unclassified", errors.New("unclassified"), contracts.RefusalUnclassified},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := refusalFor(tc.err); got != tc.want {
				t.Fatalf("refusalFor(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestRefusalAnswerWithoutDecisionOnTheWireIsInvalid(t *testing.T) {
	desk := newApprovalDesk(nil, func() {})
	waiter, err := desk.open("r1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	envelope := &pb.MethodEnvelope{Port: PortBackend, Method: approvalAnswerMethod, JsonPayload: []byte(`{"ID":"r1","Reason":"peer"}`)}
	if err := desk.accept(envelope); err != nil {
		t.Fatalf("the answer to the pending request must be accepted: %v", err)
	}
	requireRefusal(t, <-waiter, contracts.RefusalInvalid)
}

func TestRefusalKnownCodeOnTheWireIsARefusal(t *testing.T) {
	desk := newApprovalDesk(nil, func() {})
	waiter, err := desk.open("k1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	envelope := &pb.MethodEnvelope{Port: PortBackend, Method: approvalAnswerMethod, JsonPayload: []byte(`{"ID":"k1","Decision":"allow","Refusal":"too_many"}`)}
	if err := desk.accept(envelope); err != nil {
		t.Fatalf("a refusal to the pending request must be accepted: %v", err)
	}
	got := <-waiter
	if got.Allowed() {
		t.Fatalf("an answer carrying a known refusal must not be allowed, got %+v", got)
	}
	requireRefusal(t, got, contracts.RefusalTooMany)
}

func TestRefusalUnknownCodeOnTheWireIsARefusal(t *testing.T) {
	desk := newApprovalDesk(nil, func() {})
	waiter, err := desk.open("u1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	envelope := &pb.MethodEnvelope{Port: PortBackend, Method: approvalAnswerMethod, JsonPayload: []byte(`{"ID":"u1","Decision":"allow","Refusal":"from_a_newer_peer"}`)}
	if err := desk.accept(envelope); err != nil {
		t.Fatalf("a refusal with an unknown code must be accepted: %v", err)
	}
	got := <-waiter
	if got.Allowed() {
		t.Fatalf("an answer carrying an unknown refusal must not be allowed, got %+v", got)
	}
	requireRefusal(t, got, contracts.ApprovalRefusal("from_a_newer_peer"))
}

func TestRefusedAllowIsNotWritten(t *testing.T) {
	stream := &recordingClientStream{}
	turn := newApprovalTurn(func() {}, nil)
	turn.stream = stream
	turn.sendAnswer(contracts.ApprovalAnswer{ID: "w1", Decision: contracts.DecisionAllow, Refusal: contracts.RefusalInvalid})
	if len(stream.sent) != 0 {
		t.Fatalf("an allow carrying a refusal must not be written, %d sent", len(stream.sent))
	}
	if !errors.Is(turn.failed, contracts.ErrRefusedAllow) {
		t.Fatalf("the failed write must end the turn with ErrRefusedAllow, got %v", turn.failed)
	}
}

func TestRefusalExplicitDenyOnTheWireKeepsItsEmptyReason(t *testing.T) {
	desk := newApprovalDesk(nil, func() {})
	waiter, err := desk.open("r2")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := desk.accept(answerEnvelope(t, contracts.ApprovalAnswer{ID: "r2", Decision: contracts.DecisionDeny})); err != nil {
		t.Fatalf("the explicit denial must be accepted: %v", err)
	}
	got := <-waiter
	if got.Decision != contracts.DecisionDeny || got.Reason != "" || got.Refusal != "" {
		t.Fatalf("explicit denial became %+v, want deny with an empty reason and no refusal", got)
	}
}

func TestRefusalDecideWithoutDecisionIsInvalid(t *testing.T) {
	asker := func(_ context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
		return contracts.ApprovalAnswer{ID: req.ID}
	}
	turn := newApprovalTurn(func() {}, asker)
	requireRefusal(t, turn.decide(context.Background(), contracts.ApprovalRequest{ID: "r4", Tool: "Bash"}), contracts.RefusalInvalid)
}

func TestRefusalDecideExplicitDenyKeepsAskersEmptyReason(t *testing.T) {
	asker := func(_ context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
		return contracts.ApprovalAnswer{ID: req.ID, Decision: contracts.DecisionDeny}
	}
	turn := newApprovalTurn(func() {}, asker)
	got := turn.decide(context.Background(), contracts.ApprovalRequest{ID: "r5", Tool: "Bash"})
	if got.Decision != contracts.DecisionDeny || got.Reason != "" || got.Refusal != "" {
		t.Fatalf("asker's denial became %+v, want deny with an empty reason and no refusal", got)
	}
}
