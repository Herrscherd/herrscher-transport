package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

const (
	backendRespondWithApprovalsFullMethod = "/herrscher.transport.v1.BackendStream/RespondWithApprovals"
	respondWithApprovalsMethod            = "RespondWithApprovals"
	approvalAnswerMethod                  = "answer"
	maxApprovalBytes                      = 64 << 10
	maxInflightApprovals                  = 8
)

var errStreamClosed = errors.New("transport: approval stream closed")

type backendApprovalSrv interface {
	respondWithApprovals(req *pb.MethodEnvelope, stream grpc.ServerStream) error
}

func backendRespondWithApprovalsStream() grpc.StreamDesc {
	return grpc.StreamDesc{
		StreamName:    respondWithApprovalsMethod,
		Handler:       backendRespondWithApprovalsHandler,
		ServerStreams: true,
		ClientStreams: true,
	}
}

func backendRespondWithApprovalsHandler(srv any, stream grpc.ServerStream) error {
	return receiveThen(stream, func(req *pb.MethodEnvelope) error {
		handler, ok := srv.(backendApprovalSrv)
		if !ok {
			return errBackendStreamNotResponder
		}
		return handler.respondWithApprovals(req, stream)
	})
}

func (s *backendServer) respondWithApprovals(req *pb.MethodEnvelope, stream grpc.ServerStream) error {
	if req.Port != PortBackend {
		return &UnknownPortError{Port: req.Port}
	}
	if req.Method != respondWithApprovalsMethod {
		return &UnknownMethodError{Service: backendServiceName, Method: req.Method}
	}
	var prompt contracts.Prompt
	if err := decodeArgs(req.JsonPayload, &prompt); err != nil {
		return fmt.Errorf("transport: decode backend prompt: %w", err)
	}

	turn, cancel := context.WithCancel(stream.Context())
	sink := &frameSink{stream: stream}
	defer sink.close()
	defer cancel()

	desk := newApprovalDesk(sink)
	go desk.readAnswers(stream, cancel)

	reply, respErr := s.real.Respond(contracts.WithApprovalAsker(turn, desk.ask), prompt, func(event contracts.BackendEvent) {
		if err := sink.send(backendFrame{Event: &event}); err != nil {
			cancel()
		}
	})
	failure := desk.failure()
	cancel()

	sendErr := sink.send(backendFrame{Done: true, Reply: reply, Error: errString(respErr)})
	if err := errors.Join(sendErr, failure); err != nil {
		return fmt.Errorf("transport: backend approval turn: %w", err)
	}
	return nil
}

type frameSink struct {
	mu     sync.Mutex
	stream grpc.ServerStream
	err    error
	closed bool
}

func (s *frameSink) send(frame backendFrame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.closed {
		return errStreamClosed
	}
	payload, err := Marshal(frame)
	if err == nil {
		err = s.stream.SendMsg(&pb.ResultEnvelope{JsonPayload: payload})
	}
	if err != nil {
		s.err = err
	}
	return err
}

func (s *frameSink) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

type approvalDesk struct {
	sink    *frameSink
	mu      sync.Mutex
	pending map[string]chan contracts.ApprovalAnswer
	failed  error
}

func newApprovalDesk(sink *frameSink) *approvalDesk {
	return &approvalDesk{sink: sink, pending: make(map[string]chan contracts.ApprovalAnswer)}
}

func (d *approvalDesk) ask(ctx context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
	denied := contracts.ApprovalAnswer{ID: req.ID, Decision: contracts.DecisionDeny}
	if req.ID == "" || checkApprovalBound(req) != nil {
		return denied
	}
	waiter, err := d.open(req.ID)
	if err != nil {
		return denied
	}
	defer d.forget(req.ID, waiter)
	if err := d.sink.send(backendFrame{Approval: &req}); err != nil {
		return denied
	}
	select {
	case answer := <-waiter:
		return answer
	case <-ctx.Done():
		return denied
	}
}

func (d *approvalDesk) open(requestID string) (chan contracts.ApprovalAnswer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.pending) >= maxInflightApprovals {
		return nil, ErrTooManyApprovals
	}
	if _, busy := d.pending[requestID]; busy {
		return nil, ErrInvalidApproval
	}
	waiter := make(chan contracts.ApprovalAnswer, 1)
	d.pending[requestID] = waiter
	return waiter, nil
}

func (d *approvalDesk) forget(requestID string, waiter chan contracts.ApprovalAnswer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pending[requestID] == waiter {
		delete(d.pending, requestID)
	}
}

func (d *approvalDesk) readAnswers(stream grpc.ServerStream, cancel context.CancelFunc) {
	defer cancel()
	for {
		msg := new(pb.MethodEnvelope)
		if err := stream.RecvMsg(msg); err != nil {
			d.fail(fmt.Errorf("transport: approval answers ended: %w", err))
			return
		}
		if err := d.accept(msg); err != nil {
			d.fail(err)
			return
		}
	}
}

func (d *approvalDesk) accept(msg *pb.MethodEnvelope) error {
	if msg.Port != PortBackend || msg.Method != approvalAnswerMethod {
		return fmt.Errorf("%w: %s.%s", ErrInvalidApproval, msg.Port, msg.Method)
	}
	if len(msg.JsonPayload) > maxApprovalBytes {
		return ErrApprovalTooLarge
	}
	var answer contracts.ApprovalAnswer
	if err := Unmarshal(msg.JsonPayload, &answer); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidApproval, err)
	}
	return d.deliver(answer)
}

func (d *approvalDesk) deliver(answer contracts.ApprovalAnswer) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	waiter, ok := d.pending[answer.ID]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownApproval, answer.ID)
	}
	delete(d.pending, answer.ID)
	if !answer.Allowed() {
		answer.Decision = contracts.DecisionDeny
	}
	waiter <- answer
	return nil
}

func (d *approvalDesk) fail(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failed == nil {
		d.failed = err
	}
}

func (d *approvalDesk) failure() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.failed
}

// RespondWithApprovals runs one turn on a remote backend that may ask the daemon to
// approve tools. It behaves as Respond does, and in addition each approval request
// the backend raises is answered by the ApprovalAsker the context carries (see
// contracts.WithApprovalAsker). A context without an asker answers every request
// with a denial. It returns ErrApprovalStreamUnsupported, and runs no turn, when the
// remote server predates the approval stream.
func (p *BackendProxy) RespondWithApprovals(ctx context.Context, prompt contracts.Prompt, onEvent func(contracts.BackendEvent)) (string, error) {
	payload, err := encodeArgs(prompt)
	if err != nil {
		return "", err
	}
	turnCtx, cancel := context.WithCancel(ctx)
	asker, _ := contracts.ApprovalAskerFrom(turnCtx)
	turn := &approvalTurn{cancel: cancel, asker: asker}
	defer turn.close()

	stream, err := openApprovalStream(turnCtx, p.cc, payload)
	if err != nil {
		return "", err
	}
	turn.stream = stream
	onRequest := func(req contracts.ApprovalRequest) error {
		return turn.onRequest(turnCtx, req)
	}
	for {
		frame := new(pb.ResultEnvelope)
		if err := stream.RecvMsg(frame); err != nil {
			return "", turn.settle(approvalReceiveFailure(err))
		}
		if outcome := applyTurnFrame(frame, onEvent, onRequest); outcome.terminal {
			return outcome.reply, turn.settle(outcome.err)
		}
	}
}

func openApprovalStream(ctx context.Context, cc grpc.ClientConnInterface, payload []byte) (grpc.ClientStream, error) {
	stream, err := cc.NewStream(ctx, &grpc.StreamDesc{StreamName: respondWithApprovalsMethod, ServerStreams: true, ClientStreams: true}, backendRespondWithApprovalsFullMethod)
	if err != nil {
		return nil, fmt.Errorf("backend: open approval stream: %w", err)
	}
	if err := stream.SendMsg(&pb.MethodEnvelope{Port: PortBackend, Method: respondWithApprovalsMethod, JsonPayload: payload}); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, approvalReceiveFailure(streamStatus(stream))
		}
		return nil, fmt.Errorf("backend: send prompt: %w", err)
	}
	return stream, nil
}

func streamStatus(stream grpc.ClientStream) error {
	if err := stream.RecvMsg(new(pb.ResultEnvelope)); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}

func approvalReceiveFailure(err error) error {
	if status.Code(err) == codes.Unimplemented {
		return fmt.Errorf("backend: approval stream: %w", ErrApprovalStreamUnsupported)
	}
	return receiveFailure(err)
}

type approvalTurn struct {
	cancel  context.CancelFunc
	stream  grpc.ClientStream
	asker   contracts.ApprovalAsker
	sendMu  sync.Mutex
	workers sync.WaitGroup
	mu      sync.Mutex
	pending int
	settled bool
	failed  error
}

func (t *approvalTurn) onRequest(ctx context.Context, req contracts.ApprovalRequest) error {
	if req.ID == "" {
		return ErrInvalidApproval
	}
	if err := checkApprovalBound(req); err != nil {
		return err
	}
	if !t.begin() {
		return ErrTooManyApprovals
	}
	t.workers.Add(1)
	go func() {
		defer t.workers.Done()
		defer t.finish()
		t.reply(ctx, req)
	}()
	return nil
}

func (t *approvalTurn) begin() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending >= maxInflightApprovals {
		return false
	}
	t.pending++
	return true
}

func (t *approvalTurn) finish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending--
}

func (t *approvalTurn) reply(ctx context.Context, req contracts.ApprovalRequest) {
	payload, err := Marshal(t.decide(ctx, req))
	if err != nil {
		t.fail(fmt.Errorf("backend: encode approval answer: %w", err))
		return
	}
	t.sendMu.Lock()
	err = t.stream.SendMsg(&pb.MethodEnvelope{Port: PortBackend, Method: approvalAnswerMethod, JsonPayload: payload})
	t.sendMu.Unlock()
	if err != nil {
		t.fail(fmt.Errorf("backend: send approval answer: %w", err))
	}
}

func (t *approvalTurn) decide(ctx context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
	denied := contracts.ApprovalAnswer{ID: req.ID, Decision: contracts.DecisionDeny}
	if t.asker == nil {
		return denied
	}
	answer := t.asker(ctx, req)
	if answer.ID != req.ID || checkApprovalBound(answer) != nil {
		return denied
	}
	if !answer.Allowed() {
		answer.Decision = contracts.DecisionDeny
	}
	return answer
}

func (t *approvalTurn) fail(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.settled || t.failed != nil {
		return
	}
	t.failed = err
	t.cancel()
}

func (t *approvalTurn) settle(err error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.settled = true
	return errors.Join(err, t.failed)
}

func (t *approvalTurn) close() {
	t.cancel()
	t.workers.Wait()
}

func checkApprovalBound(value any) error {
	payload, err := Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidApproval, err)
	}
	if len(payload) > maxApprovalBytes {
		return ErrApprovalTooLarge
	}
	return nil
}
