package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

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
	approvalWorkerGrace                   = 500 * time.Millisecond
)

var (
	errStreamClosed = errors.New("transport: approval stream closed")
	errSendFailed   = errors.New("transport: approval send failed")
)

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

	desk := newApprovalDesk(sink, cancel)
	go desk.readAnswers(stream)

	reply, respErr := s.real.Respond(contracts.WithApprovalAsker(turn, desk.ask), prompt, func(event contracts.BackendEvent) {
		if err := sink.send(backendFrame{Event: &event}); err != nil {
			cancel()
		}
	})
	failure := desk.failure()
	cancel()

	turnErr := errors.Join(respErr, failure)
	sendErr := sink.send(backendFrame{Done: true, Reply: reply, Error: errString(turnErr), Wire: encodeWireError(turnErr)})
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
	payload, err := Marshal(frame)
	if err != nil {
		return err
	}
	return s.sendPayload(payload)
}

func (s *frameSink) sendPayload(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.closed {
		return errStreamClosed
	}
	if err := s.stream.SendMsg(&pb.ResultEnvelope{JsonPayload: payload}); err != nil {
		s.err = fmt.Errorf("%w: %w", errSendFailed, err)
		return s.err
	}
	return nil
}

func (s *frameSink) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

type approvalDesk struct {
	sink    *frameSink
	cancel  context.CancelFunc
	mu      sync.Mutex
	pending map[string]chan contracts.ApprovalAnswer
	failed  error
}

func newApprovalDesk(sink *frameSink, cancel context.CancelFunc) *approvalDesk {
	return &approvalDesk{sink: sink, cancel: cancel, pending: make(map[string]chan contracts.ApprovalAnswer)}
}

func (d *approvalDesk) ask(ctx context.Context, req contracts.ApprovalRequest) contracts.ApprovalAnswer {
	if req.ID == "" {
		d.abort(ErrInvalidApproval)
		return d.refuse(req.ID, contracts.RefusalInvalid)
	}
	payload, err := marshalWithin(backendFrame{Approval: &req})
	if err != nil {
		d.abort(err)
		return d.refuse(req.ID, refusalFor(err))
	}
	waiter, err := d.open(req.ID)
	if err != nil {
		d.abort(err)
		return d.refuse(req.ID, refusalFor(err))
	}
	defer d.forget(req.ID, waiter)
	if err := d.sink.sendPayload(payload); err != nil {
		d.abort(err)
		return d.refuse(req.ID, refusalFor(err))
	}
	select {
	case answer := <-waiter:
		return answer
	case <-ctx.Done():
		return d.refuse(req.ID, refusalFor(d.cause(ctx)))
	}
}

func (d *approvalDesk) abort(err error) {
	d.fail(err)
	d.cancel()
}

func (d *approvalDesk) refuse(requestID string, refusal contracts.ApprovalRefusal) contracts.ApprovalAnswer {
	answer, err := refusedAnswer(requestID, refusal)
	if err != nil {
		d.abort(err)
	}
	return answer
}

func (d *approvalDesk) open(requestID string) (chan contracts.ApprovalAnswer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, busy := d.pending[requestID]; busy {
		return nil, fmt.Errorf("%w: %q", ErrInvalidApproval, requestID)
	}
	if len(d.pending) >= maxInflightApprovals {
		return nil, ErrTooManyApprovals
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

func (d *approvalDesk) readAnswers(stream grpc.ServerStream) {
	defer d.cancel()
	for {
		msg := new(pb.MethodEnvelope)
		if err := stream.RecvMsg(msg); err != nil {
			d.fail(fmt.Errorf("%w: %w", errStreamClosed, err))
			return
		}
		if err := d.accept(msg); err != nil {
			d.abort(err)
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
	if !validDecision(answer.Decision) {
		refused, err := refusedAnswer(answer.ID, contracts.RefusalInvalid)
		if err != nil {
			return err
		}
		answer = refused
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

func (d *approvalDesk) cause(ctx context.Context) error {
	if err := d.failure(); err != nil {
		return err
	}
	return ctx.Err()
}

// RespondWithApprovals runs one turn on a remote backend that may ask the daemon to
// approve tools. Its events and its reply are those of Respond. Each approval request
// the backend raises is answered by the ApprovalAsker the context carries (see
// contracts.WithApprovalAsker), and a context without an asker denies every request with
// contracts.RefusalNoAsker. Each refusal the transport makes is a denial that names its cause
// in ApprovalAnswer.Refusal and carries no Reason, except a denial the asker chooses, which
// keeps the reason the asker gives, possibly empty.
// The 64 KiB bound applies to approval frames only. The client decodes the whole
// frame first and checks its size before the asker sees it, so the bound does not
// limit decoding: gRPC's 4 MiB default receive limit does. An asker must return once
// its context is cancelled: every turn waits at most 500 ms for its askers at its
// end, a turn that ends with an error then reports ErrApprovalWorkerBlocked, and a
// turn that ends with a successful final frame never reports it. It returns
// ErrApprovalStreamUnsupported, and runs no turn, when the remote server predates the
// approval stream.
func (p *BackendProxy) RespondWithApprovals(ctx context.Context, prompt contracts.Prompt, onEvent func(contracts.BackendEvent)) (string, error) {
	payload, err := encodeArgs(prompt)
	if err != nil {
		return "", err
	}
	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	asker, _ := contracts.ApprovalAskerFrom(turnCtx)
	turn := newApprovalTurn(cancel, asker)

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
			return "", turn.end(approvalReceiveFailure(err))
		}
		if outcome := applyTurnFrame(frame, onEvent, onRequest); outcome.terminal {
			return outcome.reply, turn.end(outcome.err)
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
	cancel   context.CancelFunc
	stream   grpc.ClientStream
	asker    contracts.ApprovalAsker
	sendMu   sync.Mutex
	workers  sync.WaitGroup
	mu       sync.Mutex
	inflight map[string]struct{}
	settled  bool
	failed   error
}

func newApprovalTurn(cancel context.CancelFunc, asker contracts.ApprovalAsker) *approvalTurn {
	return &approvalTurn{cancel: cancel, asker: asker, inflight: make(map[string]struct{})}
}

func (t *approvalTurn) onRequest(ctx context.Context, req contracts.ApprovalRequest) error {
	if req.ID == "" {
		return ErrInvalidApproval
	}
	if err := t.begin(req.ID); err != nil {
		return err
	}
	t.workers.Add(1)
	go func() {
		defer t.workers.Done()
		defer t.finish(req.ID)
		t.reply(ctx, req)
	}()
	return nil
}

func (t *approvalTurn) begin(requestID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, busy := t.inflight[requestID]; busy {
		return fmt.Errorf("%w: %q", ErrInvalidApproval, requestID)
	}
	if len(t.inflight) >= maxInflightApprovals {
		return ErrTooManyApprovals
	}
	t.inflight[requestID] = struct{}{}
	return nil
}

func (t *approvalTurn) finish(requestID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inflight, requestID)
}

func (t *approvalTurn) reply(ctx context.Context, req contracts.ApprovalRequest) {
	t.sendAnswer(t.decide(ctx, req))
}

func (t *approvalTurn) sendAnswer(answer contracts.ApprovalAnswer) {
	payload, err := marshalWithin(answer)
	if errors.Is(err, ErrApprovalTooLarge) {
		payload, err = marshalWithin(t.refuse(answer.ID, contracts.RefusalTooLarge))
	}
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
	if t.asker == nil {
		return t.refuse(req.ID, contracts.RefusalNoAsker)
	}
	answer := t.asker(ctx, req)
	if err := ctx.Err(); err != nil {
		return t.refuse(req.ID, refusalFor(err))
	}
	if answer.ID != req.ID || !validDecision(answer.Decision) || answer.Refusal != "" {
		return t.refuse(req.ID, contracts.RefusalInvalid)
	}
	return answer
}

func (t *approvalTurn) refuse(requestID string, refusal contracts.ApprovalRefusal) contracts.ApprovalAnswer {
	answer, err := refusedAnswer(requestID, refusal)
	if err != nil {
		t.fail(err)
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

func (t *approvalTurn) end(err error) error {
	t.mu.Lock()
	t.settled = true
	failed := t.failed
	t.mu.Unlock()
	t.cancel()
	blocked := t.drain()
	if err == nil && failed == nil {
		return nil
	}
	return errors.Join(err, failed, blocked)
}

func (t *approvalTurn) drain() error {
	drained := make(chan struct{})
	go func() {
		t.workers.Wait()
		close(drained)
	}()
	grace := time.NewTimer(approvalWorkerGrace)
	defer grace.Stop()
	select {
	case <-drained:
		return nil
	case <-grace.C:
		return ErrApprovalWorkerBlocked
	}
}

func validDecision(decision contracts.ApprovalDecision) bool {
	_, err := contracts.ParseApprovalDecision(string(decision))
	return err == nil
}

func marshalWithin(value any) ([]byte, error) {
	payload, err := Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidApproval, err)
	}
	if len(payload) > maxApprovalBytes {
		return nil, ErrApprovalTooLarge
	}
	return payload, nil
}
