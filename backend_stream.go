package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	contracts "github.com/Herrscherd/herrscher-contracts"
	"google.golang.org/grpc"

	pb "github.com/Herrscherd/herrscher-transport/proto"
)

// PortBackend names the model edge on the wire. Unlike memory/orchestrator
// (request/response over the generic Plugin.Call), the backend streams turn
// events, so it rides a dedicated server-streaming service (BackendStream).
const PortBackend = "backend"

const backendRespondFullMethod = "/herrscher.transport.v1.BackendStream/Respond"

const respondMethod = "Respond"

type backendFrame struct {
	Event *contracts.BackendEvent `json:"event,omitempty"`
	Done  bool                    `json:"done,omitempty"`
	Reply string                  `json:"reply,omitempty"`
	Error string                  `json:"error,omitempty"`
}

func errString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

type backendStreamSrv interface {
	respond(req *pb.MethodEnvelope, stream grpc.ServerStream) error
}

func backendRespondHandler(srv any, stream grpc.ServerStream) error {
	req := new(pb.MethodEnvelope)
	if err := stream.RecvMsg(req); err != nil {
		return err
	}
	handler, ok := srv.(backendStreamSrv)
	if !ok {
		return errBackendStreamNotResponder
	}
	return handler.respond(req, stream)
}

var errBackendStreamNotResponder = errors.New("backend stream: server does not respond")

func backendStreamServiceDesc() grpc.ServiceDesc {
	return grpc.ServiceDesc{
		ServiceName: "herrscher.transport.v1.BackendStream",
		HandlerType: (*backendStreamSrv)(nil),
		Streams: []grpc.StreamDesc{
			{StreamName: respondMethod, Handler: backendRespondHandler, ServerStreams: true},
		},
		Metadata: "backend_stream.go",
	}
}

type backendServer struct{ real contracts.Backend }

// RegisterBackendSkeleton wires a real Backend behind the streaming service so a
// remote host can serve the model edge. The skeleton forwards every onEvent as a
// frame and closes the stream with the terminal reply frame.
func RegisterBackendSkeleton(server *grpc.Server, impl contracts.Backend) {
	desc := backendStreamServiceDesc()
	server.RegisterService(&desc, &backendServer{real: impl})
}

func (s *backendServer) respond(req *pb.MethodEnvelope, stream grpc.ServerStream) error {
	if req.Port != PortBackend {
		return &UnknownPortError{Port: req.Port}
	}
	var prompt contracts.Prompt
	if err := decodeArgs(req.JsonPayload, &prompt); err != nil {
		return fmt.Errorf("transport: decode backend prompt: %w", err)
	}

	var (
		sendLock sync.Mutex
		sendErr  error
		closed   bool
	)
	onEvent := func(event contracts.BackendEvent) {
		sendLock.Lock()
		defer sendLock.Unlock()
		if closed || sendErr != nil {
			return
		}
		ev := event
		payload, err := Marshal(backendFrame{Event: &ev})
		if err != nil {
			sendErr = err
			return
		}
		if err := stream.SendMsg(&pb.ResultEnvelope{JsonPayload: payload}); err != nil {
			sendErr = err
		}
	}

	reply, respErr := s.real.Respond(stream.Context(), prompt, onEvent)
	sendLock.Lock()
	closed = true
	streamErr := sendErr
	sendLock.Unlock()
	if streamErr != nil {
		return fmt.Errorf("transport: backend event stream: %w", streamErr)
	}
	payload, err := Marshal(backendFrame{Done: true, Reply: reply, Error: errString(respErr)})
	if err != nil {
		return fmt.Errorf("transport: encode backend reply: %w", err)
	}
	if err := stream.SendMsg(&pb.ResultEnvelope{JsonPayload: payload}); err != nil {
		return fmt.Errorf("transport: send backend reply: %w", err)
	}
	return nil
}

// BackendProxy is a contracts.Backend backed by a remote streaming backend over
// gRPC. It holds the raw connection (NewStream needs a ClientConnInterface)
// rather than the generated unary client the other proxies use.
type BackendProxy struct {
	cc     grpc.ClientConnInterface
	dialed io.Closer
}

var _ contracts.Backend = (*BackendProxy)(nil)

// NewBackendProxy builds a proxy over an established client connection.
func NewBackendProxy(cc grpc.ClientConnInterface) *BackendProxy { return &BackendProxy{cc: cc} }

// Respond runs one turn on the remote backend: it opens the stream, forwards the
// prompt, replays each event through onEvent in order, and returns the reply at
// the terminal Done frame. A stream that ends before the Done frame (a remote
// crash or a dropped connection mid-turn) surfaces as an error so the host's
// turn loop abandons the in-flight turn (the hangup path), never hanging.
// It returns ErrInvalidFrame when a frame is neither an event nor the Done frame,
// and a remote error when the Done frame carries an error.
func (p *BackendProxy) Respond(ctx context.Context, prompt contracts.Prompt, onEvent func(contracts.BackendEvent)) (string, error) {
	payload, err := encodeArgs(prompt)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	respondStream, err := openRespondStream(ctx, p.cc, payload)
	if err != nil {
		return "", err
	}
	for {
		frame := new(pb.ResultEnvelope)
		if err := respondStream.RecvMsg(frame); err != nil {
			return "", receiveFailure(err)
		}
		if outcome := applyBackendFrame(frame, onEvent); outcome.terminal {
			return outcome.reply, outcome.err
		}
	}
}

func openRespondStream(ctx context.Context, cc grpc.ClientConnInterface, payload []byte) (grpc.ClientStream, error) {
	stream, err := cc.NewStream(ctx, &grpc.StreamDesc{StreamName: respondMethod, ServerStreams: true}, backendRespondFullMethod)
	if err != nil {
		return nil, fmt.Errorf("backend: open stream: %w", err)
	}
	if err := stream.SendMsg(&pb.MethodEnvelope{Port: PortBackend, Method: respondMethod, JsonPayload: payload}); err != nil {
		return nil, fmt.Errorf("backend: send prompt: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("backend: close send: %w", err)
	}
	return stream, nil
}

func receiveFailure(err error) error {
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("backend: stream ended before reply{done}: %w", io.ErrUnexpectedEOF)
	}
	return fmt.Errorf("backend: remote stream: %w", err)
}

type frameOutcome struct {
	reply    string
	terminal bool
	err      error
}

func applyBackendFrame(frame *pb.ResultEnvelope, onEvent func(contracts.BackendEvent)) frameOutcome {
	var decoded backendFrame
	if err := Unmarshal(frame.JsonPayload, &decoded); err != nil {
		return frameOutcome{terminal: true, err: fmt.Errorf("backend: decode frame: %w", err)}
	}
	if decoded.Event != nil {
		if onEvent != nil {
			onEvent(*decoded.Event)
		}
		return frameOutcome{}
	}
	if !decoded.Done {
		return frameOutcome{terminal: true, err: ErrInvalidFrame}
	}
	if decoded.Error != "" {
		return frameOutcome{reply: decoded.Reply, terminal: true, err: &remoteError{message: decoded.Error}}
	}
	return frameOutcome{reply: decoded.Reply, terminal: true}
}

// Close releases the client-side gRPC connection only. It never closes the
// remote backend: the plugin-host owns that process and may serve other clients,
// so one bridge shutting down must not tear the shared backend down.
func (p *BackendProxy) Close() error {
	if p.dialed != nil {
		return p.dialed.Close()
	}
	return nil
}
