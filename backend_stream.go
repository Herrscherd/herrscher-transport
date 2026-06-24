package transport

import (
	"context"
	"errors"
	"fmt"
	"io"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
	"google.golang.org/grpc"
)

// PortBackend names the model edge on the wire. Unlike memory/orchestrator
// (request/response over the generic Plugin.Call), the backend streams turn
// events, so it rides a dedicated server-streaming service (BackendStream).
const PortBackend = "backend"

// backendRespondFullMethod is the gRPC method the proxy dials and the skeleton
// serves. The service is hand-written (no proto regen): it reuses the generated
// MethodEnvelope (request) and ResultEnvelope (each stream frame) so contracts
// stays the sole source of truth for the payload types.
const backendRespondFullMethod = "/herrscher.transport.v1.BackendStream/Respond"

// backendFrame is one item in the response stream, JSON-encoded into a
// ResultEnvelope. The backend's mid-turn events arrive as Event frames in order,
// then exactly one terminal Done frame carries the reply (and Error if Respond
// failed) — the reply{done} turn boundary the host depends on.
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

// backendStreamSrv is the server side of the hand-written BackendStream service.
type backendStreamSrv interface {
	respond(req *pb.MethodEnvelope, stream grpc.ServerStream) error
}

func backendRespondHandler(srv any, stream grpc.ServerStream) error {
	req := new(pb.MethodEnvelope)
	if err := stream.RecvMsg(req); err != nil {
		return err
	}
	return srv.(backendStreamSrv).respond(req, stream)
}

// backendStreamServiceDesc is the hand-authored equivalent of a protoc-generated
// ServiceDesc for a single server-streaming RPC. gRPC registers and dispatches
// it exactly like a generated one.
var backendStreamServiceDesc = grpc.ServiceDesc{
	ServiceName: "herrscher.transport.v1.BackendStream",
	HandlerType: (*backendStreamSrv)(nil),
	Streams: []grpc.StreamDesc{
		{StreamName: "Respond", Handler: backendRespondHandler, ServerStreams: true},
	},
	Metadata: "backend_stream.go",
}

type backendServer struct{ real contracts.Backend }

// RegisterBackendSkeleton wires a real Backend behind the streaming service so a
// remote host can serve the model edge. The skeleton forwards every onEvent as a
// frame and closes the stream with the terminal reply frame.
func RegisterBackendSkeleton(s *grpc.Server, real contracts.Backend) {
	s.RegisterService(&backendStreamServiceDesc, &backendServer{real: real})
}

func (s *backendServer) respond(req *pb.MethodEnvelope, stream grpc.ServerStream) error {
	if req.Port != PortBackend {
		return fmt.Errorf("transport: unknown port %q for backend stream", req.Port)
	}
	var prompt contracts.Prompt
	if err := decodeArgs(req.JsonPayload, &prompt); err != nil {
		return fmt.Errorf("transport: decode backend prompt: %w", err)
	}

	// onEvent forwards each mid-turn event as its own frame, preserving order.
	// A send failure (the client hung up) is latched so later events are dropped
	// and the handler returns the transport error instead of the reply frame.
	var sendErr error
	onEvent := func(e contracts.BackendEvent) {
		if sendErr != nil {
			return
		}
		ev := e
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
	if sendErr != nil {
		return fmt.Errorf("transport: backend event stream: %w", sendErr)
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
	cc   grpc.ClientConnInterface
	conn io.Closer // dialed connection, released by Close; nil when cc is supplied directly
}

var _ contracts.Backend = (*BackendProxy)(nil)

// NewBackendProxy builds a proxy over an established client connection.
func NewBackendProxy(cc grpc.ClientConnInterface) *BackendProxy { return &BackendProxy{cc: cc} }

// Respond runs one turn on the remote backend: it opens the stream, forwards the
// prompt, replays each event through onEvent in order, and returns the reply at
// the terminal Done frame. A stream that ends before the Done frame — a remote
// crash or a dropped connection mid-turn — surfaces as an error so the host's
// turn loop abandons the in-flight turn (the hangup path), never hanging.
func (p *BackendProxy) Respond(ctx context.Context, prompt contracts.Prompt, onEvent func(contracts.BackendEvent)) (string, error) {
	payload, err := encodeArgs(prompt)
	if err != nil {
		return "", err
	}
	cs, err := p.cc.NewStream(ctx, &grpc.StreamDesc{StreamName: "Respond", ServerStreams: true}, backendRespondFullMethod)
	if err != nil {
		return "", fmt.Errorf("backend: open stream: %w", err)
	}
	if err := cs.SendMsg(&pb.MethodEnvelope{Port: PortBackend, Method: "Respond", JsonPayload: payload}); err != nil {
		return "", fmt.Errorf("backend: send prompt: %w", err)
	}
	if err := cs.CloseSend(); err != nil {
		return "", fmt.Errorf("backend: close send: %w", err)
	}
	for {
		frame := new(pb.ResultEnvelope)
		if err := cs.RecvMsg(frame); err != nil {
			if errors.Is(err, io.EOF) {
				return "", fmt.Errorf("backend: stream ended before reply{done}: %w", io.ErrUnexpectedEOF)
			}
			return "", fmt.Errorf("backend: remote stream: %w", err)
		}
		var f backendFrame
		if err := Unmarshal(frame.JsonPayload, &f); err != nil {
			return "", fmt.Errorf("backend: decode frame: %w", err)
		}
		if f.Event != nil {
			if onEvent != nil {
				onEvent(*f.Event)
			}
			continue
		}
		if f.Done {
			if f.Error != "" {
				return f.Reply, errors.New(f.Error)
			}
			return f.Reply, nil
		}
		// A frame that is neither an event nor the terminal Done is a protocol
		// violation; fail loud rather than silently skipping it and waiting for a
		// boundary that may never come.
		return "", fmt.Errorf("backend: invalid frame (neither event nor done)")
	}
}

// Close releases the client-side gRPC connection only. It never closes the
// remote backend: the plugin-host owns that process and may serve other clients,
// so one bridge shutting down must not tear the shared backend down.
func (p *BackendProxy) Close() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
