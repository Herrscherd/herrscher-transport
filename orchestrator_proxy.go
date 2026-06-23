package transport

import (
	"context"
	"errors"
	"io"

	contracts "github.com/Herrscherd/herrscher-contracts"
	pb "github.com/Herrscherd/herrscher-transport/proto"
)

// OrchestratorProxy is a contracts.Orchestrator backed by a remote plugin over
// gRPC, mirroring MemoryProxy.
type OrchestratorProxy struct {
	client pb.PluginClient
	conn   io.Closer // dialed connection, released by Close; nil when the client is supplied directly
}

var _ contracts.Orchestrator = (*OrchestratorProxy)(nil)

// NewOrchestratorProxy builds a proxy over an established Plugin client.
func NewOrchestratorProxy(c pb.PluginClient) *OrchestratorProxy { return &OrchestratorProxy{client: c} }

func (p *OrchestratorProxy) call(ctx context.Context, method string, args ...any) (*pb.ResultEnvelope, error) {
	payload, err := Marshal(args)
	if err != nil {
		return nil, err
	}
	res, err := p.client.Call(ctx, &pb.MethodEnvelope{Port: PortOrchestrator, Method: method, JsonPayload: payload})
	if err != nil {
		return nil, err
	}
	if res.Error != "" {
		return nil, errors.New(res.Error)
	}
	return res, nil
}

// Context primes the next turn. Per the Orchestrator contract it never returns a
// turn-breaking error: any transport or remote failure degrades to "".
func (p *OrchestratorProxy) Context(ctx context.Context) string {
	res, err := p.call(ctx, "Context")
	if err != nil {
		return ""
	}
	var out string
	if err := decodeArgs(res.JsonPayload, &out); err != nil {
		return ""
	}
	return out
}

// Observe records a completed turn. The remote error is surfaced for logging;
// the host never breaks the loop on it.
func (p *OrchestratorProxy) Observe(ctx context.Context, pr contracts.Prompt, reply string) error {
	_, err := p.call(ctx, "Observe", pr, reply)
	return err
}

// Consolidate drives proactive curation out of band on the remote orchestrator.
func (p *OrchestratorProxy) Consolidate(ctx context.Context) error {
	_, err := p.call(ctx, "Consolidate")
	return err
}

// Close releases the client-side gRPC connection only. Unlike Memory (shared
// across clients by the plugin-host), an Orchestrator is session-scoped, so
// tearing down the remote object is the owning host's job at session shutdown —
// not something a closing proxy should trigger over the wire.
func (p *OrchestratorProxy) Close() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
