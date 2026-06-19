# herrscher-transport

**The wire.** This module lets a Herrscher plugin run as a separate process and
still be reached through the same [`contracts`](https://github.com/Herrscherd/herrscher-contracts)
port the in-process plugins satisfy. Synchronous port calls travel over a single
generic gRPC service; plugins announce themselves over NATS so the host can find
them.

- **One generic service, no per-type proto.** Every port method is carried by the
  same `Plugin.Call(MethodEnvelope) → ResultEnvelope`. The method name and a
  JSON-encoded argument tuple ride inside the envelope, so `contracts` stays the
  **sole source of truth** for the data types — adding a method never touches the
  `.proto`.
- **NATS for discovery, gRPC for calls.** NATS is the async announce/event bus;
  gRPC is the request/response path for the unary port methods.
- **The host never knows the difference.** Local (in-proc factory) and remote
  (gRPC proxy) both resolve to a plain `contracts.Memory` (etc.); the plugin code
  is identical either way. Local stays the default — remote is opt-in per category.

> Part of the Herrscher family: [contracts](https://github.com/Herrscherd/herrscher-contracts)
> (the ports) · [herrscher](https://github.com/Herrscherd/herrscher) (the umbrella
> binary: core + daemon + CLI + bridge) ·
> [obsidian-memory](https://github.com/Herrscherd/herrscher-obsidian-memory) (the
> first port carried over this transport).

---

## The envelope

```proto
service Plugin {
  rpc Call(MethodEnvelope) returns (ResultEnvelope);
}

message MethodEnvelope {
  string port = 1;          // e.g. "memory"
  string method = 2;        // e.g. "Recall"
  bytes json_payload = 3;   // JSON-encoded argument tuple
}

message ResultEnvelope {
  bytes json_payload = 1;   // JSON-encoded result tuple
  string error = 2;         // non-empty => the call returned an error
}
```

`error` carries an in-band domain error (the call ran and returned `err`); a
transport-level failure (peer down, deadline) surfaces as the gRPC error from
`Call` itself. The codec is `encoding/json` — the single encode/decode point for
the whole module.

## Proxy and skeleton

Each port has a symmetric pair:

- **Skeleton** (server side): `RegisterMemorySkeleton(s, real)` wires a real
  `contracts.Memory` behind the generic service. `Call` switches on the method,
  decodes the arg tuple, invokes the real object, and encodes the result.
- **Proxy** (client side): `MemoryProxy` *is* a `contracts.Memory`. Each method
  marshals its args, issues `Call`, and decodes the result — so the caller holds
  an ordinary port object. The proxy owns its dialed connection and releases it on
  `Close`.

## Discovery

A plugin process publishes an `Announcement` (its verbatim `contracts.Manifest`,
its gRPC address, a per-process id) on `plugins.announce`, and **re-announces on a
heartbeat** — NATS core pub-sub has no replay, so a host that subscribes later
still converges. `WatchAnnouncements` + `RemoteRegistry` accumulate the live set;
`DialMemory` turns an entry into a proxy.

## Security

Localhost-trust for now: plugins bind `127.0.0.1`, gRPC uses insecure transport
credentials, and NATS runs without auth — the host and its plugins share one
machine. Crossing a machine boundary (mTLS for gRPC, NATS credentials) is a config
flip layered on later; nothing in the wire format changes.

## Status

The `memory` port is carried end to end (unary). Streaming ports (the backend's
event fan-out over `session.<name>.events`) and the gateway/orchestrator categories
extend this same transport.

Remote mode requires a **NATS server** reachable at `$HERRSCHER_NATS` (default
`nats://127.0.0.1:4222`) — neither the host nor a plugin process embeds one (the
embedded server here is test-only). A current limitation: the host resolves a remote
proxy once and pins it to that address, so a plugin restart on a *new* ephemeral port
is not yet auto-recovered within a live session — recovery (stable address or
re-dial on re-announce) is a follow-on.
