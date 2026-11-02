# herrscher-transport

**The wire between a host and its out-of-process plugins.** It carries
[`contracts`](https://github.com/Herrscherd/herrscher-contracts) ports over
JSON-over-gRPC, with NATS for discovery.

It is a Go library imported by hosts and by plugin processes. It is not an
installable plugin, and it runs no server of its own.

Status: live.

Ports carried: `contracts.Memory` and `contracts.Orchestrator`, unary via
`Plugin.Call`; and `contracts.Backend`, server-streaming via
`BackendStream.Respond`.

## Install

```bash
go get github.com/Herrscherd/herrscher-transport
```

## Configuration

None. This module reads no environment variable.

TLS is passed in as a `TLSConfig` value (`CAFile`, `CertFile`, `KeyFile`), and
NATS as an already-connected `*nats.Conn`.

## Proxy and skeleton

Each port has a symmetric pair. Memory and orchestrator share the generic `Plugin`
service, served by one `*PluginRouter` per gRPC server. Backend has its own
streaming service and registers on the `*grpc.Server` directly:

```go
router, err := transport.NewPluginRouter(s)
if err != nil {
	return err
}
if err := transport.RegisterMemorySkeleton(router, real); err != nil {
	return err
}
transport.RegisterBackendSkeleton(s, backend)
```

`NewPluginRouter` returns `ErrPluginServiceRegistered` when the server already
serves the Plugin service, so call it once per server. `RegisterMemorySkeleton`
and `RegisterOrchestratorSkeleton` return `ErrPortRegistered` when their port is
already registered on the router. `DialMemory(ctx, entry, creds)` returns a
`contracts.Memory` proxy that owns its connection and releases it on `Close`.

Adding a method never touches the `.proto`. The method name and a JSON argument
tuple ride inside `MethodEnvelope`.

`ResultEnvelope.error` is an in-band domain error. A peer being down surfaces as
the gRPC error instead.

## Discovery and trust

A plugin publishes an `Announcement` (its manifest, gRPC address and instance id)
on `plugins.announce`, and `WatchAnnouncements` feeds a `RemoteRegistry`. Both of its hooks are
required: a nil hook is refused with `ErrNilHook`.

NATS core pub-sub has no replay, so the publisher is expected to re-announce
periodically. This module only publishes what it is told to.

The announce bus is unauthenticated. Without `WithAddrAllow`, `RemoteRegistry`
accepts any well formed announcement, so any process that can reach the bus, a
local one included, can announce itself and take over a category.
`NewRemoteRegistry(WithAddrAllow(fn))` drops announcements whose address the
allowlist rejects. Dialing with `nil` credentials is refused for any non-loopback
address.

`TLSConfig` requires CA, cert and key together, which is mutual TLS. Any partial
set is an error rather than a silent downgrade to plaintext.

## Further reading

- [Herrscher docs](https://github.com/Herrscherd/herrscher-docs), page
  `architecture/transport`
- [contracts](https://github.com/Herrscherd/herrscher-contracts), for the port
  signatures
