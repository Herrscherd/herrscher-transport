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

Each port has a symmetric pair. `RegisterMemorySkeleton(s, real)`, and its
`Orchestrator` and `Backend` equivalents, put a real port object behind the gRPC
service. `DialMemory(ctx, entry, creds)` returns a `contracts.Memory` proxy that
owns its connection and releases it on `Close`.

Adding a method never touches the `.proto`. The method name and a JSON argument
tuple ride inside `MethodEnvelope`.

`ResultEnvelope.error` is an in-band domain error. A peer being down surfaces as
the gRPC error instead.

## Discovery and trust

A plugin publishes an `Announcement` (its manifest, gRPC address and instance id)
on `plugins.announce`, and `WatchAnnouncements` feeds a `RemoteRegistry`.

NATS core pub-sub has no replay, so the publisher is expected to re-announce
periodically. This module only publishes what it is told to.

The announce bus is unauthenticated, so both trust decisions fail closed.
`NewRemoteRegistry(WithAddrAllow(fn))` drops announcements whose address the
allowlist rejects, and dialing with `nil` credentials is refused for any
non-loopback address.

`TLSConfig` requires CA, cert and key together, which is mutual TLS. Any partial
set is an error rather than a silent downgrade to plaintext.

## Further reading

- [Herrscher docs](https://github.com/Herrscherd/herrscher-docs), page
  `architecture/transport`
- [contracts](https://github.com/Herrscherd/herrscher-contracts), for the port
  signatures
