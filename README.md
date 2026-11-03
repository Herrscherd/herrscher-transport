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

TLS is passed in as a `TLSConfig` value (`CAFile`, `CertFile`, `KeyFile`), NATS
as an already-connected `*nats.Conn`, and the announce secret as an
`AnnounceKey` built by `ParseAnnounceKey`.

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
has a router, so call it once per server, before `Serve`. After `Serve`, grpc
exits the process on the registration instead of returning an error.
`RegisterMemorySkeleton`
and `RegisterOrchestratorSkeleton` return `ErrPortRegistered` when their port is
already registered on the router. `DialMemory(ctx, entry, creds)` returns a
`contracts.Memory` proxy that owns its connection and releases it on `Close`.

Adding a method never touches the `.proto`. The method name and a JSON argument
tuple ride inside `MethodEnvelope`.

`ResultEnvelope.error` is an in-band domain error. A peer being down surfaces as
the gRPC error instead.

## Discovery and trust

A plugin publishes an `Announcement` (its manifest, gRPC address and instance
id) on `plugins.announce` with `Announce(conn, ann, key)`. The announcement is
sealed in an envelope: its body is authenticated with HMAC-SHA256 under an
`AnnounceKey` that the host and its plugin processes share, and stamped with
its issue time. `ParseAnnounceKey` refuses a secret shorter than 32 bytes
(`ErrWeakAnnounceKey`).

`WatchAnnouncements(conn, key, onAnnouncement, onRejected)` delivers only the
announcements whose MAC verifies and whose issue time lies within
`AnnounceWindow` (ten seconds) of the receiver's clock. Every other message goes
to `onRejected`, as `ErrUnauthenticatedAnnouncement` (unsigned, tampered or
signed with another key) or `ErrStaleAnnouncement`. Both hooks are required
(`ErrNilHook`), and so is a non-zero key (`ErrNoAnnounceKey`), on both `Announce`
and `WatchAnnouncements`.

`RemoteRegistry` is a second gate, and it fails closed. `NewRemoteRegistry()`
refuses every announcement until `WithAddrAllow(fn)` supplies an address
allowlist. An address allowlist alone does not identify a plugin: a plugin-host
listens on an ephemeral port by default, and any local process can bind a
loopback port, so a loopback allowlist is never the only gate. The authenticated
key is what identifies a plugin.

NATS core pub-sub has no replay, so the publisher re-announces periodically.
Hosts and plugins on different machines must keep their clocks within
`AnnounceWindow` of each other (NTP).

Residual risks, which the key does not remove:

- Any process holding the key can announce any category. The key is shared by
  every process that announces, so one plugin can impersonate another. Per-plugin
  keys bound to their category are the follow-up.
- A captured announcement can be replayed within `AnnounceWindow`, to a
  listener that has taken over the address it names.
- The gRPC connection that follows is authenticated only by `TLSConfig`. Dialing
  a loopback address stays plaintext and does not prove the server's identity.

## Further reading

- [Herrscher docs](https://github.com/Herrscherd/herrscher-docs), page
  `architecture/transport`
- [contracts](https://github.com/Herrscherd/herrscher-contracts), for the port
  signatures
