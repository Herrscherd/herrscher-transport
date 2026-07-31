# herrscher-transport

**The wire between host and out-of-process plugins.** Carries
[`contracts`](https://github.com/Herrscherd/herrscher-contracts) ports over
JSON-over-gRPC, with NATS for discovery. It is a Go library imported by hosts
and plugin processes — not an installable plugin, and it runs no server of its
own.

## Role · Category · Ports · Config · Status · Repo

| Aspect | Value |
|--------|-------|
| **Role** | Moves `contracts` port calls across a process boundary, so a remote plugin resolves to a plain port object. |
| **Category** | Transport (library) |
| **Ports carried** | `contracts.Memory`, `contracts.Orchestrator` (unary, via `Plugin.Call`) · `contracts.Backend` (server-streaming, via `BackendStream.Respond`) |
| **Config & env** | None. This module reads no environment variable; TLS is passed in as a `TLSConfig` value (`CAFile`, `CertFile`, `KeyFile`), NATS as an already-connected `*nats.Conn`. |
| **Status** | live |
| **Repo** | [herrscher-transport](https://github.com/Herrscherd/herrscher-transport) |

## Install

```bash
go get github.com/Herrscherd/herrscher-transport
```

## Proxy and skeleton

Each port has a symmetric pair. `RegisterMemorySkeleton(s, real)` (and the
`Orchestrator` / `Backend` equivalents) puts a real port object behind the gRPC
service; `DialMemory(ctx, entry, creds)` returns a `contracts.Memory` proxy that
owns its connection and releases it on `Close`. Adding a method never touches
the `.proto` — the method name and a JSON argument tuple ride inside
`MethodEnvelope`. `ResultEnvelope.error` is an in-band domain error; a peer
being down surfaces as the gRPC error instead.

## Discovery and trust

A plugin publishes an `Announcement` (manifest, gRPC address, instance id) on
`plugins.announce`; `WatchAnnouncements` feeds a `RemoteRegistry`. NATS core
pub-sub has no replay, so the publisher is expected to re-announce
periodically — this module only publishes what it is told to.

The announce bus is unauthenticated, so both trust decisions fail closed:
`NewRemoteRegistry(WithAddrAllow(fn))` drops announcements whose address the
allowlist rejects, and dialing with `nil` credentials is refused for any
non-loopback address. `TLSConfig` requires CA, cert and key together (mutual
TLS); any partial set is an error rather than a silent downgrade to plaintext.

## Further reading

- [Herrscher docs](https://github.com/Herrscherd/herrscher-docs) — `architecture/transport`
- [contracts](https://github.com/Herrscherd/herrscher-contracts) — port signatures
