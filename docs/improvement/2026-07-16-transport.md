# herrscher-transport — continuous-improvement audit (2026-07-16)

Axes: security · performance · code-quality · bug-review. No new features.
Multi-agent pass, every finding adversarially verified. Concurrency was the priority axis.

## Applied (safe-fix branch `improve/transport-docs`, commit `85364e8`)

Doc comments on exported `PortMemory`, `PortOrchestrator`, `NewRemoteRegistry` — matching the existing `PortBackend` / sibling-constructor convention. Gate green: build + vet + 26 tests.

## Proposals (need your call — not applied)

### MEDIUM · bug — `BackendProxy.Respond` leaks the per-call gRPC context
`backend_stream.go:137-181` — `NewStream` gets the caller's ctx directly, never wrapped in a cancellable child. The receive loop returns on the application-level `Done` frame (and on decode-error / invalid-frame paths) **without** draining `RecvMsg` to `io.EOF` or cancelling. Per grpc-go's `ClientConn.NewStream` contract, the internally-derived context is only released on cancel or RecvMsg-to-error — neither happens on the happy path, so the child cancelCtx stays registered in the (long-lived, session-spanning) parent ctx and **accumulates per turn**.
*(The raw HTTP/2 stream is still reclaimed by the transport reader — hence medium, not high — but the un-cancelled context accumulation is real.)*
**Fix:** `ctx, cancel := context.WithCancel(ctx); defer cancel()` before `NewStream`. One-line, idiomatic, covers every return path.

### MEDIUM · security — NATS announcements are trusted and dialed without auth or address validation
`remote_registry.go:33-37, 67-75` — `Observe` stores any `Announcement` seen on `plugins.announce` (attacker-controllable `GrpcAddr` keyed by attacker-controllable `InstanceID`); `Dial*` then proxies real Memory/Orchestrator/Backend traffic (prompts, recalled memory, replies) to that address. No signature/identity check, no allowlist. Any NATS-bus participant can register a rogue plugin and hijack a category → exfiltrate or inject turn data (SSRF/authz-bypass shape).
*Tempered:* default deployment is loopback/plaintext single-host; arbitrary-external exfiltration needs a shared multi-host bus without mTLS. But mTLS doesn't bind identity→category, so an authorized fleet member could still hijack a category.
**Fix:** authenticate announcements (signed manifests / out-of-band identity) and/or validate `GrpcAddr` against an allowlist or loopback constraint before storing.

### MEDIUM · security — `dialConn` fails *open* to plaintext for any address when `creds == nil`
`remote_registry.go:71-74` — nil creds → `insecure.NewCredentials()` unconditionally, regardless of whether `GrpcAddr` is loopback. Doc comment claims "loopback default" but nothing enforces it. Combined with the unvalidated announcement address, an off-host address is dialed in cleartext, exposing prompts/replies/memory on the wire. Unlike `TLSConfig.Validate` (fails closed), this fails open.
**Fix:** when creds is nil, reject non-loopback `GrpcAddr` (or require explicit plaintext opt-in).

### LOW · security/quality — announcement error handling
- `announce.go:29-33` — `WatchAnnouncements` silently swallows unmarshal/decode errors → a malformed or malicious payload is dropped invisibly, no observability.
- `announce.go:18-24` — `Announce` returns Marshal/Publish errors without context or `%w`.
**Fix:** wrap with `%w` + surface a callback/log hook for decode failures.

### LOW · perf — `codec.go:7-16` marshals every RPC via reflection-based `encoding/json` with no reuse
Awareness only; fine for current throughput. Consider a faster codec / buffer reuse only if RPC volume grows.

### LOW · quality
- `remote_registry.go:15-19` — `RemoteEntry` duplicates `Announcement` structurally with manual field copy (drift risk).
- `remote_registry.go:67-75` — `dialConn` checks `ctx.Err()` but never applies `ctx` to the connection.

## Verdict
No data races, no goroutine-per-request leaks beyond the ctx-accumulation. The three MEDIUMs cluster around **trusting the NATS bus for plugin identity + failing open to plaintext** — worth a hardening pass together (auth announcements + loopback/allowlist enforcement + cancellable backend ctx). The rest is polish.
