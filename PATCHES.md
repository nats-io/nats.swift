# Local patches

This fork carries patches that are not (yet) upstream. Everything else is a verbatim copy of
[`nats-io/nats.swift`](https://github.com/nats-io/nats.swift).

Branch `traderverse/rtt-promise-leak` is based on upstream `a9031f129810c5855d5f85771f9523d9b7d707d4`
(`main`, 9 Apr 2026) — the revision `traderverse-ios-v3` was already pinned to, so the patch is
the only thing that changed for that app.

## 1. RTT promises are never left unfulfilled

**Files:** `Sources/Nats/RttCommand.swift`, `Sources/Nats/ConcurrentQueue.swift`,
`Sources/Nats/NatsConnection.swift`
**Test:** `Tests/NatsTests/Unit/RttCommandTests.swift`

`ConnectionHandler.sendPing()` enqueues an `RttCommand` holding an
`EventLoopPromise<TimeInterval>`, and that promise is only ever completed in the `.pong`
branch of the message loop. Nothing drained `pingQueue` on `channelInactive`,
`handleDisconnect()`, `disconnect()`, `suspend()` or `close()`, so a connection that dies with
pings in flight — a stale connection, a backgrounded phone, a NAT rebind — abandoned up to
three promises. They stayed alive with the `ConnectionHandler`, and whatever eventually
released the `NatsClient` deallocated them unfulfilled. SwiftNIO traps on exactly that in
`EventLoopFuture.deinit`:

```
Nats/RttCommand.swift:22: Fatal error: leaking promise created at (file: "Nats/RttCommand.swift", line: 22)
```

The check is inside NIO's `debugOnly`, so it crashes every debug build of the host
application and leaks silently in release.

The patch:

- `RttCommand.fail(_:)` plus a `deinit` that completes the promise with
  `NatsError.ClientError.connectionClosed`. This is the backstop that makes the trap
  unreachable no matter which path drops a command. Completing an already-completed promise
  is a no-op in NIO (`EventLoopFuture._setValue` returns early once `_value != nil`), so a
  command that was answered keeps its measured round trip time.
- `ConcurrentQueue.drain()` — takes every element under one lock.
- `ConnectionHandler.failOutstandingPings(_:)`, called from `channelInactive`,
  `handleDisconnect()`, `disconnect()`, `close()` and `suspend()`, so a pending
  `getRoundTripTime()` throws promptly instead of hanging until the client is released.

## 2. `outstandingPings` can no longer stick above its threshold

**File:** `Sources/Nats/NatsConnection.swift` (folded into `failOutstandingPings(_:)` above)

`outstandingPings` was only reset by an incoming `PONG`, but `sendPing()` returns early via
`handleDisconnect()` once the counter exceeds 2 — *without* writing the `PING` that could earn
that `PONG`. A connection that missed three pings therefore stayed stuck above the threshold
for the rest of the client's life and force-disconnected on every subsequent ping tick, even
after reconnecting cleanly. `failOutstandingPings(_:)` resets the counter alongside the drain.

## Rebasing onto a newer upstream

```bash
git remote add upstream https://github.com/nats-io/nats.swift
git fetch upstream
git rebase <upstream-sha> traderverse/rtt-promise-leak
swift test --filter RttCommandTests
```

Then repoint `traderverse-ios-v3.xcodeproj`'s package reference at the new revision — it pins
by SHA, deliberately, so a rebase here cannot reach the app without someone deciding it should.
