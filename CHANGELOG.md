# Changelog

All notable changes to this project are documented in this file.

## 0.5.0

This release adds complete JetStream support, including atomic batch publishing, the Key-Value
Store and the Object Store, adopts Swift 6, and documents the whole package.

### Modules

| Product | Contents |
|---|---|
| `Nats` | Core NATS |
| `JetStream` | Streams, publishing, atomic batches and consumers |
| `JetStreamKeyValue` | The Key-Value Store (new) |
| `JetStreamObjectStore` | The Object Store (new) |
| `NatsServer` | `nats-server` test helper |

### Swift 6 and platforms

- The package requires Swift 6 (`swift-tools-version: 6.0`) and compiles in the Swift 6 language
  mode with complete concurrency checking, without warnings.
- `NatsClient`, `JetStreamContext`, `Stream`, `Consumer`, `OrderedConsumer`, `PushConsumer`,
  `KeyValue`, `ObjectStore`, all results and all public value types are `Sendable`, so they can be
  shared between tasks and actors.
- All libraries build for macOS, iOS, tvOS, watchOS and visionOS, verified in CI.
- Dependencies are updated to their latest releases: swift-nio 2.105, swift-nio-ssl 2.37,
  swift-log 1.17, nkeys.swift 0.2 and swift-nuid 0.3.
- The client reports version `0.5.0` to the server.

### JetStream

- **Streams**: create, update, create-or-update, delete, list, info with deleted details and
  per-subject counts, lookup by subject, get and direct get, delete and secure delete, purge, all
  stream configuration fields (TTLs, delete markers, counters, schedules, persistence modes),
  mirrors and sources across domains.
- **Atomic batch publishing**: `BatchPublisher` (an actor) and `publishBatch` store a batch of
  messages all or nothing, with flow control and expectations per message.
- **Publishing**: acknowledgements of all publishes on a single subscription,
  `publishAsyncPending()`, `publishAsyncComplete()` and a maximum of pending publishes;
  deduplication, optimistic concurrency, per-message TTLs, message schedules and retries when no
  stream responds.
- **Consumers**: `fetch`, `fetchBytes`, `fetchNoWait`, `next`, and `consume` with pipelined pull
  requests that recover from missed heartbeats, reconnects and leader elections; ordered
  consumers; push consumers with flow control; priority groups (`pinned`, `overflow`,
  `prioritized`); pause, resume, reset and unpin.
- **Messages**: ack, nak with delay, in progress, term with reason, double ack, metadata; a
  message is acknowledged only once.
- **Key-Value Store**: bucket management and listing, get, put, create, update, delete, purge,
  watch, keys, history, purge deletes, per-key TTLs, mirrors, sources and cross-domain buckets.
- **Object Store**: bucket management and listing, chunked put from data, async sequences and
  files, streamed get with SHA-256 verification, list, delete, watch, metadata updates, links and
  sealing.
- **Account info** with limits, tiers and API statistics.

### Fixes

- Failed publish acknowledgements were decoded as successful acknowledgements.
- Every publish leaked a reply subscription.
- Errors with code 10164 were not recognized as wrong last sequence errors.
- Listing consumers of a missing stream returned no consumers instead of an error.

### Documentation and structure

- DocC catalogs for `Nats`, `JetStream`, `JetStreamKeyValue`, `JetStreamObjectStore` and
  `NatsServer` with guides, published on the Swift Package Index, and documentation of every
  public symbol.
- A comprehensive README covering Core NATS, JetStream, the Key-Value Store and the Object Store.
- Sources and tests are organized in feature folders; examples and benchmarks have their own
  folders.
- `JetStreamExample`, a runnable walkthrough of JetStream, run against a server in CI.
- `JetStreamBenchmark`, measuring publishing, atomic batches, consumers, the Key-Value Store and
  the Object Store.

### Breaking changes

- Swift 6 is required.
- The Key-Value Store and the Object Store are imported from the `JetStreamKeyValue` and
  `JetStreamObjectStore` products.
- `NatsClient`, `JetStreamContext`, `Stream` and `Consumer` are `final` classes.
- `Consumer.consume()` takes `ConsumeOptions` and is `async throws`.
- `NatsServer.start(...)` and `stop()` manage a temporary JetStream store directory per server.
