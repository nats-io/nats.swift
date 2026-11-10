# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NATS.swift is a Swift client library for the NATS messaging system, providing asynchronous messaging capabilities for Swift applications across macOS and iOS platforms. The project consists of three main modules:
- **Nats**: Core NATS functionality including pub/sub, request/reply, auth, TLS
- **JetStream**: Streams, consumers (pull, push, ordered), Key-Value Store and Object Store
- **NatsServer**: Test server utilities for integration testing

## Build and Test Commands

```bash
# Build the project
swift build

# Run all tests (requires nats-server on PATH)
swift test

# Run a specific test
swift test --filter <TestClass>/<testMethod>
# Example: swift test --filter NatsTests.CoreNatsTests/testConnect

# Run tests for a specific module
swift test --filter NatsTests
swift test --filter JetStreamTests

# Build release version
swift build -c release

# Clean build artifacts
swift package clean

# Install nats-server (required for integration tests)
curl --fail https://binaries.nats.dev/nats-io/nats-server/v2@latest | PREFIX='/usr/local/bin' sh

# Lint (strict)
swift-format lint --configuration .swift-format -r --strict Sources Tests

# Format code
swift-format format --in-place --configuration .swift-format -r Sources Tests
```

## Architecture

### Core Components

**Package layout**: library targets `Nats`, `JetStream` and `NatsServer` (test helper) are organized in feature folders, each with a DocC catalog (`<Target>.docc`) published by Swift Package Index (`.spi.yml`). Examples live in `Sources/Examples/`, benchmarks in `Sources/Benchmarks/` (targets set `path:`).

**Nats** (`Sources/Nats/`): `Client/` (client, options, events), `Connection/` (connection handler, protocol parser, WebSocket upgrade, write batching), `Messaging/` (messages, headers, subscriptions), `Auth/`, `Errors/`, `Utilities/`.

**NatsClient** (`Sources/Nats/Client/NatsClient.swift`): Main client interface providing connection management, publish/subscribe operations, and event handling. Built on Swift NIO for asynchronous networking.

**ConnectionHandler** (`Sources/Nats/Connection/NatsConnection.swift`): Internal class implementing `ChannelInboundHandler`. Handles the network connection lifecycle, reconnection logic, and protocol-level communication with NATS servers. Uses `NIOLockedValueBox` and `ManagedAtomic` for thread-safe state management.

**NatsMessage** & **NatsSubscription**: Message handling and subscription management with support for headers and wildcards. Subscriptions implement `AsyncSequence` for message iteration.

**JetStream** (`Sources/JetStream/`): organized in feature folders compiled into one module:
- `Context/`: `JetStreamContext` (entry point, API requests with configurable prefixes/domains), stream and consumer management, account info.
- `Publish/`: `publish`, `PublishOptions`, `AckFuture`. `AsyncPublisher` receives all publish acks on one wildcard inbox subscription and enforces max pending.
- `Stream/`, `Message/`: `Stream` (config, state, message access) and `JetStreamMessage` (acks, enforced once; metadata).
- `Consumer/`: `fetch`/`next` send single pull requests; `consume` (`ConsumePuller` actor) pipelines pull requests and recovers from missed heartbeats and reconnects. `OrderedConsumer` recreates ephemeral consumers on gaps and backs the KV and Object Store watchers. `PushConsumer` handles flow control.
- Key-Value Store and Object Store are separate targets/products `JetStreamKeyValue` and `JetStreamObjectStore` (`Sources/JetStreamKeyValue`, `Sources/JetStreamObjectStore`), built on `KV_<bucket>` and `OBJ_<bucket>` streams and wire compatible with other NATS clients. They may use JetStream symbols marked `package` (e.g. `JetStreamContext.prefix`, `NanoTimeInterval.value`).
- `Publish/BatchPublisher.swift`: atomic batch publishing (`BatchPublisher` actor, `publishBatch`).
- Benchmarks: `swift run -c release JetStreamBenchmark` (needs `nats-server -js`).
- `Common/`: `JetStreamError` (typed errors per area), `Response<T>` decoding (error responses first), `NanoTimeInterval`.

Lifecycles of publishes, consumers and watchers: `Sources/JetStream/JetStream.docc/Lifecycles.md`. Release notes: `CHANGELOG.md`.

### Key Design Patterns

- **Async/Await**: Modern Swift concurrency throughout the API
- **AsyncSequence**: Subscriptions implement AsyncSequence for message iteration
- **Protocol-Oriented**: Extensive use of protocols for extensibility
- **Event-Driven**: Event system for connection state changes
- **NIO Integration**: `ChannelInboundHandler` for protocol parsing, `EventLoopGroup` for async I/O

### Authentication Methods

The client supports multiple authentication mechanisms configured through `NatsClientOptions`:
- Username/password
- Token authentication
- NKEY authentication
- JWT with credentials file
- TLS mutual authentication

### Testing Infrastructure

Integration tests use `NatsServer` helper to spawn local NATS server instances with various configurations (auth, TLS, permissions). The `nats-server` binary must be available on `PATH`. Test configurations are stored in `Tests/*/Integration/Resources/`.

## Key Dependencies

- **swift-nio**: Asynchronous event-driven network framework
- **swift-nio-ssl**: TLS support
- **swift-log**: Structured logging
- **nkeys.swift**: NKEY cryptographic operations
- **swift-nuid**: Unique identifier generation

## Coding Style

- Swift style enforced via `swift-format` (configuration in `.swift-format`)
- Indentation: 4 spaces; line length: 100
- Types use `UpperCamelCase`, members use `lowerCamelCase`

## Development Notes

- Minimum deployment targets: macOS 13, iOS 13, tvOS 13, watchOS 6, visionOS 1
- Swift 6 required (`swift-tools-version: 6.0`, Swift 6 language mode with complete concurrency checking). Public types are `Sendable`; JetStream reference types keep mutable state in `Locked<Value>` (`Sources/JetStream/Common/Locking.swift`). Avoid `NSLock.withLock` (iOS 16+), use `locked(_:)`.
- Libraries build for macOS, iOS, tvOS, watchOS and visionOS (CI `apple-platforms` job)
- WebSocket upgrade support available for browser-compatible connections
- Batch message processing optimized with `BatchBuffer` for performance