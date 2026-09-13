# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NATS.swift is a Swift client library for the NATS messaging system, providing asynchronous messaging capabilities for Swift applications across macOS and iOS platforms. The project consists of three main modules:
- **Nats**: Core NATS functionality including pub/sub, request/reply, auth, TLS
- **JetStream**: Advanced streaming capabilities (work in progress)
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

**NatsClient** (`Sources/Nats/NatsClient/NatsClient.swift`): Main client interface providing connection management, publish/subscribe operations, and event handling. Built on Swift NIO for asynchronous networking.

**ConnectionHandler** (`Sources/Nats/NatsConnection.swift`): Internal class implementing `ChannelInboundHandler`. Handles the network connection lifecycle, reconnection logic, and protocol-level communication with NATS servers. Uses `NIOLockedValueBox` and `ManagedAtomic` for thread-safe state management.

**NatsMessage** & **NatsSubscription**: Message handling and subscription management with support for headers and wildcards. Subscriptions implement `AsyncSequence` for message iteration.

**JetStreamContext** (`Sources/JetStream/JetStreamContext.swift`): Entry point for JetStream operations, manages streams and consumers with configurable prefixes/domains.

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

- Minimum deployment targets: macOS 13.0, iOS 13.0
- Swift 5.7+ required
- WebSocket upgrade support available for browser-compatible connections
- Batch message processing optimized with `BatchBuffer` for performance