// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Foundation
import NIO
import NIOConcurrencyHelpers
import XCTest

@testable import Nats

final class HandshakeTests: XCTestCase {
    private var peers: [HandshakePeer] = []
    private var clients: [NatsClient] = []
    private var tasks: [Task<Void, Never>] = []

    override func tearDown() async throws {
        for task in tasks { task.cancel() }
        // Close peers first, so a failed assertion cannot leave client I/O waiting on the fixture.
        for peer in peers { await peer.stop() }
        for client in clients { try? await client.close() }
        for task in tasks { await task.value }
        try await super.tearDown()
    }

    func testFailureWhileCredentialsAreLoadingEndsConnect() async throws {
        for sendError in [true, false] {
            let loading = expectation(description: "credentials request is pending")
            let credentials = try await makePeer(
                delimiter: "\r\n\r\n",
                onRead: { _, _ in
                    loading.fulfill()
                })
            let server = try await makePeer(onConnect: { channel in
                channel.writeAndFlush(channel.allocator.buffer(string: Self.info), promise: nil)
            })
            let client = NatsClientOptions().url(server.url)
                .credentialsFile(credentials.url(scheme: "http")).build()
            clients.append(client)
            let finished = expectation(description: "handshake fails without credentials returning")
            let connecting = Task {
                defer { finished.fulfill() }
                do {
                    try await client.connect()
                    XCTFail("Expected the handshake to fail")
                } catch {
                    if sendError {
                        XCTAssertEqual(error as? NatsError.ServerError, .authenticationTimeout)
                    } else {
                        guard case NatsError.ConnectError.io(let cause) = error else {
                            XCTFail("Expected connectionClosed, got \(error)")
                            return
                        }
                        guard case NatsError.ClientError.connectionClosed = cause else {
                            XCTFail("Expected connectionClosed, got \(cause)")
                            return
                        }
                    }
                }
            }

            tasks.append(connecting)

            await fulfillment(of: [loading], timeout: 2)
            let channel = try XCTUnwrap(server.channels.withLockedValue { $0.first })
            if sendError {
                // Leave the socket open: the error itself must end the attempt.
                try await channel.writeAndFlush(
                    channel.allocator.buffer(string: "-ERR 'Authentication Timeout'\r\n"))
            } else {
                try await channel.close().get()
            }
            await fulfillment(of: [finished], timeout: 2)
        }
    }

    func testMessagesAfterHandshakePongUseNormalProcessing() async throws {
        let awaitingPong = expectation(description: "handshake awaits PONG")
        let replied = expectation(description: "server PING receives PONG")
        let server = try await makePeer(
            onConnect: { channel in
                channel.writeAndFlush(channel.allocator.buffer(string: Self.info), promise: nil)
            },
            onRead: { _, line in
                if line == "PING" { awaitingPong.fulfill() }
                if line == "PONG" { replied.fulfill() }
            })
        let client = NatsClientOptions().url(server.url).build()
        clients.append(client)
        let handler = try XCTUnwrap(client.connectionHandler)
        let receivedInfo = NIOLockedValueBox(false)
        let receivedError = NIOLockedValueBox(false)
        let listener = client.on(.lameDuckMode) { _ in
            XCTAssertNotNil(handler.batchBuffer, "Normal writes must be ready at protocol handoff")
            receivedInfo.withLockedValue { $0 = true }
        }
        defer { client.off(listener) }
        client.on(.error) { event in
            guard case .error(let error) = event else { return }
            XCTAssertEqual(error as? NatsError.ServerError, .authenticationTimeout)
            receivedError.withLockedValue { $0 = true }
        }
        let connected = expectation(description: "handshake completes")
        tasks.append(
            Task {
                defer { connected.fulfill() }
                do {
                    try await client.connect()
                } catch {
                    XCTFail("Connect failed: \(error)")
                }
            })
        await fulfillment(of: [awaitingPong], timeout: 2)

        let channel = try XCTUnwrap(handler.channel)
        let info = Self.info.replacingOccurrences(
            of: "\"proto\":1", with: "\"proto\":1,\"ldm\":true")
        // Feed one read batch so TCP packet splitting cannot hide the protocol boundary.
        try await channel.eventLoop.submit {
            let messages = "PONG\r\n" + info + "-ERR 'Authentication Timeout'\r\nPING\r\n"
            channel.pipeline.syncOperations.fireChannelRead(
                NIOAny(channel.allocator.buffer(string: messages)))
            channel.pipeline.fireChannelReadComplete()
            XCTAssertTrue(
                receivedInfo.withLockedValue { $0 }, "Post-PONG INFO must not be swallowed")
            XCTAssertTrue(
                receivedError.withLockedValue { $0 }, "Post-PONG -ERR must not be swallowed")
        }.get()

        await fulfillment(of: [connected, replied], timeout: 2)
    }

    func testHandshakeErrorCannotBeOverriddenByPong() async throws {
        let awaitingPong = expectation(description: "handshake awaits PONG")
        let server = try await makePeer(
            onConnect: { channel in
                channel.writeAndFlush(channel.allocator.buffer(string: Self.info), promise: nil)
            },
            onRead: { _, line in
                if line == "PING" { awaitingPong.fulfill() }
            })
        let client = NatsClientOptions().url(server.url).build()
        clients.append(client)
        let handler = try XCTUnwrap(client.connectionHandler)
        client.on(.connected) { _ in XCTFail("A failed handshake must not connect") }
        let finished = expectation(description: "handshake preserves the original rejection")
        tasks.append(
            Task {
                defer { finished.fulfill() }
                do {
                    try await client.connect()
                    XCTFail("Expected authentication timeout")
                } catch {
                    XCTAssertEqual(error as? NatsError.ServerError, .authenticationTimeout)
                }
            })
        await fulfillment(of: [awaitingPong], timeout: 2)

        let channel = try XCTUnwrap(handler.channel)
        try await channel.eventLoop.submit {
            let messages = "-ERR 'Authentication Timeout'\r\nPONG\r\n"
            channel.pipeline.syncOperations.fireChannelRead(
                NIOAny(channel.allocator.buffer(string: messages)))
            channel.pipeline.fireChannelReadComplete()
            XCTAssertNil(handler.batchBuffer, "A rejected handshake must not enable normal writes")
        }.get()

        await fulfillment(of: [finished], timeout: 2)
    }

    func testSilentHandshakeTimesOut() async throws {
        for greeting in ["", Self.info] {
            let server = try await makePeer(
                onConnect: { channel in
                    channel.writeAndFlush(channel.allocator.buffer(string: greeting), promise: nil)
                },
                onRead: { channel, line in
                    if line == "PING" {
                        // Send non-PONG messages only after CONNECT and the client's PING arrive.
                        channel.writeAndFlush(
                            channel.allocator.buffer(string: "+OK\r\nPING\r\n"), promise: nil)
                    }
                })
            let client = NatsClientOptions().url(server.url).build()
            clients.append(client)
            let finished = expectation(description: "handshake reaches its deadline")
            let connecting = Task {
                defer { finished.fulfill() }
                do {
                    try await client.connect()
                    XCTFail("Expected timeout, not a successful connection")
                } catch {
                    guard case NatsError.ConnectError.timeout = error else {
                        XCTFail("Expected timeout, got \(error)")
                        return
                    }
                }
            }

            tasks.append(connecting)

            await fulfillment(of: [finished], timeout: 7)
        }
    }

    func testWebSocketUpgradeTimesOut() async throws {
        let server = try await makePeer()
        let client = NatsClientOptions().url(server.url(scheme: "ws")).build()
        clients.append(client)
        let finished = expectation(description: "silent WebSocket upgrade times out")
        let connecting = Task {
            defer { finished.fulfill() }
            do {
                try await client.connect()
                XCTFail("Expected timeout")
            } catch {
                guard case NatsError.ConnectError.timeout = error else {
                    XCTFail("Expected timeout, got \(error)")
                    return
                }
            }
        }

        tasks.append(connecting)

        await fulfillment(of: [finished], timeout: 7)
    }

    func testCancellationBeforeInfoEndsConnect() async throws {
        let accepted = expectation(description: "TCP connected without INFO")
        let server = try await makePeer(onConnect: { _ in accepted.fulfill() })
        let client = NatsClientOptions().url(server.url).build()
        clients.append(client)
        let finished = expectation(description: "cancellation releases the handshake")
        let connecting = Task {
            defer { finished.fulfill() }
            do {
                try await client.connect()
                XCTFail("Expected cancellation")
            } catch {
                XCTAssertTrue(error is CancellationError, "Expected cancellation, got \(error)")
            }
        }

        tasks.append(connecting)

        await fulfillment(of: [accepted], timeout: 2)
        connecting.cancel()
        await fulfillment(of: [finished], timeout: 2)
    }

    func testReconnectRetriesAfterErrorDuringCredentialsLoading() async throws {
        let loading = expectation(description: "reconnect credentials request is pending")
        let requests = NIOLockedValueBox(0)
        let contents = try String(
            contentsOf: XCTUnwrap(
                Bundle.module.url(forResource: "TestUser", withExtension: "creds")),
            encoding: .utf8)
        let response =
            "HTTP/1.1 200 OK\r\nContent-Length: \(contents.utf8.count)\r\n"
            + "Cache-Control: no-store\r\nConnection: close\r\n\r\n" + contents
        let credentials = try await makePeer(
            delimiter: "\r\n\r\n",
            onRead: { channel, _ in
                let request = requests.withLockedValue { count in
                    count += 1
                    return count
                }
                if request == 2 {
                    loading.fulfill()
                } else {
                    channel.writeAndFlush(channel.allocator.buffer(string: response)).whenComplete {
                        _ in
                        channel.close(promise: nil)
                    }
                }
            })
        let subscriptionID = NIOLockedValueBox<String?>(nil)
        let server = try await makePeer(
            onConnect: { channel in
                channel.writeAndFlush(channel.allocator.buffer(string: Self.info), promise: nil)
            },
            onRead: { channel, line in
                if line == "PING" {
                    channel.writeAndFlush(
                        channel.allocator.buffer(string: "PONG\r\n"), promise: nil)
                } else if line.hasPrefix("SUB test ") {
                    subscriptionID.withLockedValue {
                        $0 = line.split(separator: " ").last.map(String.init)
                    }
                } else if line == "ok", let sid = subscriptionID.withLockedValue({ $0 }) {
                    channel.writeAndFlush(
                        channel.allocator.buffer(string: "MSG test \(sid) 2\r\nok\r\n"),
                        promise: nil)
                }
            })
        let client = NatsClientOptions().url(server.url).reconnectWait(0.01)
            .credentialsFile(credentials.url(scheme: "http")).build()
        clients.append(client)
        try await client.connect()
        let subscription = try await client.subscribe(subject: "test")
        addTeardownBlock { subscription.complete() }
        _ = try await client.rtt()

        let errors = NIOLockedValueBox<[NatsErrorProtocol]>([])
        client.on(.error) { event in
            if case .error(let error) = event {
                errors.withLockedValue { $0.append(error) }
            }
        }
        let reconnected = expectation(description: "the next attempt reconnects")
        client.on(.connected) { _ in reconnected.fulfill() }
        let first = try XCTUnwrap(server.channels.withLockedValue { $0.first })
        try await first.close().get()
        await fulfillment(of: [loading], timeout: 3)
        subscriptionID.withLockedValue { $0 = nil }
        let rejected = try XCTUnwrap(server.channels.withLockedValue { $0.last })
        try await rejected.writeAndFlush(
            rejected.allocator.buffer(string: "-ERR 'Authentication Timeout'\r\n"))

        await fulfillment(of: [reconnected], timeout: 3)
        XCTAssertEqual(errors.withLockedValue { $0.count }, 1)
        XCTAssertEqual(
            errors.withLockedValue { $0.first as? NatsError.ServerError }, .authenticationTimeout)
        XCTAssertEqual(requests.withLockedValue { $0 }, 3)
        XCTAssertEqual(server.channels.withLockedValue { $0.count }, 3)

        let delivered = expectation(description: "subscription and publishing work after reconnect")
        let receiving = Task {
            defer { delivered.fulfill() }
            do {
                let message = try await subscription.makeAsyncIterator().next()
                XCTAssertEqual(message?.payload, Data("ok".utf8))
            } catch {
                XCTFail("Restored subscription failed: \(error)")
            }
        }
        tasks.append(receiving)

        try await client.publish(Data("ok".utf8), subject: "test")
        await fulfillment(of: [delivered], timeout: 3)
    }

    func testDisconnectDuringConnectedNotificationStartsNextAttempt() async throws {
        let server = try await makePeer(
            onConnect: { channel in
                channel.writeAndFlush(channel.allocator.buffer(string: Self.info), promise: nil)
            },
            onRead: { channel, line in
                if line == "PING" {
                    channel.writeAndFlush(
                        channel.allocator.buffer(string: "PONG\r\n"), promise: nil)
                }
            })
        let client = NatsClientOptions().url(server.url).reconnectWait(0.01).build()
        clients.append(client)
        let handler = try XCTUnwrap(client.connectionHandler)
        let events = NIOLockedValueBox<[NatsEventKind]>([])
        let reconnected = expectation(description: "the finishing task hands off to reconnect")
        let listener = client.on([.connected, .disconnected]) { event in
            let count = events.withLockedValue { events in
                events.append(event.kind())
                return events.count
            }
            if case .connected = event {
                if count == 1 {
                    // Close synchronously before the initial connection task finishes notifying.
                    handler.channel?.close(promise: nil)
                } else {
                    reconnected.fulfill()
                }
            } else {
                XCTAssertEqual(handler.currentState, .connecting)
            }
        }
        defer { client.off(listener) }

        try await client.connect()
        await fulfillment(of: [reconnected], timeout: 2)
        XCTAssertEqual(events.withLockedValue { $0 }, [.connected, .disconnected, .connected])
        XCTAssertEqual(server.channels.withLockedValue { $0.count }, 2)
    }

    func testReconnectClearsMissedHeartbeats() async throws {
        let connections = NIOLockedValueBox(0)
        let pings = NIOLockedValueBox(0)
        let missedHeartbeats = expectation(description: "two heartbeats receive no reply")
        missedHeartbeats.expectedFulfillmentCount = 2
        let heartbeat = expectation(
            description: "the recovered connection sends its next heartbeat")
        let server = try await makePeer(
            onConnect: { channel in
                connections.withLockedValue { $0 += 1 }
                channel.writeAndFlush(channel.allocator.buffer(string: Self.info), promise: nil)
            },
            onRead: { channel, line in
                guard line == "PING" else { return }
                let ping = pings.withLockedValue { count in
                    count += 1
                    return count
                }
                // Complete the first handshake, then drop two heartbeats to force a reconnect.
                if ping == 2 || ping == 3 {
                    missedHeartbeats.fulfill()
                    return
                }
                channel.writeAndFlush(channel.allocator.buffer(string: "PONG\r\n"), promise: nil)
                if ping == 5, connections.withLockedValue({ $0 }) == 2 {
                    heartbeat.fulfill()
                }
            })
        let client = NatsClientOptions().url(server.url).reconnectWait(0.01).build()
        clients.append(client)
        try await client.connect()
        let handler = try XCTUnwrap(client.connectionHandler)
        let heartbeats = (0..<4).map { _ in RttCommand.makeFrom(channel: handler.channel) }
        addTeardownBlock {
            // The test deliberately leaves heartbeat replies pending.
            for heartbeat in heartbeats {
                heartbeat.promise?.fail(NatsError.ClientError.connectionClosed)
            }
        }
        let reconnected = expectation(description: "heartbeat loss triggers a new handshake")
        client.on(.connected) { _ in reconnected.fulfill() }

        await handler.sendPing(heartbeats[0])
        await handler.sendPing(heartbeats[1])
        await fulfillment(of: [missedHeartbeats], timeout: 2)
        await handler.sendPing(heartbeats[2])
        await fulfillment(of: [reconnected], timeout: 2)

        await handler.sendPing(heartbeats[3])
        await fulfillment(of: [heartbeat], timeout: 2)
        XCTAssertEqual(connections.withLockedValue { $0 }, 2)
        XCTAssertEqual(handler.currentState, .connected)
    }

    func testCloseDuringReconnectEndsHandshake() async throws {
        let attempts = NIOLockedValueBox(0)
        let reconnecting = expectation(description: "reconnect is waiting for INFO")
        let server = try await makePeer(
            onConnect: { channel in
                let attempt = attempts.withLockedValue { count in
                    count += 1
                    return count
                }
                if attempt == 1 {
                    channel.writeAndFlush(channel.allocator.buffer(string: Self.info), promise: nil)
                } else {
                    reconnecting.fulfill()
                }
            },
            onRead: { channel, line in
                if line == "PING" {
                    channel.writeAndFlush(
                        channel.allocator.buffer(string: "PONG\r\n"), promise: nil)
                }
            })
        let client = NatsClientOptions().url(server.url).reconnectWait(0.01).build()
        clients.append(client)
        try await client.connect()
        let first = try XCTUnwrap(server.channels.withLockedValue { $0.first })
        try await first.close().get()
        await fulfillment(of: [reconnecting], timeout: 2)

        let errorCount = NIOLockedValueBox(0)
        client.on(.error) { _ in errorCount.withLockedValue { $0 += 1 } }
        let closed = expectation(description: "close drains the reconnect attempt")
        let closing = Task {
            defer { closed.fulfill() }
            do {
                try await client.close()
            } catch {
                XCTFail("Close failed: \(error)")
            }
        }
        tasks.append(closing)

        await fulfillment(of: [closed], timeout: 2)
        XCTAssertEqual(errorCount.withLockedValue { $0 }, 0)
    }

    func testStopDuringInitialConnectDoesNotPublishSuccess() async throws {
        for suspend in [false, true] {
            let awaitingPong = expectation(description: "initial handshake awaits PONG")
            let server = try await makePeer(
                onConnect: { channel in
                    channel.writeAndFlush(channel.allocator.buffer(string: Self.info), promise: nil)
                },
                onRead: { _, line in
                    if line == "PING" { awaitingPong.fulfill() }
                })
            let client = NatsClientOptions().url(server.url).build()
            clients.append(client)
            let connected = NIOLockedValueBox(false)
            client.on(.connected) { _ in connected.withLockedValue { $0 = true } }
            let finished = expectation(description: "initial connect is cancelled")
            let connecting = Task {
                defer { finished.fulfill() }
                do {
                    try await client.connect()
                    XCTFail("A stopped handshake must not connect")
                } catch {
                    XCTAssertTrue(error is CancellationError, "Expected cancellation, got \(error)")
                }
            }
            tasks.append(connecting)
            await fulfillment(of: [awaitingPong], timeout: 2)

            let stopped = expectation(description: "stop drains the initial connection task")
            let stopping = Task {
                defer { stopped.fulfill() }
                do {
                    if suspend {
                        try await client.suspend()
                    } else {
                        try await client.close()
                    }
                } catch {
                    XCTFail("Stop failed: \(error)")
                }
            }
            tasks.append(stopping)
            await fulfillment(of: [stopped, finished], timeout: 2)

            XCTAssertFalse(connected.withLockedValue { $0 })
            XCTAssertEqual(client.connectionHandler?.currentState, suspend ? .suspended : .closed)
        }
    }

    func testReconnectLimitClosesWithoutWaitingOnItself() async throws {
        let server = try await makePeer(onConnect: { channel in
            channel.writeAndFlush(
                channel.allocator.buffer(string: Self.info + "-ERR 'Authentication Timeout'\r\n"),
                promise: nil)
        })
        let client = NatsClientOptions().url(server.url)
            .retryOnfailedConnect().maxReconnects(1).reconnectWait(30).build()
        clients.append(client)
        let closed = expectation(description: "retry exhaustion closes the client")
        client.on(.closed) { _ in closed.fulfill() }

        try await client.connect()
        await fulfillment(of: [closed], timeout: 2)
        XCTAssertEqual(server.channels.withLockedValue { $0.count }, 1)
    }

    private func makePeer(
        delimiter: String = "\r\n",
        onConnect: @escaping @Sendable (Channel) -> Void = { _ in },
        onRead: @escaping @Sendable (Channel, String) -> Void = { _, _ in }
    ) async throws -> HandshakePeer {
        let peer = try await HandshakePeer(
            delimiter: delimiter, onConnect: onConnect, onRead: onRead)
        peers.append(peer)
        return peer
    }

    private static let info =
        "INFO {\"server_id\":\"test\",\"server_name\":\"test\",\"host\":\"127.0.0.1\","
        + "\"port\":4222,\"version\":\"2.0.0\",\"max_payload\":1048576,\"proto\":1,"
        + "\"go\":\"test\",\"client_ip\":\"127.0.0.1\",\"headers\":true,\"nonce\":\"test\"}\r\n"
}

// Loopback peers control protocol boundaries without depending on timing inside nats-server.
private final class HandshakePeer {
    let channels = NIOLockedValueBox<[Channel]>([])
    private let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    private let listener: Channel

    var url: URL { url(scheme: "nats") }

    init(
        delimiter: String = "\r\n",
        onConnect: @escaping @Sendable (Channel) -> Void = { _ in },
        onRead: @escaping @Sendable (Channel, String) -> Void = { _, _ in }
    ) async throws {
        let channels = self.channels
        do {
            listener = try await ServerBootstrap(group: group)
                .childChannelInitializer { channel in
                    channels.withLockedValue { $0.append(channel) }
                    return channel.pipeline.addHandler(
                        Peer(delimiter: delimiter, onConnect: onConnect, onRead: onRead))
                }
                .bind(host: "127.0.0.1", port: 0).get()
        } catch {
            try? await group.shutdownGracefully()
            throw error
        }
    }

    func url(scheme: String) -> URL {
        URL(string: "\(scheme)://127.0.0.1:\(listener.localAddress!.port!)")!
    }

    func stop() async {
        try? await listener.close().get()
        for channel in channels.withLockedValue({ $0 }) {
            try? await channel.close().get()
        }
        try? await group.shutdownGracefully()
    }

    private final class Peer: ChannelInboundHandler {
        typealias InboundIn = ByteBuffer
        let delimiter: String
        let onConnect: @Sendable (Channel) -> Void
        let onRead: @Sendable (Channel, String) -> Void
        private var input = ""

        init(
            delimiter: String,
            onConnect: @escaping @Sendable (Channel) -> Void,
            onRead: @escaping @Sendable (Channel, String) -> Void
        ) {
            self.delimiter = delimiter
            self.onConnect = onConnect
            self.onRead = onRead
        }

        func channelActive(context: ChannelHandlerContext) {
            onConnect(context.channel)
        }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            input += String(buffer: unwrapInboundIn(data))
            while let boundary = input.range(of: delimiter) {
                let message = String(input[..<boundary.lowerBound])
                input.removeSubrange(..<boundary.upperBound)
                onRead(context.channel, message)
            }
        }
    }
}
