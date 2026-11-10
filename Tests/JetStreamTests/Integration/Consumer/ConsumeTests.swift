// Copyright 2024 The NATS Authors
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

import JetStream
import Logging
import NatsServer
import XCTest

@testable import Nats

class ConsumeTests: XCTestCase {

    static let allTests = [
        ("testConsumeBuffersAhead", testConsumeBuffersAhead),
        ("testConsumeMaxBytes", testConsumeMaxBytes),
        ("testConsumeHeartbeats", testConsumeHeartbeats),
        ("testConsumeStop", testConsumeStop),
        ("testConsumeDrain", testConsumeDrain),
        ("testConsumeReconnect", testConsumeReconnect),
    ]

    var natsServer = NatsServer()
    let payload = Data(repeating: 1, count: 100)

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func start(
        port: Int = -1, messages: Int
    ) async throws -> (NatsClient, JetStreamContext, Consumer) {
        let bundle = Bundle.module
        natsServer.start(
            port: port,
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let ctx = JetStreamContext(client: client)
        let consumer = try await setUpStream(ctx: ctx, messages: messages)
        return (client, ctx, consumer)
    }

    private func setUpStream(ctx: JetStreamContext, messages: Int) async throws -> Consumer {
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        for _ in 0..<messages {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }
        return try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))
    }

    func testConsumeBuffersAhead() async throws {
        let (_, _, consumer) = try await start(messages: 100)

        let messages = try await consumer.consume(
            options: ConsumeOptions(maxMessages: 10, expires: 5))

        // without iterating, at most maxMessages are delivered by the server
        try await Task.sleep(nanoseconds: 300_000_000)
        var info = try await consumer.info()
        XCTAssertEqual(info.numAckPending, 10)

        var received = 0
        for try await msg in messages {
            try await msg.ack()
            received += 1
            if received == 100 {
                break
            }
        }
        info = try await consumer.info()
        XCTAssertEqual(info.delivered.stream, 100)
    }

    func testConsumeMaxBytes() async throws {
        let (_, _, consumer) = try await start(messages: 20)

        let messages = try await consumer.consume(
            options: ConsumeOptions(maxBytes: 1000, expires: 5))
        try await Task.sleep(nanoseconds: 300_000_000)
        let info = try await consumer.info()
        XCTAssertLessThan(info.numAckPending, 10)

        var received = 0
        for try await msg in messages {
            try await msg.ack()
            received += 1
            if received == 20 {
                break
            }
        }
        XCTAssertEqual(received, 20)
    }

    func testConsumeHeartbeats() async throws {
        let (_, ctx, consumer) = try await start(messages: 0)

        let errors = ErrorCollector()
        let messages = try await consumer.consume(
            options: ConsumeOptions(expires: 1, idleHeartbeat: 0.2),
            errorHandler: { errors.append($0) })

        // several pull requests expire while idle, heartbeats keep the watchdog quiet
        let payload = payload
        _ = Task {
            try await Task.sleep(nanoseconds: 2_500_000_000)
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }
        for try await msg in messages {
            try await msg.ack()
            break
        }
        XCTAssertEqual(errors.count, 0)
    }

    func testConsumeStop() async throws {
        let (_, _, consumer) = try await start(messages: 10)

        let messages = try await consumer.consume(
            options: ConsumeOptions(maxMessages: 10, expires: 5))
        try await Task.sleep(nanoseconds: 300_000_000)

        var received = 0
        for try await msg in messages {
            try await msg.ack()
            received += 1
            if received == 2 {
                await messages.stop()
            }
        }
        XCTAssertEqual(received, 2)
    }

    func testConsumeDrain() async throws {
        let (_, ctx, consumer) = try await start(messages: 5)

        let messages = try await consumer.consume(
            options: ConsumeOptions(maxMessages: 10, expires: 5))
        try await Task.sleep(nanoseconds: 300_000_000)
        await messages.drain()

        // published after draining, not delivered
        _ = try await ctx.publish("foo.A", message: payload).wait()

        var received = 0
        for try await msg in messages {
            try await msg.ack()
            received += 1
        }
        XCTAssertEqual(received, 5)
    }

    func testConsumeReconnect() async throws {
        let (client, ctx, consumer) = try await start(port: 4567, messages: 1)

        let errors = ErrorCollector()
        let messages = try await consumer.consume(
            options: ConsumeOptions(expires: 2), errorHandler: { errors.append($0) })

        var received = 0
        for try await msg in messages {
            try await msg.ack()
            received += 1
            if received == 1 {
                // restart the server, pull requests are lost with the connection
                natsServer.stop()
                natsServer.start(
                    port: 4567,
                    cfg: Bundle.module.url(forResource: "jetstream", withExtension: "conf")!
                        .relativePath)
                try await waitForReconnect(client)
                _ = try await setUpStream(ctx: ctx, messages: 1)
            }
            if received == 2 {
                break
            }
        }
        XCTAssertEqual(received, 2)
    }

    private func waitForReconnect(_ client: NatsClient) async throws {
        for _ in 0..<100 {
            if client.connectionHandler?.currentState == .connected {
                return
            }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        XCTFail("client did not reconnect")
    }
}

final class ErrorCollector: @unchecked Sendable {
    private let lock = NSLock()
    private var errors: [Error] = []

    func append(_ error: Error) {
        lock.withLock { errors.append(error) }
    }

    var count: Int {
        lock.withLock { errors.count }
    }
}
