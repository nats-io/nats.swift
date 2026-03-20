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
import Nats
import NatsServer
import XCTest

class ConsumerConsumeTests: XCTestCase {

    static var allTests = [
        ("testConsumeBasic", testConsumeBasic),
        ("testConsumeStop", testConsumeStop),
        ("testConsumeDrain", testConsumeDrain),
        ("testConsumeConsumerDeleted", testConsumeConsumerDeleted),
        ("testConsumeRePull", testConsumeRePull),
        ("testConsumeMissedHeartbeat", testConsumeMissedHeartbeat),
        ("testConsumeInvalidConfig", testConsumeInvalidConfig),
        ("testConsumeRecoverAfterTimeout", testConsumeRecoverAfterTimeout),
        ("testConsumeReconnect", testConsumeReconnect),
        ("testConsumeMaxBytes", testConsumeMaxBytes),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testConsumeBasic() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        for _ in 0..<50 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }

        let messages = try await consumer.consume()

        var count = 0
        for try await msg in messages {
            try await msg.ack()
            XCTAssertEqual(msg.payload, payload)
            count += 1
            if count == 50 {
                messages.stop()
            }
        }
        XCTAssertEqual(count, 50)
    }

    func testConsumeStop() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        for _ in 0..<100 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }

        let messages = try await consumer.consume(
            config: ConsumeConfig(maxMessages: 100, expires: 5))

        var count = 0
        for try await msg in messages {
            try await msg.ack()
            count += 1
            if count == 10 {
                // Stop after 10 messages - should discard the rest
                messages.stop()
            }
        }
        // Should have received exactly 10 messages before stop took effect
        XCTAssertEqual(count, 10)
    }

    func testConsumeDrain() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        for _ in 0..<20 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }

        let messages = try await consumer.consume(
            config: ConsumeConfig(maxMessages: 100, expires: 5))

        // Give time for all messages to arrive in buffer
        try await Task.sleep(nanoseconds: 500_000_000)

        // Call drain - should deliver buffered messages then end
        messages.drain()

        var count = 0
        for try await msg in messages {
            try await msg.ack()
            count += 1
        }
        // Should have received all 20 messages during drain
        XCTAssertEqual(count, 20)
    }

    func testConsumeConsumerDeleted() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        for _ in 0..<5 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }

        let messages = try await consumer.consume(
            config: ConsumeConfig(maxMessages: 100, expires: 5))

        // Delete the consumer after a brief delay
        Task {
            try await Task.sleep(nanoseconds: 500_000_000)
            try await stream.deleteConsumer(name: "cons")
        }

        var count = 0
        do {
            for try await msg in messages {
                try await msg.ack()
                count += 1
            }
        } catch JetStreamError.ConsumeError.consumerDeleted {
            // Expected - consumer was deleted
            XCTAssertEqual(count, 5)
            return
        }
        XCTFail("should get consumer deleted error")
    }

    func testConsumeRePull() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        // Use a small buffer to force re-pulls
        let messages = try await consumer.consume(
            config: ConsumeConfig(maxMessages: 10, expires: 5, thresholdMessages: 5))

        // Publish messages in batches to test re-pull
        let payload = "hello".data(using: .utf8)!

        // Publish first batch
        for _ in 0..<30 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }

        var count = 0
        for try await msg in messages {
            try await msg.ack()
            count += 1
            if count == 30 {
                messages.stop()
            }
        }
        // Should have received all 30 messages across multiple re-pulls
        XCTAssertEqual(count, 30)
    }

    func testConsumeMissedHeartbeat() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        // Delete the consumer before starting consume so the server never
        // sets up heartbeat monitoring for the pull request. This guarantees
        // no heartbeats arrive and the client's timer detects the miss.
        try await stream.deleteConsumer(name: "cons")

        let heartbeatExpectation = XCTestExpectation(
            description: "missed heartbeat should be reported")
        let messages = try await consumer.consume(
            config: ConsumeConfig(maxMessages: 10, expires: 2, idleHeartbeat: 0.5),
            errorHandler: { error in
                if case JetStreamError.ConsumeWarning.missedHeartbeat = error {
                    heartbeatExpectation.fulfill()
                }
            }
        )

        await fulfillment(of: [heartbeatExpectation], timeout: 5.0)
        messages.stop()
    }

    func testConsumeInvalidConfig() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        // maxMessages and maxBytes are mutually exclusive
        try await assertConsumeInvalidConfig(
            consumer,
            config: ConsumeConfig(maxMessages: 100, maxBytes: 1024),
            expectedMessage: "only one of maxMessages and maxBytes can be specified"
        )

        // maxMessages must be at least 1
        try await assertConsumeInvalidConfig(
            consumer,
            config: ConsumeConfig(maxMessages: 0),
            expectedMessage: "maxMessages must be at least 1"
        )

        // maxBytes must be greater than 0
        try await assertConsumeInvalidConfig(
            consumer,
            config: ConsumeConfig(maxBytes: 0),
            expectedMessage: "maxBytes must be greater than 0"
        )

        // expires must be at least 1s
        try await assertConsumeInvalidConfig(
            consumer,
            config: ConsumeConfig(expires: 0.5),
            expectedMessage: "expires value must be at least 1s"
        )

        // idleHeartbeat must be within 500ms-30s range
        try await assertConsumeInvalidConfig(
            consumer,
            config: ConsumeConfig(idleHeartbeat: 0.1),
            expectedMessage: "idleHeartbeat value must be within 500ms-30s range"
        )
        try await assertConsumeInvalidConfig(
            consumer,
            config: ConsumeConfig(idleHeartbeat: 31),
            expectedMessage: "idleHeartbeat value must be within 500ms-30s range"
        )

        // idleHeartbeat must be less than 50% of expires
        try await assertConsumeInvalidConfig(
            consumer,
            config: ConsumeConfig(expires: 4, idleHeartbeat: 3),
            expectedMessage: "idleHeartbeat must be less than 50% of expires"
        )
    }

    func testConsumeRecoverAfterTimeout() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        // Start consuming with a short expires BEFORE any messages are published.
        // The first pull will expire (408 timeout) with no messages delivered.
        // After the timeout, consume must re-pull and pick up newly published messages.
        let messages = try await consumer.consume(
            config: ConsumeConfig(expires: 2, idleHeartbeat: 0.5))

        // Wait for the first pull request to expire
        try await Task.sleep(nanoseconds: 3_000_000_000)

        // Now publish messages — these should be picked up by the re-pull
        let payload = "hello".data(using: .utf8)!
        for _ in 0..<10 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }

        var count = 0
        for try await msg in messages {
            try await msg.ack()
            count += 1
            if count == 10 {
                messages.stop()
            }
        }
        XCTAssertEqual(count, 10)
    }

    func testConsumeReconnect() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        for _ in 0..<5 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }

        let messages = try await consumer.consume(
            config: ConsumeConfig(maxMessages: 100, expires: 2))

        var count = 0
        for try await msg in messages {
            try await msg.ack()
            count += 1
            if count == 5 {
                break
            }
        }
        XCTAssertEqual(count, 5)

        // Force reconnect — server stays up, JetStream state is preserved.
        // This tests the reconnect handler: pending reset + re-pull.
        try await client.reconnect()
        try await Task.sleep(nanoseconds: 1_000_000_000)

        // Publish more messages after reconnect
        for _ in 0..<5 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }

        // Continue consuming — should receive the new messages
        for try await msg in messages {
            try await msg.ack()
            count += 1
            if count == 10 {
                messages.stop()
            }
        }
        XCTAssertEqual(count, 10)
    }

    func testConsumeMaxBytes() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        for _ in 0..<20 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }

        // Use maxBytes instead of maxMessages
        let messages = try await consumer.consume(
            config: ConsumeConfig(maxBytes: 4096, expires: 5))

        var count = 0
        for try await msg in messages {
            try await msg.ack()
            XCTAssertEqual(msg.payload, payload)
            count += 1
            if count == 20 {
                messages.stop()
            }
        }
        XCTAssertEqual(count, 20)
    }

    private func assertConsumeInvalidConfig(
        _ consumer: Consumer,
        config: ConsumeConfig,
        expectedMessage: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        do {
            _ = try await consumer.consume(config: config)
            XCTFail("should throw invalidConfig error", file: file, line: line)
        } catch let error as JetStreamError.ConsumeError {
            guard case .invalidConfig(let message) = error else {
                XCTFail("expected invalidConfig, got \(error)", file: file, line: line)
                return
            }
            XCTAssertEqual(message, expectedMessage, file: file, line: line)
        }
    }
}
