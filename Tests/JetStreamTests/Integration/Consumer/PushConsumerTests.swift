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

class PushConsumerTests: XCTestCase {

    static let allTests = [
        ("testPushConsume", testPushConsume),
        ("testPushConsumeFlowControl", testPushConsumeFlowControl),
        ("testPushConsumeQueueGroup", testPushConsumeQueueGroup),
        ("testPushConsumerErrors", testPushConsumerErrors),
        ("testPushConsumeConsumerDeleted", testPushConsumeConsumerDeleted),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func start(
        messages: Int, size: Int = 10
    ) async throws -> (
        JetStreamContext, JetStream.Stream
    ) {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        let payload = Data(repeating: 1, count: size)
        for _ in 0..<messages {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }
        return (ctx, stream)
    }

    func testPushConsume() async throws {
        let (_, stream) = try await start(messages: 10)
        let consumer = try await stream.createPushConsumer(
            cfg: ConsumerConfig(durable: "push", deliverSubject: "deliver.push"))

        var received = 0
        for try await msg in try await consumer.consume() {
            try await msg.ack()
            received += 1
            if received == 10 {
                break
            }
        }
        XCTAssertEqual(received, 10)
        let info = try await consumer.info()
        XCTAssertEqual(info.delivered.stream, 10)
    }

    func testPushConsumeFlowControl() async throws {
        // large messages make the server request flow control
        let (_, stream) = try await start(messages: 200, size: 64 * 1024)
        let consumer = try await stream.createPushConsumer(
            cfg: ConsumerConfig(
                durable: "push", ackPolicy: .none, deliverSubject: "deliver.push",
                flowControl: true, idleHeartbeat: NanoTimeInterval(1)))

        let errors = ErrorCollector()
        var received = 0
        for try await _ in try await consumer.consume(errorHandler: { errors.append($0) }) {
            received += 1
            if received == 200 {
                break
            }
        }
        XCTAssertEqual(received, 200)
        XCTAssertEqual(errors.count, 0)
    }

    func testPushConsumeQueueGroup() async throws {
        let (ctx, stream) = try await start(messages: 0)
        let first = try await stream.createPushConsumer(
            cfg: ConsumerConfig(
                durable: "push", deliverSubject: "deliver.push", deliverGroup: "workers"))
        let second = try await ctx.pushConsumer(stream: "test", name: "push")!

        let firstMessages = try await first.consume()
        let secondMessages = try await second.consume()
        for _ in 0..<20 {
            _ = try await ctx.publish("foo.A", message: Data([1])).wait()
        }

        let counter = ErrorCollector()
        let tasks = [firstMessages, secondMessages].map { messages in
            Task {
                var received = 0
                for try await msg in messages {
                    try await msg.ack()
                    received += 1
                    counter.append(JetStreamError.ConsumerError.alreadyConsuming)
                    if counter.count == 20 {
                        await firstMessages.stop()
                        await secondMessages.stop()
                    }
                }
                return received
            }
        }
        var counts: [Int] = []
        for task in tasks {
            counts.append(try await task.value)
        }
        XCTAssertEqual(counts.reduce(0, +), 20)
        XCTAssertTrue(counts.allSatisfy { $0 > 0 }, "messages not distributed: \(counts)")
    }

    func testPushConsumerErrors() async throws {
        let (ctx, stream) = try await start(messages: 0)

        do {
            _ = try await stream.createPushConsumer(cfg: ConsumerConfig(durable: "pull"))
            XCTFail("expected not push consumer error")
        } catch JetStreamError.ConsumerError.notPushConsumer {
            // success
        }

        _ = try await stream.createConsumer(cfg: ConsumerConfig(durable: "pull"))
        do {
            _ = try await ctx.pushConsumer(stream: "test", name: "pull")
            XCTFail("expected not push consumer error")
        } catch JetStreamError.ConsumerError.notPushConsumer {
            // success
        }
        let missing = try await ctx.pushConsumer(stream: "test", name: "missing")
        XCTAssertNil(missing)

        let consumer = try await stream.createPushConsumer(
            cfg: ConsumerConfig(durable: "push", deliverSubject: "deliver.push"))
        let messages = try await consumer.consume()
        do {
            _ = try await consumer.consume()
            XCTFail("expected already consuming error")
        } catch JetStreamError.ConsumerError.alreadyConsuming {
            // success
        }
        await messages.stop()
    }

    func testPushConsumeConsumerDeleted() async throws {
        let (ctx, stream) = try await start(messages: 0)
        let consumer = try await stream.createPushConsumer(
            cfg: ConsumerConfig(
                durable: "push", deliverSubject: "deliver.push",
                idleHeartbeat: NanoTimeInterval(0.5)))

        // the server stops sending heartbeats to deleted push consumers
        let errors = ErrorCollector()
        let messages = try await consumer.consume(errorHandler: { errors.append($0) })
        try await Task.sleep(nanoseconds: 1_500_000_000)
        XCTAssertEqual(errors.count, 0)

        try await ctx.deleteConsumer(stream: "test", name: "push")
        try await Task.sleep(nanoseconds: 2_000_000_000)
        XCTAssertGreaterThan(errors.count, 0)
        await messages.stop()
    }
}
