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

class ConsumerAdminTests: XCTestCase {

    static let allTests = [
        ("testPauseResumeConsumer", testPauseResumeConsumer),
        ("testResetConsumer", testResetConsumer),
        ("testUnpinConsumer", testUnpinConsumer),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func start(
        consumer cfg: ConsumerConfig
    ) async throws -> (
        JetStreamContext, JetStream.Stream, Consumer
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
        let consumer = try await stream.createConsumer(cfg: cfg)
        return (ctx, stream, consumer)
    }

    func testPauseResumeConsumer() async throws {
        let (ctx, stream, consumer) = try await start(consumer: ConsumerConfig(name: "cons"))
        _ = try await ctx.publish("foo.A", message: "hello".data(using: .utf8)!).wait()

        let paused = try await stream.pauseConsumer(
            name: "cons", until: Date().addingTimeInterval(3600))
        XCTAssertTrue(paused.paused)
        XCTAssertNotNil(paused.pauseRemaining)
        let pausedInfo = try await consumer.info()
        XCTAssertEqual(pausedInfo.paused, true)

        // paused consumers do not deliver messages
        var received = 0
        for try await _ in try await consumer.fetch(batch: 1, expires: 0.5) {
            received += 1
        }
        XCTAssertEqual(received, 0)

        let resumed = try await ctx.resumeConsumer(stream: "test", name: "cons")
        XCTAssertFalse(resumed.paused)
        for try await _ in try await consumer.fetch(batch: 1, expires: 1) {
            received += 1
        }
        XCTAssertEqual(received, 1)

        do {
            _ = try await stream.pauseConsumer(name: "bad", until: Date())
            XCTFail("expected consumer not found error")
        } catch JetStreamError.ConsumerError.consumerNotFound {
            // success
        }
    }

    func testResetConsumer() async throws {
        let (ctx, stream, consumer) = try await start(consumer: ConsumerConfig(name: "cons"))
        for i in 1...5 {
            _ = try await ctx.publish("foo.A", message: "\(i)".data(using: .utf8)!).wait()
        }
        for try await msg in try await consumer.fetch(batch: 5, expires: 1) {
            try await msg.doubleAck()
        }

        let reset = try await stream.resetConsumer(name: "cons", sequence: 3)
        XCTAssertEqual(reset.resetSequence, 3)
        XCTAssertEqual(reset.info.name, "cons")

        var payloads: [String] = []
        for try await msg in try await consumer.fetch(batch: 5, expires: 1) {
            payloads.append(String(data: msg.payload!, encoding: .utf8)!)
            try await msg.ack()
        }
        XCTAssertEqual(payloads, ["3", "4", "5"])

        // sequence below the consumer start sequence
        _ = try await stream.createConsumer(
            cfg: ConsumerConfig(name: "start", deliverPolicy: .byStartSequence, optStartSeq: 4))
        do {
            _ = try await stream.resetConsumer(name: "start", sequence: 3)
            XCTFail("expected invalid reset error")
        } catch JetStreamError.ConsumerError.invalidReset {
            // success
        }
    }

    func testUnpinConsumer() async throws {
        let (_, stream, _) = try await start(
            consumer: ConsumerConfig(name: "cons", priorityPolicy: .pinned, priorityGroups: ["A"]))

        try await stream.unpinConsumer(name: "cons", group: "A")

        do {
            try await stream.unpinConsumer(name: "bad", group: "A")
            XCTFail("expected consumer not found error")
        } catch JetStreamError.ConsumerError.consumerNotFound {
            // success
        }
    }
}
