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

class OrderedConsumerTests: XCTestCase {

    static let allTests = [
        ("testOrderedConsume", testOrderedConsume),
        ("testOrderedConsumeRecreatesDeletedConsumer", testOrderedConsumeRecreatesDeletedConsumer),
        ("testOrderedFetchAndNext", testOrderedFetchAndNext),
        ("testOrderedDeliverPolicyAndFilter", testOrderedDeliverPolicyAndFilter),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func start(messages: Int) async throws -> (JetStreamContext, JetStream.Stream) {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        try await publish(ctx: ctx, from: 1, to: messages)
        return (ctx, stream)
    }

    private func publish(
        ctx: JetStreamContext, from: Int, to: Int, subject: String = "foo.A"
    )
        async throws
    {
        guard from <= to else { return }
        for i in from...to {
            _ = try await ctx.publish(subject, message: "\(i)".data(using: .utf8)!).wait()
        }
    }

    private func payload(_ message: JetStreamMessage?) -> String? {
        message?.payload.flatMap { String(data: $0, encoding: .utf8) }
    }

    func testOrderedConsume() async throws {
        let (_, stream) = try await start(messages: 10)
        let ordered = stream.orderedConsumer()

        var received: [String] = []
        for try await msg in try await ordered.consume(options: ConsumeOptions(maxMessages: 3)) {
            received.append(payload(msg)!)
            if received.count == 10 {
                break
            }
        }
        XCTAssertEqual(received, (1...10).map { "\($0)" })

        let info = try await ordered.info()
        XCTAssertEqual(info?.config.ackPolicy, AckPolicy.none)
        XCTAssertEqual(info?.config.memoryStorage, true)
    }

    func testOrderedConsumeRecreatesDeletedConsumer() async throws {
        let (ctx, stream) = try await start(messages: 5)
        let ordered = stream.orderedConsumer()

        let errors = ErrorCollector()
        let messages = try await ordered.consume(
            options: ConsumeOptions(expires: 2), errorHandler: { errors.append($0) })

        var received: [String] = []
        var firstName: String? = nil
        for try await msg in messages {
            received.append(payload(msg)!)
            if received.count == 5 {
                // delete the underlying consumer and publish more messages
                firstName = ordered.cachedInfo?.name
                try await ctx.deleteConsumer(stream: "test", name: firstName!)
                try await publish(ctx: ctx, from: 6, to: 10)
            }
            if received.count == 10 {
                break
            }
        }
        XCTAssertEqual(received, (1...10).map { "\($0)" })
        XCTAssertNotEqual(ordered.cachedInfo?.name, firstName)
        XCTAssertGreaterThan(errors.count, 0)
        await messages.stop()
    }

    func testOrderedFetchAndNext() async throws {
        let (ctx, _) = try await start(messages: 7)
        let ordered = ctx.orderedConsumer(stream: "test")

        var received: [String] = []
        for try await msg in try await ordered.fetch(batch: 3, expires: 1) {
            received.append(payload(msg)!)
        }
        XCTAssertEqual(received, ["1", "2", "3"])

        received = []
        for try await msg in try await ordered.fetch(batch: 3, expires: 1) {
            received.append(payload(msg)!)
        }
        XCTAssertEqual(received, ["4", "5", "6"])

        let next = try await ordered.next(expires: 1)
        XCTAssertEqual(payload(next), "7")
        let none = try await ordered.next(expires: 0.5)
        XCTAssertNil(none)
    }

    func testOrderedDeliverPolicyAndFilter() async throws {
        let (ctx, _) = try await start(messages: 3)
        try await publish(ctx: ctx, from: 4, to: 6, subject: "foo.B")

        let last = ctx.orderedConsumer(
            stream: "test", config: OrderedConsumerConfig(deliverPolicy: .last))
        let lastMsg = try await last.next(expires: 1)
        XCTAssertEqual(payload(lastMsg), "6")

        let filtered = ctx.orderedConsumer(
            stream: "test", config: OrderedConsumerConfig(filterSubjects: ["foo.B"]))
        var received: [String] = []
        for try await msg in try await filtered.fetch(batch: 10, expires: 1) {
            received.append(payload(msg)!)
        }
        XCTAssertEqual(received, ["4", "5", "6"])

        let fromSeq = ctx.orderedConsumer(
            stream: "test",
            config: OrderedConsumerConfig(deliverPolicy: .byStartSequence, optStartSeq: 5))
        let fromSeqMsg = try await fromSeq.next(expires: 1)
        XCTAssertEqual(payload(fromSeqMsg), "5")
    }
}
