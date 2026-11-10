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

class PullTests: XCTestCase {

    static let allTests = [
        ("testNext", testNext),
        ("testFetchNoWait", testFetchNoWait),
        ("testFetchMaxBytes", testFetchMaxBytes),
        ("testFetchOverflow", testFetchOverflow),
        ("testFetchPinned", testFetchPinned),
    ]

    var natsServer = NatsServer()
    let payload = Data(repeating: 1, count: 100)

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func start(
        consumer cfg: ConsumerConfig, messages: Int
    ) async throws -> (JetStreamContext, Consumer) {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        for _ in 0..<messages {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }
        return (ctx, try await stream.createConsumer(cfg: cfg))
    }

    func testNext() async throws {
        let (_, consumer) = try await start(consumer: ConsumerConfig(name: "cons"), messages: 2)

        let first = try await consumer.next(expires: 1)
        XCTAssertEqual(try first?.metadata().streamSequence, 1)
        try await first?.ack()
        let second = try await consumer.next(expires: 1)
        XCTAssertEqual(try second?.metadata().streamSequence, 2)
        try await second?.ack()

        let start = Date()
        let none = try await consumer.next(expires: 0.5)
        XCTAssertNil(none)
        XCTAssertGreaterThanOrEqual(Date().timeIntervalSince(start), 0.5)
    }

    func testFetchNoWait() async throws {
        let (_, consumer) = try await start(consumer: ConsumerConfig(name: "cons"), messages: 3)

        var received = 0
        for try await msg in try await consumer.fetchNoWait(batch: 10) {
            try await msg.ack()
            received += 1
        }
        XCTAssertEqual(received, 3)

        // no messages available, returns without waiting
        let start = Date()
        for try await _ in try await consumer.fetchNoWait(batch: 10) {
            XCTFail("expected no messages")
        }
        XCTAssertLessThan(Date().timeIntervalSince(start), 0.5)
    }

    func testFetchMaxBytes() async throws {
        let (_, consumer) = try await start(consumer: ConsumerConfig(name: "cons"), messages: 10)

        // each message is a bit over 100 bytes, so only 2 fit
        var received = 0
        for try await msg in try await consumer.fetchBytes(maxBytes: 300, expires: 1) {
            try await msg.ack()
            received += 1
        }
        XCTAssertEqual(received, 2)

        received = 0
        for try await msg in try await consumer.fetch(batch: 10, maxBytes: 300, expires: 1) {
            try await msg.ack()
            received += 1
        }
        XCTAssertEqual(received, 2)
    }

    func testFetchOverflow() async throws {
        let (_, consumer) = try await start(
            consumer: ConsumerConfig(
                name: "cons", priorityPolicy: .overflow, priorityGroups: ["A"]),
            messages: 5)

        // only 5 messages pending, below the threshold
        var received = 0
        let overflow = PriorityGroupOptions(group: "A", minPending: 10)
        for try await _ in try await consumer.fetch(
            batch: 5, expires: 0.5, priorityGroup: overflow)
        {
            received += 1
        }
        XCTAssertEqual(received, 0)

        for try await _ in try await consumer.fetch(
            batch: 5, expires: 0.5, priorityGroup: PriorityGroupOptions(group: "A", minPending: 5))
        {
            received += 1
        }
        // delivery stops as soon as pending drops below the threshold
        XCTAssertEqual(received, 1)

        // unknown priority group
        do {
            for try await _ in try await consumer.fetch(
                batch: 1, expires: 0.5, priorityGroup: PriorityGroupOptions(group: "B"))
            {}
            XCTFail("expected bad request")
        } catch JetStreamError.FetchError.badRequest {
            // success
        }
    }

    func testFetchPinned() async throws {
        let (ctx, consumer) = try await start(
            consumer: ConsumerConfig(name: "cons", priorityPolicy: .pinned, priorityGroups: ["A"]),
            messages: 4)
        let group = PriorityGroupOptions(group: "A")

        // first request gets pinned
        let first = try await consumer.next(expires: 1, priorityGroup: group)
        XCTAssertNotNil(first?.headers?[.natsPinId])
        try await first?.ack()

        // a different client is not served while the pin is active
        let other = try await ctx.getConsumer(stream: "test", name: "cons")!
        let notPinned = try await other.next(expires: 0.5, priorityGroup: group)
        XCTAssertNil(notPinned)

        let second = try await consumer.next(expires: 1, priorityGroup: group)
        XCTAssertNotNil(second)
        try await second?.ack()

        // after unpinning, the old pin ID is rejected
        try await ctx.unpinConsumer(stream: "test", name: "cons", group: "A")
        do {
            _ = try await consumer.next(expires: 1, priorityGroup: group)
            XCTFail("expected pin ID mismatch")
        } catch JetStreamError.FetchError.pinIdMismatch {
            // success
        }

        // pin is reset, so the next request can get pinned again
        let third = try await consumer.next(expires: 1, priorityGroup: group)
        XCTAssertNotNil(third?.headers?[.natsPinId])
    }
}
