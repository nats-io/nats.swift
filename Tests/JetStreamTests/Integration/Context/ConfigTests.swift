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

class ConfigTests: XCTestCase {

    static let allTests = [
        ("testStreamConfigNewFields", testStreamConfigNewFields),
        ("testStreamConfigCounter", testStreamConfigCounter),
        ("testConsumerConfigPriorityAndPause", testConsumerConfigPriorityAndPause),
        ("testConsumerConfigPush", testConsumerConfigPush),
        ("testAccountInfoDetails", testAccountInfoDetails),
        ("testCreateOrUpdateStream", testCreateOrUpdateStream),
        ("testStreamNameBySubject", testStreamNameBySubject),
        ("testDoubleAck", testDoubleAck),
        ("testAckOnce", testAckOnce),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func connect() async throws -> JetStreamContext {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        return JetStreamContext(client: client)
    }

    func testStreamConfigNewFields() async throws {
        let ctx = try await connect()

        let cfg = StreamConfig(
            name: "test", subjects: ["foo.*"], allowRollup: true, allowDirect: true,
            allowMsgTTL: true, subjectDeleteMarkerTTL: NanoTimeInterval(60),
            allowAtomicPublish: true, allowMsgSchedules: true, allowBatchPublish: true)
        let stream = try await ctx.createStream(cfg: cfg)

        XCTAssertEqual(stream.info.config.allowMsgTTL, true)
        XCTAssertEqual(stream.info.config.subjectDeleteMarkerTTL, NanoTimeInterval(60))
        XCTAssertEqual(stream.info.config.allowAtomicPublish, true)
        XCTAssertEqual(stream.info.config.allowMsgSchedules, true)
        XCTAssertEqual(stream.info.config.allowBatchPublish, true)
        XCTAssertNil(stream.info.state.lost)
    }

    func testStreamConfigCounter() async throws {
        let ctx = try await connect()

        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "counter", subjects: ["cnt.*"], allowMsgCounter: true))
        XCTAssertEqual(stream.info.config.allowMsgCounter, true)

        // async persist mode cannot be combined with atomic publish
        let asyncStream = try await ctx.createStream(
            cfg: StreamConfig(name: "async", subjects: ["async.*"], persistMode: .async))
        XCTAssertEqual(asyncStream.info.config.persistMode, .async)
    }

    func testConsumerConfigPriorityAndPause() async throws {
        let ctx = try await connect()
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let pauseUntil = ISO8601DateFormatter().string(from: Date().addingTimeInterval(3600))
        let consumer = try await stream.createConsumer(
            cfg: ConsumerConfig(
                name: "cons", pauseUntil: pauseUntil, priorityPolicy: .pinned,
                pinnedTTL: NanoTimeInterval(30), priorityGroups: ["A"]))

        XCTAssertEqual(consumer.info.config.priorityPolicy, .pinned)
        XCTAssertEqual(consumer.info.config.pinnedTTL, NanoTimeInterval(30))
        XCTAssertEqual(consumer.info.config.priorityGroups, ["A"])
        XCTAssertNotNil(consumer.info.config.pauseUntil)
        XCTAssertEqual(consumer.info.paused, true)
        XCTAssertNotNil(consumer.info.pauseRemaining)
        XCTAssertEqual(consumer.info.priorityGroups?.first?.group, "A")
    }

    func testConsumerConfigPush() async throws {
        let ctx = try await connect()
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let consumer = try await stream.createConsumer(
            cfg: ConsumerConfig(
                durable: "push", deliverSubject: "deliver.push", deliverGroup: "workers",
                flowControl: true, idleHeartbeat: NanoTimeInterval(5)))

        XCTAssertEqual(consumer.info.config.deliverSubject, "deliver.push")
        XCTAssertEqual(consumer.info.config.deliverGroup, "workers")
        XCTAssertEqual(consumer.info.config.flowControl, true)
        XCTAssertEqual(consumer.info.config.idleHeartbeat, NanoTimeInterval(5))
    }

    func testAccountInfoDetails() async throws {
        let ctx = try await connect()
        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let info = try await ctx.accountInfo()
        XCTAssertEqual(info.streams, 1)
        XCTAssertNotNil(info.limits)
        XCTAssertNotNil(info.api)
        XCTAssertGreaterThan(info.api?.total ?? 0, 0)
    }

    func testCreateOrUpdateStream() async throws {
        let ctx = try await connect()

        var cfg = StreamConfig(name: "test", subjects: ["foo.*"])
        var stream = try await ctx.createOrUpdateStream(cfg: cfg)
        XCTAssertNil(stream.info.config.description)

        cfg.description = "updated"
        stream = try await ctx.createOrUpdateStream(cfg: cfg)
        XCTAssertEqual(stream.info.config.description, "updated")

        // non-updatable property
        cfg.storage = .memory
        do {
            _ = try await ctx.createOrUpdateStream(cfg: cfg)
            XCTFail("expected invalid config error")
        } catch JetStreamError.StreamError.invalidConfig {
            // success
        }
    }

    func testStreamNameBySubject() async throws {
        let ctx = try await connect()
        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        _ = try await ctx.createStream(cfg: StreamConfig(name: "other", subjects: ["bar"]))

        let name = try await ctx.streamNameBySubject("foo.A")
        XCTAssertEqual(name, "test")
        let otherName = try await ctx.streamNameBySubject("bar")
        XCTAssertEqual(otherName, "other")
        let missing = try await ctx.streamNameBySubject("baz")
        XCTAssertNil(missing)
    }

    func testDoubleAck() async throws {
        let ctx = try await connect()
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        for _ in 1...5 {
            _ = try await ctx.publish("foo.A", message: "hello".data(using: .utf8)!).wait()
        }

        for try await msg in try await consumer.fetch(batch: 5, expires: 1) {
            try await msg.doubleAck()
        }

        // no need to wait, acks are confirmed by the server
        let info = try await consumer.info()
        XCTAssertEqual(info.numAckPending, 0)
        XCTAssertEqual(info.ackFloor.stream, 5)
    }

    func testAckOnce() async throws {
        let ctx = try await connect()
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))
        _ = try await ctx.publish("foo.A", message: "hello".data(using: .utf8)!).wait()

        let msg = try await consumer.next(expires: 1)!
        // in progress can be sent any number of times
        try await msg.ack(ackType: .inProgress)
        try await msg.ack(ackType: .inProgress)
        try await msg.doubleAck()

        // copies share the acknowledgement state
        let copy = msg
        for ackType in [AckKind.ack, .nak(), .term(), .inProgress] {
            do {
                try await copy.ack(ackType: ackType)
                XCTFail("expected already acknowledged for \(ackType)")
            } catch JetStreamError.AckError.alreadyAcknowledged {
                // success
            }
        }
    }
}
