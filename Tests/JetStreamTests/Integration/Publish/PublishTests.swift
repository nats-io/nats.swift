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

class PublishTests: XCTestCase {

    static let allTests = [
        ("testPublishSharesAckSubscription", testPublishSharesAckSubscription),
        ("testPublishAsync", testPublishAsync),
        ("testPublishAsyncMaxPending", testPublishAsyncMaxPending),
        ("testPublishRetryOnNoResponders", testPublishRetryOnNoResponders),
        ("testPublishNoRetry", testPublishNoRetry),
        (
            "testPublishExpectedLastSubjectSequenceSubject",
            testPublishExpectedLastSubjectSequenceSubject
        ),
        ("testPublishTTL", testPublishTTL),
        ("testPublishSchedule", testPublishSchedule),
    ]

    var natsServer = NatsServer()
    let payload = "hello".data(using: .utf8)!

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func connect() async throws -> (NatsClient, JetStreamContext) {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        return (client, JetStreamContext(client: client))
    }

    func testPublishSharesAckSubscription() async throws {
        let (client, ctx) = try await connect()
        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        let baseline = client.connectionHandler?.subscriptionCount ?? -1
        for _ in 0..<10 {
            _ = try await ctx.publish("foo.A", message: payload).wait()
        }
        // a single subscription receives the acknowledgements of all publishes
        XCTAssertEqual(client.connectionHandler?.subscriptionCount ?? -1, baseline + 1)
    }

    func testPublishAsync() async throws {
        let (_, ctx) = try await connect()
        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        var futures: [AckFuture] = []
        for _ in 0..<500 {
            futures.append(try await ctx.publish("foo.A", message: payload))
        }
        await ctx.publishAsyncComplete()
        let pending = await ctx.publishAsyncPending()
        XCTAssertEqual(pending, 0)

        // acknowledgements received before waiting are kept
        var sequences: [UInt64] = []
        for future in futures {
            sequences.append(try await future.wait().seq)
        }
        XCTAssertEqual(sequences, Array(1...500))
    }

    func testPublishAsyncMaxPending() async throws {
        let (client, ctx) = try await connect()
        // a plain subscriber which never replies, so publishes are never acknowledged
        let silent = try await client.subscribe(subject: "silent.>")
        await ctx.setPublishAsyncMaxPending(3, stallWait: 0.1)

        for _ in 0..<3 {
            _ = try await ctx.publish("silent.A", message: payload)
        }
        let pending = await ctx.publishAsyncPending()
        XCTAssertEqual(pending, 3)
        do {
            _ = try await ctx.publish("silent.A", message: payload)
            XCTFail("expected too many stalled messages")
        } catch JetStreamError.PublishError.tooManyStalledMessages {
            // success
        }
        try await silent.unsubscribe()
    }

    func testPublishRetryOnNoResponders() async throws {
        let (_, ctx) = try await connect()

        // stream is created while publish is retrying
        _ = Task {
            try await Task.sleep(nanoseconds: 300_000_000)
            _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))
        }
        let ack = try await ctx.publish(
            "foo.A", message: payload,
            options: PublishOptions(retryAttempts: 10, retryWait: 0.2)
        ).wait()
        XCTAssertEqual(ack.stream, "test")
        XCTAssertEqual(ack.seq, 1)
    }

    func testPublishNoRetry() async throws {
        let (_, ctx) = try await connect()

        let start = Date()
        do {
            _ = try await ctx.publish(
                "foo.A", message: payload, options: PublishOptions(retryAttempts: 0)
            ).wait()
            XCTFail("expected stream not found error")
        } catch JetStreamError.PublishError.streamNotFound {
            // success
        }
        XCTAssertLessThan(Date().timeIntervalSince(start), PublishOptions.defaultRetryWait)
    }

    func testPublishExpectedLastSubjectSequenceSubject() async throws {
        let (_, ctx) = try await connect()
        _ = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        _ = try await ctx.publish("foo.A", message: payload).wait()
        _ = try await ctx.publish("foo.B", message: payload).wait()

        // last message on foo.* is 2
        let ack = try await ctx.publish(
            "foo.C", message: payload,
            options: PublishOptions(
                expectedLastSubjectSequence: 2, expectedLastSubjectSequenceSubject: "foo.*")
        ).wait()
        XCTAssertEqual(ack.seq, 3)

        do {
            _ = try await ctx.publish(
                "foo.C", message: payload,
                options: PublishOptions(
                    expectedLastSubjectSequence: 2, expectedLastSubjectSequenceSubject: "foo.*")
            ).wait()
            XCTFail("expected wrong last sequence error")
        } catch JetStreamError.PublishError.streamWrongLastSequence {
            // success
        }
    }

    func testPublishTTL() async throws {
        let (_, ctx) = try await connect()
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"], allowMsgTTL: true))

        _ = try await ctx.publish(
            "foo.A", message: payload, options: PublishOptions(ttl: .after(1))
        ).wait()
        _ = try await ctx.publish(
            "foo.B", message: payload, options: PublishOptions(ttl: .never)
        ).wait()

        let ttl = try await stream.getMessage(sequence: 1)
        XCTAssertEqual(ttl?.headers?[.natsTTL]?.description, "1000ms")

        var info = try await stream.info()
        for _ in 0..<30 where info.state.messages != 1 {
            try await Task.sleep(nanoseconds: 100_000_000)
            info = try await stream.info()
        }
        XCTAssertEqual(info.state.messages, 1)
        XCTAssertEqual(info.state.firstSeq, 2)
    }

    func testPublishSchedule() async throws {
        let (_, ctx) = try await connect()
        let stream = try await ctx.createStream(
            cfg: StreamConfig(
                name: "test", subjects: ["schedules.*", "target"], allowMsgTTL: true,
                allowMsgSchedules: true))

        _ = try await ctx.publish(
            "schedules.once", message: payload,
            options: PublishOptions(
                schedule: .at(Date().addingTimeInterval(1)), scheduleTarget: "target",
                scheduleTTL: .after(60))
        ).wait()

        var scheduled: StreamMessage? = nil
        for _ in 0..<50 where scheduled == nil {
            try await Task.sleep(nanoseconds: 100_000_000)
            scheduled = try await stream.getMessage(lastForSubject: "target")
        }
        XCTAssertEqual(scheduled?.payload, payload)
        XCTAssertEqual(scheduled?.headers?[.natsTTL]?.description, "60000ms")

        // repeated schedules require an interval of at least 1 second
        do {
            _ = try await ctx.publish(
                "schedules.every", message: payload,
                options: PublishOptions(schedule: .every(0.5), scheduleTarget: "target")
            ).wait()
            XCTFail("expected invalid schedule error")
        } catch let err as JetStreamError.APIError {
            XCTAssertEqual(err.code, 400)
        }
    }
}
