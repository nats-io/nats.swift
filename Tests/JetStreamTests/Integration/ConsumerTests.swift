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

class ConsumerTests: XCTestCase {

    static var allTests = [
        ("testFetchWithDefaultOptions", testFetchWithDefaultOptions),
        ("testFetchConsumerDeleted", testFetchConsumerDeleted),
        ("testFetchExpires", testFetchExpires),
        ("testFetchInvalidIdleHeartbeat", testFetchInvalidIdleHeartbeat),
        ("testAck", testAck),
        ("testNak", testNak),
        ("testNakWithDelay", testNakWithDelay),
        ("testTerm", testTerm),
        ("testFetchEmptyTerminatesWithoutHang", testFetchEmptyTerminatesWithoutHang),
        ("testFetchCancellationTearsDownSubscription", testFetchCancellationTearsDownSubscription),
        ("testFetchEarlyBreakTearsDownSubscription", testFetchEarlyBreakTearsDownSubscription),
        ("testFetchCompletionTearsDownSubscription", testFetchCompletionTearsDownSubscription),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testFetchWithDefaultOptions() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        // publish some messages on stream
        for _ in 1...100 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 100)

        let batch = try await consumer.fetch(batch: 30)

        var i = 0
        for try await msg in batch {
            try await msg.ack()
            XCTAssertEqual(msg.payload, payload)
            i += 1
        }
        XCTAssertEqual(i, 30)
    }

    func testFetchConsumerDeleted() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        // publish some messages on stream
        for _ in 1...10 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        let batch = try await consumer.fetch(batch: 30)

        sleep(1)
        try await stream.deleteConsumer(name: "cons")
        var i = 0
        do {
            for try await msg in batch {
                try await msg.ack()
                XCTAssertEqual(msg.payload, payload)
                i += 1
            }
        } catch JetStreamError.FetchError.consumerDeleted {
            XCTAssertEqual(i, 10)
            return
        }
        XCTFail("should get consumer deleted")
    }

    func testFetchExpires() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        // publish some messages on stream
        for _ in 1...10 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        let batch = try await consumer.fetch(batch: 30, expires: 1)

        var i = 0
        for try await msg in batch {
            try await msg.ack()
            XCTAssertEqual(msg.payload, payload)
            i += 1
        }
        XCTAssertEqual(i, 10)
    }

    func testFetchInvalidIdleHeartbeat() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let batch = try await consumer.fetch(batch: 30, expires: 1, idleHeartbeat: 2)

        do {
            for try await _ in batch {}
        } catch JetStreamError.FetchError.badRequest {
            // success
            return
        }
        XCTFail("should get bad request")
    }

    func testFetchMissingHeartbeat() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        let payload = "hello".data(using: .utf8)!
        // publish some messages on stream
        for _ in 1...10 {
            let ack = try await ctx.publish("foo.A", message: payload)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        try await stream.deleteConsumer(name: "cons")

        let batch = try await consumer.fetch(batch: 30, idleHeartbeat: 1)

        do {
            for try await _ in batch {}
        } catch JetStreamError.FetchError.noHeartbeatReceived {
            return
        } catch JetStreamError.FetchError.noResponders {
            // This is also expected when the consumer has been deleted
            return
        }
        XCTFail("should get missing heartbeats or no responders error")
    }

    func testAck() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        // create a consumer with 500ms ack wait
        let consumer = try await stream.createConsumer(
            cfg: ConsumerConfig(name: "cons", ackWait: NanoTimeInterval(0.5)))

        // publish some messages on stream
        for i in 0..<100 {
            let ack = try await ctx.publish("foo.A", message: "\(i)".data(using: .utf8)!)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 100)

        var batch = try await consumer.fetch(batch: 10)

        var i = 0
        for try await msg in batch {
            try await msg.ack()
            XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(i)")
            i += 1
        }
        XCTAssertEqual(i, 10)

        // now wait 1 second and make sure the messages are not re-delivered
        sleep(1)

        batch = try await consumer.fetch(batch: 10)

        for try await msg in batch {
            try await msg.ack()
            XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(i)")
            i += 1
        }
        XCTAssertEqual(i, 20)
    }

    func testNak() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        // create a consumer with 500ms ack wait
        let consumer = try await stream.createConsumer(
            cfg: ConsumerConfig(name: "cons", ackWait: NanoTimeInterval(0.5)))

        // publish some messages on stream
        for i in 0..<10 {
            let ack = try await ctx.publish("foo.A", message: "\(i)".data(using: .utf8)!)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        var batch = try await consumer.fetch(batch: 1)
        var iter = batch.makeAsyncIterator()
        var msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(0)")
        var meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 1)
        XCTAssertEqual(meta.consumerSequence, 1)
        try await msg.ack(ackType: .nak())

        // Give the server time to process the NAK and requeue the message before fetching.
        // Without this, the fetch can race ahead and receive the next undelivered message
        // instead of the redelivered NAK'd one.
        try await Task.sleep(nanoseconds: 200_000_000)

        // now fetch the message again, it should be redelivered
        batch = try await consumer.fetch(batch: 1)
        iter = batch.makeAsyncIterator()
        msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(0)")
        meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 1)
        XCTAssertEqual(meta.consumerSequence, 2)
        try await msg.ack()
    }

    func testNakWithDelay() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        // create a consumer with 500ms ack wait
        let consumer = try await stream.createConsumer(cfg: ConsumerConfig(name: "cons"))

        // publish some messages on stream
        for i in 0..<10 {
            let ack = try await ctx.publish("foo.A", message: "\(i)".data(using: .utf8)!)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        var batch = try await consumer.fetch(batch: 1)
        var iter = batch.makeAsyncIterator()
        var msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(0)")
        var meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 1)
        XCTAssertEqual(meta.consumerSequence, 1)
        try await msg.ack(ackType: .nak(delay: 0.5))

        // now fetch the next message immediately, it should be the next message
        batch = try await consumer.fetch(batch: 1)
        iter = batch.makeAsyncIterator()
        msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(1)")
        meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 2)
        XCTAssertEqual(meta.consumerSequence, 2)
        try await msg.ack()

        // wait a second, the first message should be redelivered at this point
        sleep(1)
        batch = try await consumer.fetch(batch: 1)
        iter = batch.makeAsyncIterator()
        msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(0)")
        meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 1)
        XCTAssertEqual(meta.consumerSequence, 3)
    }

    func testTerm() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        // create a consumer with 500ms ack wait
        let consumer = try await stream.createConsumer(
            cfg: ConsumerConfig(name: "cons", ackWait: NanoTimeInterval(0.5)))

        // publish some messages on stream
        for i in 0..<10 {
            let ack = try await ctx.publish("foo.A", message: "\(i)".data(using: .utf8)!)
            _ = try await ack.wait()
        }
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 10)

        var batch = try await consumer.fetch(batch: 1)
        var iter = batch.makeAsyncIterator()
        var msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(0)")
        var meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 1)
        XCTAssertEqual(meta.consumerSequence, 1)
        try await msg.ack(ackType: .term())

        // wait 1s, the first message should not be redelivered (even though we are past ack wait)
        sleep(1)
        batch = try await consumer.fetch(batch: 1)
        iter = batch.makeAsyncIterator()
        msg = try await iter.next()!
        XCTAssertEqual(String(decoding: msg.payload!, as: UTF8.self), "\(1)")
        meta = try msg.metadata()
        XCTAssertEqual(meta.streamSequence, 2)
        XCTAssertEqual(meta.consumerSequence, 2)
        try await msg.ack()
    }

    func testFetchEmptyTerminatesWithoutHang() async throws {
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

        // A fetch on an empty consumer must terminate, not hang; race it against a
        // wall-clock budget that returns -1 if the fetch hung.
        let count = try await withThrowingTaskGroup(of: Int.self) { group -> Int in
            group.addTask {
                let batch = try await consumer.fetch(batch: 5, expires: 1)
                var n = 0
                for try await _ in batch { n += 1 }
                return n
            }
            group.addTask {
                try await Task.sleep(nanoseconds: 8 * 1_000_000_000)
                return -1
            }
            let first = try await group.next()!
            group.cancelAll()
            return first
        }
        XCTAssertEqual(count, 0, "empty fetch should return 0 messages (-1 means it hung)")

        // The consumer must still be usable after the empty fetch.
        _ = try await ctx.publish("foo.A", message: "hi".data(using: .utf8)!).wait()
        let batch = try await consumer.fetch(batch: 1, expires: 2)
        var got = 0
        for try await msg in batch {
            try await msg.ack()
            got += 1
        }
        XCTAssertEqual(got, 1)

        try await client.close()
    }

    func testFetchCancellationTearsDownSubscription() async throws {
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

        let baseline = client.connectionHandler?.subscriptionCount ?? -1

        // Empty consumer: the fetch blocks waiting for a message.
        let task = Task {
            let batch = try await consumer.fetch(batch: 1, expires: 30)
            for try await _ in batch {}
        }
        // Let the fetch create its inbox subscription and start waiting, then cancel.
        try await Task.sleep(nanoseconds: 500_000_000)
        task.cancel()
        _ = try? await task.value

        // Teardown runs asynchronously after cancellation; poll briefly for it.
        var count = client.connectionHandler?.subscriptionCount ?? -1
        for _ in 0..<20 where count != baseline {
            try await Task.sleep(nanoseconds: 100_000_000)
            count = client.connectionHandler?.subscriptionCount ?? -1
        }
        XCTAssertEqual(count, baseline, "cancelled fetch leaked its inbox subscription")

        try await client.close()
    }

    func testFetchEarlyBreakTearsDownSubscription() async throws {
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

        for i in 0..<5 {
            _ = try await ctx.publish("foo.\(i)", message: "hi".data(using: .utf8)!).wait()
        }

        let baseline = client.connectionHandler?.subscriptionCount ?? -1

        // Consume one message then break out early. The fetch's inbox subscription
        // must still be torn down, not leaked.
        let batch = try await consumer.fetch(batch: 5, expires: 30)
        for try await msg in batch {
            try await msg.ack()
            break
        }

        var count = client.connectionHandler?.subscriptionCount ?? -1
        for _ in 0..<20 where count != baseline {
            try await Task.sleep(nanoseconds: 100_000_000)
            count = client.connectionHandler?.subscriptionCount ?? -1
        }
        XCTAssertEqual(count, baseline, "early-break fetch leaked its inbox subscription")

        try await client.close()
    }

    func testFetchCompletionTearsDownSubscription() async throws {
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

        for i in 0..<3 {
            _ = try await ctx.publish("foo.\(i)", message: "hi".data(using: .utf8)!).wait()
        }

        let baseline = client.connectionHandler?.subscriptionCount ?? -1

        // Consume the whole batch to completion.
        let batch = try await consumer.fetch(batch: 3, expires: 5)
        var got = 0
        for try await msg in batch {
            try await msg.ack()
            got += 1
        }
        XCTAssertEqual(got, 3)

        var count = client.connectionHandler?.subscriptionCount ?? -1
        for _ in 0..<20 where count != baseline {
            try await Task.sleep(nanoseconds: 100_000_000)
            count = client.connectionHandler?.subscriptionCount ?? -1
        }
        XCTAssertEqual(count, baseline, "completed fetch leaked its inbox subscription")

        try await client.close()
    }
}
