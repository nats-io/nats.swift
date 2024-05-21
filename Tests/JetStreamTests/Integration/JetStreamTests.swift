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
import NIO
import Nats
import NatsServer
import XCTest

class JetStreamTests: XCTestCase {

    static var allTests = [
        ("testJetStreamContext", testJetStreamContext),
        ("testRequest", testRequest),
        ("testStreamCRUD", testStreamCRUD),
        ("testStreamConfig", testStreamConfig),
        ("testStreamInfo", testStreamInfo),
        ("testListStreams", testListStreams),
        ("testGetMsg", testGetMsg),
        ("testGetMsgDirect", testGetMsgDirect),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testJetStreamContext() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        _ = JetStreamContext(client: client)
        _ = JetStreamContext(client: client, prefix: "$JS.API")
        _ = JetStreamContext(client: client, domain: "STREAMS")
        _ = JetStreamContext(client: client, timeout: 10)
        let ctx = JetStreamContext(client: client)

        let stream = """
            {
                "name": "FOO",
                "subjects": ["foo"]
            }
            """
        let data = stream.data(using: .utf8)!

        _ = try await client.request(data, subject: "$JS.API.STREAM.CREATE.FOO")
        let ack = try await ctx.publish("foo", message: "Hello, World!".data(using: .utf8)!)
        _ = try await ack.wait()

        try await client.close()
    }

    func testRequest() async throws {

        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let stream = """
            {
                "name": "FOO",
                "subjects": ["foo"]
            }
            """
        let data = stream.data(using: .utf8)!

        _ = try await client.request(data, subject: "$JS.API.STREAM.CREATE.FOO")

        let info: Response<AccountInfo> = try await ctx.request("INFO", message: Data())

        guard case .success(let info) = info else {
            XCTFail("request should be successful")
            return
        }

        XCTAssertEqual(info.streams, 1)
        let badInfo: Response<AccountInfo> = try await ctx.request(
            "STREAM.INFO.BAD", message: Data())
        guard case .error(let jetStreamAPIResponse) = badInfo else {
            XCTFail("should get error")
            return
        }

        XCTAssertEqual(ErrorCode.streamNotFound, jetStreamAPIResponse.error.errorCode)

    }

    func testStreamCRUD() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        // minimal config
        var cfg = StreamConfig(name: "test", subjects: ["foo"])
        var stream = try await ctx.createStream(cfg: cfg)

        var expectedConfig = StreamConfig(
            name: "test", description: nil, subjects: ["foo"], retention: .limits, maxConsumers: -1,
            maxMsgs: -1, maxBytes: -1, discard: .old, discardNewPerSubject: nil,
            maxAge: NanoTimeInterval(0), maxMsgsPerSubject: -1, maxMsgSize: -1, storage: .file,
            replicas: 1, noAck: nil, duplicates: NanoTimeInterval(120), placement: nil, mirror: nil,
            sources: nil, sealed: false, denyDelete: false, denyPurge: false, allowRollup: false,
            compression: StoreCompression.none, firstSeq: nil, subjectTransform: nil,
            rePublish: nil, allowDirect: false, mirrorDirect: false,
            consumerLimits: StreamConsumerLimits(inactiveThreshold: nil, maxAckPending: nil),
            metadata: nil)

        XCTAssertEqual(expectedConfig, stream.info.config)

        // attempt overwriting existing stream
        do {
            _ = try await ctx.createStream(
                cfg: StreamConfig(name: "test", description: "cannot update with create"))
        } catch let err as JetStreamError {
            XCTAssertEqual(err.errorCode, .streamNameExist)
        }

        // get a stream
        stream = try await ctx.getStream(name: "test")
        XCTAssertEqual(expectedConfig, stream.info.config)

        // get a non-existing stream
        do {
            stream = try await ctx.getStream(name: "bad")
        } catch let err as JetStreamError {
            XCTAssertEqual(err.errorCode, .streamNotFound)
        }

        // update the stream
        cfg.description = "updated"
        stream = try await ctx.updateStream(cfg: cfg)
        expectedConfig.description = "updated"

        XCTAssertEqual(expectedConfig, stream.info.config)

        // attempt updating non-existing stream
        do {
            _ = try await ctx.updateStream(cfg: StreamConfig(name: "bad"))
        } catch let err as JetStreamError {
            XCTAssertEqual(err.errorCode, .streamNotFound)
        }

        // delete the stream
        try await ctx.deleteStream(name: "test")

        // make sure the stream no longer exists
        do {
            stream = try await ctx.getStream(name: "test")
        } catch let err as JetStreamError {
            XCTAssertEqual(err.errorCode, .streamNotFound)
        }
    }

    func testStreamConfig() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let cfg = StreamConfig(
            name: "full", description: "desc", subjects: ["bar"], retention: .interest,
            maxConsumers: 50, maxMsgs: 100, maxBytes: 1000, discard: .new,
            discardNewPerSubject: true, maxAge: NanoTimeInterval(300), maxMsgsPerSubject: 50,
            maxMsgSize: 100, storage: .memory, replicas: 1, noAck: true,
            duplicates: NanoTimeInterval(120), placement: Placement(cluster: "cluster"),
            mirror: nil, sources: [StreamSource(name: "source")], sealed: false, denyDelete: false,
            denyPurge: true, allowRollup: false, compression: .s2, firstSeq: 10,
            subjectTransform: nil, rePublish: nil, allowDirect: false, mirrorDirect: false,
            consumerLimits: StreamConsumerLimits(inactiveThreshold: NanoTimeInterval(10)),
            metadata: ["key": "value"])

        let stream = try await ctx.createStream(cfg: cfg)

        XCTAssertEqual(stream.info.config, cfg)
    }

    func testStreamInfo() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        // minimal config
        let cfg = StreamConfig(name: "test", subjects: ["foo"])
        let stream = try await ctx.createStream(cfg: cfg)

        let info = try await stream.info()
        XCTAssertEqual(info.config.name, "test")

        // simulate external update of stream
        let updateJSON = """
            {
                "name": "test",
                "subjects": ["foo"],
                "description": "updated"
            }
            """
        let data = updateJSON.data(using: .utf8)!

        _ = try await client.request(data, subject: "$JS.API.STREAM.UPDATE.test")

        XCTAssertNil(stream.info.config.description)

        let newInfo = try await stream.info()
        XCTAssertEqual(newInfo.config.description, "updated")
        XCTAssertEqual(stream.info.config.description, "updated")
    }

    func testListStreams() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        for i in 0..<260 {
            let subPrefix = i % 2 == 0 ? "foo" : "bar"
            let cfg = StreamConfig(name: "STREAM-\(i)", subjects: ["\(subPrefix).\(i)"])
            let _ = try await ctx.createStream(cfg: cfg)
        }

        // list all streams
        var streams = await ctx.streams()

        var i = 0
        for try await _ in streams {
            i += 1
        }
        XCTAssertEqual(i, 260)

        var names = await ctx.streamNames()
        i = 0
        for try await _ in names {
            i += 1
        }
        XCTAssertEqual(i, 260)

        // list streams with subject foo.*
        streams = await ctx.streams(subject: "foo.*")

        i = 0
        for try await stream in streams {
            XCTAssert(stream.config.subjects!.first!.starts(with: "foo."))
            i += 1
        }
        XCTAssertEqual(i, 130)

        names = await ctx.streamNames(subject: "foo.*")
        i = 0
        for try await _ in names {
            i += 1
        }
        XCTAssertEqual(i, 130)

        // list streams with subject not matching any
        streams = await ctx.streams(subject: "baz.*")

        i = 0
        for try await stream in streams {
            XCTFail("should return 0 streams, got: \(stream.config)")
        }
        XCTAssertEqual(i, 0)

        names = await ctx.streamNames(subject: "baz.*")
        i = 0
        for try await _ in names {
            i += 1
        }
        XCTAssertEqual(i, 0)
    }

    func testGetMsg() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let cfg = StreamConfig(name: "STREAM", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: cfg)

        var hm = NatsHeaderMap()
        hm[try! NatsHeaderName("foo")] = NatsHeaderValue("bar")
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("baz"))
        hm[try! NatsHeaderName("key")] = NatsHeaderValue("val")
        for i in 1...100 {
            let msg = "\(i)".data(using: .utf8)!
            let subj = i % 2 == 0 ? "foo.A" : "foo.B"
            let ack = try await ctx.publish(subj, message: msg, headers: hm)
            _ = try await ack.wait()
        }

        // get by sequence
        var msg = try await stream.getMsg(sequence: 50)
        XCTAssertEqual(msg!.payload, "50".data(using: .utf8)!)

        // get by sequence and subject
        msg = try await stream.getMsg(sequence: 50, subject: "foo.B")
        // msg with sequence 50 is on subject foo.A, so we expect the next message which should be on foo.B
        XCTAssertEqual(msg!.payload, "51".data(using: .utf8)!)
        XCTAssertEqual(msg!.headers, hm)

        // get first message from a subject
        msg = try await stream.getMsg(firstForSubject: "foo.A")
        XCTAssertEqual(msg!.payload, "2".data(using: .utf8)!)
        XCTAssertEqual(msg!.headers, hm)

        // get last message from subject
        msg = try await stream.getMsg(lastForSubject: "foo.B")
        XCTAssertEqual(msg!.payload, "99".data(using: .utf8)!)
        XCTAssertEqual(msg!.headers, hm)

        // message not found
        msg = try await stream.getMsg(sequence: 200)
        XCTAssertNil(msg)
    }

    func testGetMsgDirect() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let cfg = StreamConfig(name: "STREAM", subjects: ["foo.*"], allowDirect: true)
        let stream = try await ctx.createStream(cfg: cfg)

        var hm = NatsHeaderMap()
        hm[try! NatsHeaderName("foo")] = NatsHeaderValue("bar")
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("baz"))
        hm[try! NatsHeaderName("key")] = NatsHeaderValue("val")
        for i in 1...100 {
            let msg = "\(i)".data(using: .utf8)!
            let subj = i % 2 == 0 ? "foo.A" : "foo.B"
            let ack = try await ctx.publish(subj, message: msg, headers: hm)
            _ = try await ack.wait()
        }

        // get by sequence
        var msg = try await stream.getMsgDirect(sequence: 50)
        XCTAssertEqual(msg!.payload, "50".data(using: .utf8)!)

        // get by sequence and subject
        msg = try await stream.getMsgDirect(sequence: 50, subject: "foo.B")
        // msg with sequence 50 is on subject foo.A, so we expect the next message which should be on foo.B
        XCTAssertEqual(msg!.payload, "51".data(using: .utf8)!)

        // get first message from a subject
        msg = try await stream.getMsgDirect(firstForSubject: "foo.A")
        XCTAssertEqual(msg!.payload, "2".data(using: .utf8)!)

        // get last message from subject
        msg = try await stream.getMsgDirect(lastForSubject: "foo.B")
        XCTAssertEqual(msg!.payload, "99".data(using: .utf8)!)

        // message not found
        msg = try await stream.getMsgDirect(sequence: 200)
        XCTAssertNil(msg)
    }
}
