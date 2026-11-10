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

class BatchPublishTests: XCTestCase {

    static let allTests = [
        ("testCommitBatch", testCommitBatch),
        ("testCloseBatch", testCloseBatch),
        ("testDiscardedBatchIsNotStored", testDiscardedBatchIsNotStored),
        ("testPublishBatch", testPublishBatch),
        ("testBatchNotEnabled", testBatchNotEnabled),
        ("testBatchValidation", testBatchValidation),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func start(atomic: Bool = true) async throws -> (JetStreamContext, JetStream.Stream) {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(
                name: "orders", subjects: ["orders.>"], allowAtomicPublish: atomic ? true : nil))
        return (ctx, stream)
    }

    func testCommitBatch() async throws {
        let (ctx, stream) = try await start()

        let batch = ctx.batchPublisher(flowControl: BatchFlowControl(ackEvery: 2))
        for i in 1...4 {
            try await batch.add("orders.\(i)", message: Data("order \(i)".utf8))
        }
        let size = await batch.size
        XCTAssertEqual(size, 4)

        let ack = try await batch.commit("orders.5", message: Data("order 5".utf8))
        XCTAssertEqual(ack.stream, "orders")
        XCTAssertEqual(ack.batchSize, 5)
        XCTAssertEqual(ack.seq, 5)
        XCTAssertEqual(ack.batchId, batch.batchId)
        let closed = await batch.isClosed
        XCTAssertTrue(closed)

        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 5)
        let first = try await stream.getMessage(sequence: 1)
        XCTAssertEqual(first?.headers?[.natsBatchId]?.description, batch.batchId)
    }

    func testCloseBatch() async throws {
        let (ctx, stream) = try await start()

        let batch = ctx.batchPublisher()
        try await batch.add("orders.1", message: Data("a".utf8))
        try await batch.add("orders.2", message: Data("b".utf8))
        // the end of batch marker commits without being stored
        let ack = try await batch.close()
        XCTAssertEqual(ack.batchSize, 2)

        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 2)

        do {
            try await batch.add("orders.3", message: Data())
            XCTFail("expected closed batch")
        } catch JetStreamError.BatchPublishError.closed {
            // success
        }
    }

    func testDiscardedBatchIsNotStored() async throws {
        let (ctx, stream) = try await start()

        let batch = ctx.batchPublisher()
        for i in 1...3 {
            try await batch.add("orders.\(i)", message: Data("order \(i)".utf8))
        }
        try await batch.discard()

        // messages of a batch which is never committed are not stored
        try await Task.sleep(nanoseconds: 500_000_000)
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 0)
    }

    func testPublishBatch() async throws {
        let (ctx, stream) = try await start()

        let ack = try await ctx.publishBatch([
            BatchMessage(subject: "orders.created", payload: Data("order".utf8)),
            BatchMessage(subject: "orders.reserved", payload: Data("stock".utf8)),
            BatchMessage(
                subject: "orders.charged", payload: Data("payment".utf8),
                options: BatchMessageOptions(expectedStream: "orders")),
        ])
        XCTAssertEqual(ack.batchSize, 3)
        let info = try await stream.info()
        XCTAssertEqual(info.state.messages, 3)

        // a failed expectation rejects the whole batch
        do {
            try await ctx.publishBatch([
                BatchMessage(
                    subject: "orders.created", payload: Data(),
                    options: BatchMessageOptions(expectedLastSequence: 1)),
                BatchMessage(subject: "orders.charged", payload: Data()),
            ])
            XCTFail("expected rejected batch")
        } catch let error as JetStreamError.BatchPublishError {
            if case .rejected = error {
                // success
            } else {
                XCTFail("unexpected error \(error)")
            }
        }
        let after = try await stream.info()
        XCTAssertEqual(after.state.messages, 3)
    }

    func testBatchNotEnabled() async throws {
        let (ctx, _) = try await start(atomic: false)

        do {
            try await ctx.publishBatch([
                BatchMessage(subject: "orders.1", payload: Data()),
                BatchMessage(subject: "orders.2", payload: Data()),
            ])
            XCTFail("expected batch publish not enabled")
        } catch JetStreamError.BatchPublishError.notEnabled {
            // success
        }
    }

    func testBatchValidation() async throws {
        let (ctx, _) = try await start()

        do {
            try await ctx.publishBatch([])
            XCTFail("expected empty batch")
        } catch JetStreamError.BatchPublishError.emptyBatch {
            // success
        }

        let empty = ctx.batchPublisher()
        do {
            _ = try await empty.close()
            XCTFail("expected empty batch")
        } catch JetStreamError.BatchPublishError.emptyBatch {
            // success
        }

        let batch = ctx.batchPublisher()
        try await batch.add("orders.1", message: Data())
        do {
            try await batch.add(
                "orders.2", message: Data(),
                options: BatchMessageOptions(expectedLastSequence: 1))
            XCTFail("expected last sequence not first")
        } catch JetStreamError.BatchPublishError.expectedLastSequenceNotFirst {
            // success
        }
    }
}
