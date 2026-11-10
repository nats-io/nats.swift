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

import JetStreamKeyValue
import Logging
import NatsServer
import XCTest

@testable import JetStream
@testable import JetStreamObjectStore
@testable import Nats

class ObjectStoreTests: XCTestCase {

    static let allTests = [
        ("testBucketCRUD", testBucketCRUD),
        ("testPutGetString", testPutGetString),
        ("testPutGetLargeObject", testPutGetLargeObject),
        ("testPutAsyncSequence", testPutAsyncSequence),
        ("testEmptyObject", testEmptyObject),
        ("testReplaceObject", testReplaceObject),
        ("testDelete", testDelete),
        ("testList", testList),
        ("testDigestMismatch", testDigestMismatch),
        ("testFiles", testFiles),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func start() async throws -> JetStreamContext {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        return JetStreamContext(client: client)
    }

    private func randomData(_ count: Int) -> Data {
        Data((0..<count).map { _ in UInt8.random(in: 0...255) })
    }

    func testBucketCRUD() async throws {
        let ctx = try await start()
        let missing = try await ctx.objectStore(bucket: "files")
        XCTAssertNil(missing)

        _ = try await ctx.createObjectStore(
            cfg: ObjectStoreConfig(bucket: "files", description: "my files"))
        let stream = try await ctx.getStream(name: "OBJ_files")
        XCTAssertEqual(stream?.info.config.subjects, ["$O.files.C.>", "$O.files.M.>"])
        XCTAssertEqual(stream?.info.config.allowRollup, true)
        XCTAssertEqual(stream?.info.config.allowDirect, true)
        XCTAssertEqual(stream?.info.config.discard, .new)

        do {
            _ = try await ctx.createObjectStore(
                cfg: ObjectStoreConfig(bucket: "files", description: "other"))
            XCTFail("expected bucket exists error")
        } catch JetStreamError.ObjectStoreError.bucketExists {
            // success
        }

        let updated = try await ctx.updateObjectStore(
            cfg: ObjectStoreConfig(bucket: "files", description: "updated"))
        let status = try await updated.status()
        XCTAssertEqual(status.description, "updated")
        XCTAssertFalse(status.sealed)

        do {
            _ = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "a.b"))
            XCTFail("expected invalid store name")
        } catch JetStreamError.ObjectStoreError.invalidStoreName {
            // success
        }

        try await ctx.deleteObjectStore(bucket: "files")
        let deleted = try await ctx.objectStore(bucket: "files")
        XCTAssertNil(deleted)
        do {
            try await ctx.deleteObjectStore(bucket: "files")
            XCTFail("expected bucket not found")
        } catch JetStreamError.ObjectStoreError.bucketNotFound {
            // success
        }
    }

    func testPutGetString() async throws {
        let ctx = try await start()
        let store = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))

        let info = try await store.put(
            ObjectMeta(name: "test.txt", description: "a file", metadata: ["k": "v"]),
            data: Data("hello world".utf8))
        XCTAssertEqual(info.name, "test.txt")
        XCTAssertEqual(info.bucket, "files")
        XCTAssertEqual(info.size, 11)
        XCTAssertEqual(info.chunks, 1)
        // SHA-256 of "hello world", base64url encoded like other NATS clients
        XCTAssertEqual(info.digest, "SHA-256=uU0nuZNNPgilLlLX2n2r-sSE7-N6U4DukIj3rOLvzek=")
        XCTAssertEqual(info.meta.description, "a file")
        XCTAssertEqual(info.meta.metadata, ["k": "v"])
        XCTAssertEqual(info.meta.options?.chunkSize, ObjectStore.defaultChunkSize)
        XCTAssertFalse(info.modified.isEmpty)

        // meta subject uses the base64url encoded name
        let meta = try await store.stream.getMessage(lastForSubject: "$O.files.M.dGVzdC50eHQ=")
        XCTAssertNotNil(meta)

        let value = try await store.getString("test.txt")
        XCTAssertEqual(value, "hello world")
        let missing = try await store.getString("missing")
        XCTAssertNil(missing)
    }

    func testPutGetLargeObject() async throws {
        let ctx = try await start()
        let store = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))

        let data = randomData(1024 * 1024 + 100)
        let info = try await store.put(
            ObjectMeta(name: "blob", options: ObjectMetaOptions(chunkSize: 64 * 1024)), data: data)
        XCTAssertEqual(info.chunks, 17)
        XCTAssertEqual(info.size, UInt64(data.count))

        let result = try await store.get("blob")
        var chunks = 0
        var received = Data()
        for try await chunk in result! {
            chunks += 1
            received.append(chunk)
        }
        XCTAssertEqual(chunks, 17)
        XCTAssertEqual(received, data)
    }

    func testPutAsyncSequence() async throws {
        let ctx = try await start()
        let store = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))

        // pieces of different sizes are re-chunked
        let pieces = [randomData(10), randomData(700), randomData(3), randomData(300)]
        let source = AsyncStream<Data> { continuation in
            for piece in pieces {
                continuation.yield(piece)
            }
            continuation.finish()
        }
        let info = try await store.put(
            ObjectMeta(name: "pieces", options: ObjectMetaOptions(chunkSize: 256)), data: source)
        XCTAssertEqual(info.size, 1013)
        XCTAssertEqual(info.chunks, 4)

        let data = try await store.getData("pieces")
        XCTAssertEqual(data, pieces.reduce(Data(), +))
    }

    func testEmptyObject() async throws {
        let ctx = try await start()
        let store = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))

        let info = try await store.put(name: "empty", data: Data())
        XCTAssertEqual(info.size, 0)
        XCTAssertEqual(info.chunks, 0)
        let data = try await store.getData("empty")
        XCTAssertEqual(data, Data())
    }

    func testReplaceObject() async throws {
        let ctx = try await start()
        let store = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))

        let first = try await store.put(
            ObjectMeta(name: "file", options: ObjectMetaOptions(chunkSize: 10)),
            data: randomData(100))
        let second = try await store.put(
            ObjectMeta(name: "file", options: ObjectMetaOptions(chunkSize: 10)),
            data: Data("replaced".utf8))
        XCTAssertNotEqual(first.nuid, second.nuid)

        // chunks of the first version are purged, one chunk and one meta remain
        let status = try await store.status()
        XCTAssertEqual(status.streamInfo.state.messages, 2)
        let value = try await store.getString("file")
        XCTAssertEqual(value, "replaced")
    }

    func testDelete() async throws {
        let ctx = try await start()
        let store = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))
        try await store.put(name: "file", string: "content")

        try await store.delete("file")
        let info = try await store.info("file")
        XCTAssertNil(info)
        let deleted = try await store.info("file", showDeleted: true)
        XCTAssertEqual(deleted?.deleted, true)
        XCTAssertEqual(deleted?.size, 0)
        let data = try await store.getData("file")
        XCTAssertNil(data)

        // only the delete marker remains
        let status = try await store.status()
        XCTAssertEqual(status.streamInfo.state.messages, 1)

        do {
            try await store.delete("missing")
            XCTFail("expected object not found")
        } catch JetStreamError.ObjectStoreError.objectNotFound {
            // success
        }
    }

    func testList() async throws {
        let ctx = try await start()
        let store = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))
        let empty = try await store.list()
        XCTAssertEqual(empty.count, 0)

        try await store.put(name: "a", string: "1")
        try await store.put(name: "b", string: "2")
        try await store.put(name: "c", string: "3")
        try await store.delete("b")

        let objects = try await store.list()
        XCTAssertEqual(objects.map { $0.name }, ["a", "c"])
        let all = try await store.list(showDeleted: true)
        XCTAssertEqual(all.map { $0.name }.sorted(), ["a", "b", "c"])
    }

    func testDigestMismatch() async throws {
        let ctx = try await start()
        let store = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))
        var info = try await store.put(name: "file", string: "content")

        // corrupt the stored digest
        info.digest = "SHA-256=" + ObjectStore.base64URL(Data(repeating: 0, count: 32))
        try await store.publishMeta(info)

        do {
            _ = try await store.getData("file")
            XCTFail("expected digest mismatch")
        } catch JetStreamError.ObjectStoreError.digestMismatch {
            // success
        }
    }

    func testFiles() async throws {
        let ctx = try await start()
        let store = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))

        let dir = FileManager.default.temporaryDirectory
        let source = dir.appendingPathComponent("source-\(UUID().uuidString).bin")
        let target = dir.appendingPathComponent("target-\(UUID().uuidString).bin")
        defer {
            try? FileManager.default.removeItem(at: source)
            try? FileManager.default.removeItem(at: target)
        }
        let data = randomData(300 * 1024 + 7)
        try data.write(to: source)

        let info = try await store.putFile(source)
        XCTAssertEqual(info.name, source.lastPathComponent)
        XCTAssertEqual(info.size, UInt64(data.count))
        XCTAssertEqual(info.chunks, 3)

        let read = try await store.getFile(source.lastPathComponent, to: target)
        XCTAssertEqual(read?.size, UInt64(data.count))
        XCTAssertEqual(try Data(contentsOf: target), data)

        let missing = try await store.getFile("missing", to: target)
        XCTAssertNil(missing)
    }
}
