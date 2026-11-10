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

import Logging
import NatsServer
import XCTest

@testable import JetStream
@testable import JetStreamKeyValue
@testable import Nats

class KeyValueTests: XCTestCase {

    static let allTests = [
        ("testBucketCRUD", testBucketCRUD),
        ("testBucketValidation", testBucketValidation),
        ("testPutGet", testPutGet),
        ("testCreateUpdate", testCreateUpdate),
        ("testDeletePurge", testDeletePurge),
        ("testKeyTTL", testKeyTTL),
        ("testStatus", testStatus),
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

    private func string(_ entry: KeyValueEntry?) -> String? {
        entry.flatMap { String(data: $0.value, encoding: .utf8) }
    }

    func testBucketCRUD() async throws {
        let ctx = try await start()

        let missing = try await ctx.keyValue(bucket: "config")
        XCTAssertNil(missing)

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "config", history: 5))
        XCTAssertEqual(kv.bucket, "config")
        let stream = try await ctx.getStream(name: "KV_config")
        XCTAssertEqual(stream?.info.config.subjects, ["$KV.config.>"])
        XCTAssertEqual(stream?.info.config.maxMsgsPerSubject, 5)
        XCTAssertEqual(stream?.info.config.allowRollup, true)
        XCTAssertEqual(stream?.info.config.denyDelete, true)
        XCTAssertEqual(stream?.info.config.allowDirect, true)
        XCTAssertEqual(stream?.info.config.discard, .new)

        // same config is idempotent, a different one is rejected
        _ = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "config", history: 5))
        do {
            _ = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "config", history: 10))
            XCTFail("expected bucket exists error")
        } catch JetStreamError.KeyValueError.bucketExists {
            // success
        }

        let updated = try await ctx.updateKeyValue(
            cfg: KeyValueConfig(bucket: "config", history: 10))
        let status = try await updated.status()
        XCTAssertEqual(status.history, 10)

        _ = try await ctx.createOrUpdateKeyValue(cfg: KeyValueConfig(bucket: "other"))
        let other = try await ctx.keyValue(bucket: "other")
        XCTAssertNotNil(other)

        try await ctx.deleteKeyValue(bucket: "config")
        let deleted = try await ctx.keyValue(bucket: "config")
        XCTAssertNil(deleted)
        do {
            try await ctx.deleteKeyValue(bucket: "config")
            XCTFail("expected bucket not found error")
        } catch JetStreamError.KeyValueError.bucketNotFound {
            // success
        }
        do {
            _ = try await ctx.updateKeyValue(cfg: KeyValueConfig(bucket: "config"))
            XCTFail("expected bucket not found error")
        } catch JetStreamError.KeyValueError.bucketNotFound {
            // success
        }
    }

    func testBucketValidation() async throws {
        let ctx = try await start()

        for bucket in ["", "a.b", "a b", "a*", "ą"] {
            do {
                _ = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: bucket))
                XCTFail("expected invalid bucket name: \(bucket)")
            } catch JetStreamError.KeyValueError.invalidBucketName {
                // success
            }
        }
        do {
            _ = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "kv", history: 65))
            XCTFail("expected history too large")
        } catch JetStreamError.KeyValueError.historyTooLarge {
            // success
        }

        // a stream which is not a key-value bucket
        _ = try await ctx.createStream(
            cfg: StreamConfig(name: "KV_bad", subjects: ["bad"], maxMsgsPerSubject: 0))
        do {
            _ = try await ctx.keyValue(bucket: "bad")
            XCTFail("expected bad bucket")
        } catch JetStreamError.KeyValueError.badBucket {
            // success
        }

        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "kv"))
        for key in ["", ".a", "a.", "a..b", "a b", "a*", "a>", "ą"] {
            do {
                try await kv.put(key, "value")
                XCTFail("expected invalid key: \(key)")
            } catch JetStreamError.KeyValueError.invalidKey {
                // success
            }
        }
        try await kv.put("a-b_c/d=e.f", "valid")
    }

    func testPutGet() async throws {
        let ctx = try await start()
        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "kv", history: 5))

        let first = try await kv.put("name", "first")
        let second = try await kv.put("name", "second")
        XCTAssertEqual(second, first + 1)

        let latest = try await kv.get("name")
        XCTAssertEqual(string(latest), "second")
        XCTAssertEqual(latest?.revision, second)
        XCTAssertEqual(latest?.operation, .put)
        XCTAssertEqual(latest?.bucket, "kv")
        XCTAssertEqual(latest?.key, "name")

        let old = try await kv.get("name", revision: first)
        XCTAssertEqual(string(old), "first")

        // revision of a different key
        let otherRevision = try await kv.put("other", "value")
        let wrongKey = try await kv.get("name", revision: otherRevision)
        XCTAssertNil(wrongKey)

        let missing = try await kv.get("missing")
        XCTAssertNil(missing)
        let missingRevision = try await kv.get("name", revision: 1000)
        XCTAssertNil(missingRevision)
    }

    func testCreateUpdate() async throws {
        let ctx = try await start()
        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "kv", history: 5))

        let created = try await kv.create("name", Data("first".utf8))
        do {
            try await kv.create("name", Data("again".utf8))
            XCTFail("expected key exists error")
        } catch JetStreamError.KeyValueError.keyExists {
            // success
        }

        let updated = try await kv.update("name", Data("second".utf8), revision: created)
        do {
            try await kv.update("name", Data("stale".utf8), revision: created)
            XCTFail("expected wrong last revision error")
        } catch JetStreamError.KeyValueError.wrongLastRevision {
            // success
        }
        let latest = try await kv.get("name")
        XCTAssertEqual(latest?.revision, updated)

        // deleted keys can be created again
        try await kv.delete("name")
        let recreated = try await kv.create("name", Data("third".utf8))
        let recreatedEntry = try await kv.get("name")
        XCTAssertEqual(recreatedEntry?.revision, recreated)
        XCTAssertEqual(string(recreatedEntry), "third")
    }

    func testDeletePurge() async throws {
        let ctx = try await start()
        let kv = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "kv", history: 10))

        for i in 1...3 {
            try await kv.put("a", "\(i)")
            try await kv.put("b", "\(i)")
        }

        try await kv.delete("a")
        let deleted = try await kv.get("a")
        XCTAssertNil(deleted)
        let marker = try await kv.entry("a")
        XCTAssertEqual(marker?.operation, .delete)
        // history is kept on delete
        let history = try await kv.get("a", revision: 1)
        XCTAssertEqual(string(history), "1")

        try await kv.purge("b")
        let purged = try await kv.entry("b")
        XCTAssertEqual(purged?.operation, .purge)
        // 3 values of a, a delete marker and a purge marker
        let status = try await kv.status()
        XCTAssertEqual(status.values, 5)

        // deletes with an expected revision
        let revision = try await kv.put("c", "1")
        do {
            try await kv.delete("c", lastRevision: revision - 1)
            XCTFail("expected wrong last revision error")
        } catch JetStreamError.KeyValueError.wrongLastRevision {
            // success
        }
        try await kv.purge("c", lastRevision: revision)
        let purgedC = try await kv.get("c")
        XCTAssertNil(purgedC)
    }

    func testKeyTTL() async throws {
        let ctx = try await start()
        let kv = try await ctx.createKeyValue(
            cfg: KeyValueConfig(bucket: "kv", limitMarkerTTL: 1))

        try await kv.create("session", Data("token".utf8), ttl: 1)
        let created = try await kv.get("session")
        XCTAssertNotNil(created)

        var expired: KeyValueEntry? = created
        for _ in 0..<30 where expired != nil {
            try await Task.sleep(nanoseconds: 100_000_000)
            expired = try await kv.get("session")
        }
        XCTAssertNil(expired)

        // the key can be created again after it expired
        try await kv.create("session", Data("new".utf8))
    }

    func testStatus() async throws {
        let ctx = try await start()
        let kv = try await ctx.createKeyValue(
            cfg: KeyValueConfig(
                bucket: "kv", history: 3, ttl: 60, compression: true, metadata: ["owner": "test"]))
        try await kv.put("a", "1")

        let status = try await kv.status()
        XCTAssertEqual(status.bucket, "kv")
        XCTAssertEqual(status.values, 1)
        XCTAssertEqual(status.history, 3)
        XCTAssertEqual(status.ttl, NanoTimeInterval(60))
        XCTAssertTrue(status.isCompressed)
        XCTAssertGreaterThan(status.bytes, 0)
        XCTAssertEqual(status.metadata?["owner"], "test")
        XCTAssertEqual(status.config.history, 3)
        XCTAssertEqual(status.config.ttl, 60)
        XCTAssertTrue(status.config.compression)
        XCTAssertEqual(status.streamInfo.config.duplicates, NanoTimeInterval(60))
    }
}
