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
import JetStreamKeyValue
import Logging
import NatsServer
import XCTest

@testable import Nats

class KeyValueWatchTests: XCTestCase {

    static let allTests = [
        ("testWatchAll", testWatchAll),
        ("testWatchEmptyBucket", testWatchEmptyBucket),
        ("testWatchFiltered", testWatchFiltered),
        ("testWatchOptions", testWatchOptions),
        ("testKeys", testKeys),
        ("testHistory", testHistory),
        ("testPurgeDeletes", testPurgeDeletes),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func start(history: UInt8 = 1) async throws -> KeyValue {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let ctx = JetStreamContext(client: client)
        return try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "kv", history: history))
    }

    /// Collects watcher elements, using "<init>" for the initial values marker.
    private func collect(
        _ watcher: KeyValueWatcher, count: Int, while action: (() async throws -> Void)? = nil
    ) async throws -> [String] {
        var events: [String] = []
        var acted = false
        for try await entry in watcher {
            if let entry {
                let value = String(data: entry.value, encoding: .utf8) ?? ""
                events.append("\(entry.operation.rawValue) \(entry.key)=\(value)")
            } else {
                events.append("<init>")
            }
            if events.count == count {
                break
            }
            if !acted, entry == nil, let action {
                acted = true
                try await action()
            }
        }
        await watcher.stop()
        return events
    }

    func testWatchAll() async throws {
        let kv = try await start()
        try await kv.put("a", "1")
        try await kv.put("b", "1")
        try await kv.put("a", "2")

        let watcher = try await kv.watchAll()
        let events = try await collect(watcher, count: 6) {
            try await kv.put("c", "1")
            try await kv.delete("a")
            try await kv.purge("b")
        }
        XCTAssertEqual(
            events, ["PUT b=1", "PUT a=2", "<init>", "PUT c=1", "DEL a=", "PURGE b="])
    }

    func testWatchEmptyBucket() async throws {
        let kv = try await start()
        let watcher = try await kv.watchAll()
        let events = try await collect(watcher, count: 2) {
            try await kv.put("a", "1")
        }
        XCTAssertEqual(events, ["<init>", "PUT a=1"])
    }

    func testWatchFiltered() async throws {
        let kv = try await start()
        try await kv.put("users.1.name", "alice")
        try await kv.put("users.2.name", "bob")
        try await kv.put("groups.1.name", "admins")
        try await kv.put("users.1.age", "30")

        let wildcard = try await kv.watch("users.*.name")
        let wildcardEvents = try await collect(wildcard, count: 3)
        XCTAssertEqual(
            wildcardEvents, ["PUT users.1.name=alice", "PUT users.2.name=bob", "<init>"])

        let multiple = try await kv.watch(["groups.>", "users.1.age"])
        let multipleEvents = try await collect(multiple, count: 3)
        XCTAssertEqual(
            multipleEvents, ["PUT groups.1.name=admins", "PUT users.1.age=30", "<init>"])

        do {
            _ = try await kv.watch("users..name")
            XCTFail("expected invalid key error")
        } catch JetStreamError.KeyValueError.invalidKey {
            // success
        }
    }

    func testWatchOptions() async throws {
        let kv = try await start(history: 5)
        try await kv.put("a", "1")
        let second = try await kv.put("a", "2")
        try await kv.put("b", "1")
        try await kv.delete("b")

        let history = try await kv.watchAll(options: KeyValueWatchOptions(includeHistory: true))
        let historyEvents = try await collect(history, count: 5)
        XCTAssertEqual(historyEvents, ["PUT a=1", "PUT a=2", "PUT b=1", "DEL b=", "<init>"])

        let ignoreDeletes = try await kv.watchAll(
            options: KeyValueWatchOptions(ignoreDeletes: true))
        let ignoreDeletesEvents = try await collect(ignoreDeletes, count: 2)
        XCTAssertEqual(ignoreDeletesEvents, ["PUT a=2", "<init>"])

        let metaOnly = try await kv.watchAll(options: KeyValueWatchOptions(metaOnly: true))
        let metaOnlyEvents = try await collect(metaOnly, count: 3)
        XCTAssertEqual(metaOnlyEvents, ["PUT a=", "DEL b=", "<init>"])

        let resume = try await kv.watchAll(
            options: KeyValueWatchOptions(resumeFromRevision: second))
        let resumeEvents = try await collect(resume, count: 4)
        XCTAssertEqual(resumeEvents, ["PUT a=2", "PUT b=1", "DEL b=", "<init>"])

        let updatesOnly = try await kv.watchAll(options: KeyValueWatchOptions(updatesOnly: true))
        _ = Task {
            try await Task.sleep(nanoseconds: 200_000_000)
            try await kv.put("c", "1")
        }
        let updatesOnlyEvents = try await collect(updatesOnly, count: 1)
        XCTAssertEqual(updatesOnlyEvents, ["PUT c=1"])
    }

    func testKeys() async throws {
        let kv = try await start(history: 5)
        let empty = try await kv.keys()
        XCTAssertEqual(empty, [])

        try await kv.put("b", "1")
        try await kv.put("a", "1")
        try await kv.put("a", "2")
        try await kv.put("c.1", "1")
        try await kv.put("c.2", "1")
        try await kv.delete("b")

        let keys = try await kv.keys()
        XCTAssertEqual(keys, ["a", "c.1", "c.2"])
        let filtered = try await kv.keys(filters: ["c.*"])
        XCTAssertEqual(filtered, ["c.1", "c.2"])

        var listed: [String] = []
        for try await key in try await kv.listKeys() {
            listed.append(key)
        }
        XCTAssertEqual(listed.sorted(), ["a", "c.1", "c.2"])
    }

    func testHistory() async throws {
        let kv = try await start(history: 10)
        for i in 1...3 {
            try await kv.put("a", "\(i)")
        }
        try await kv.put("b", "1")
        try await kv.delete("a")

        let history = try await kv.history("a")
        XCTAssertEqual(history.map { $0.operation }, [.put, .put, .put, .delete])
        XCTAssertEqual(
            history.map { String(data: $0.value, encoding: .utf8)! }, ["1", "2", "3", ""])
        XCTAssertEqual(history.map { $0.revision }, [1, 2, 3, 5])

        let missing = try await kv.history("missing")
        XCTAssertEqual(missing.count, 0)
    }

    func testPurgeDeletes() async throws {
        let kv = try await start(history: 10)
        for i in 1...3 {
            try await kv.put("a", "\(i)")
            try await kv.put("b", "\(i)")
        }
        try await kv.put("c", "1")
        try await kv.delete("a")
        try await kv.delete("b")

        // recent markers are kept, the rest of the history is removed
        try await kv.purgeDeletes()
        var status = try await kv.status()
        XCTAssertEqual(status.values, 3)

        // all markers removed
        try await kv.purgeDeletes(olderThan: 0)
        status = try await kv.status()
        XCTAssertEqual(status.values, 1)
        let keys = try await kv.keys()
        XCTAssertEqual(keys, ["c"])
    }
}
