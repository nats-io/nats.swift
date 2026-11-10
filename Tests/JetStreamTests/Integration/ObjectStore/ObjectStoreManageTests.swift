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
import JetStreamObjectStore
import Logging
import NatsServer
import XCTest

@testable import Nats

class ObjectStoreManageTests: XCTestCase {

    static let allTests = [
        ("testWatch", testWatch),
        ("testWatchOptions", testWatchOptions),
        ("testUpdateMeta", testUpdateMeta),
        ("testLinks", testLinks),
        ("testSeal", testSeal),
        ("testListBuckets", testListBuckets),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    private func start() async throws -> (JetStreamContext, ObjectStore) {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let ctx = JetStreamContext(client: client)
        return (ctx, try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "files")))
    }

    /// Collects watcher elements, using "<init>" for the initial infos marker.
    private func collect(
        _ watcher: ObjectWatcher, count: Int, while action: (() async throws -> Void)? = nil
    ) async throws -> [String] {
        var events: [String] = []
        var acted = false
        for try await info in watcher {
            if let info {
                events.append(info.deleted == true ? "deleted \(info.name)" : info.name)
            } else {
                events.append("<init>")
            }
            if events.count == count {
                break
            }
            if !acted, info == nil, let action {
                acted = true
                try await action()
            }
        }
        await watcher.stop()
        return events
    }

    func testWatch() async throws {
        let (_, store) = try await start()
        let empty = try await store.watch()
        let emptyEvents = try await collect(empty, count: 1)
        XCTAssertEqual(emptyEvents, ["<init>"])

        try await store.put(name: "a", string: "1")
        try await store.put(name: "b", string: "1")

        let watcher = try await store.watch()
        let events = try await collect(watcher, count: 5) {
            try await store.put(name: "c", string: "1")
            try await store.delete("a")
        }
        XCTAssertEqual(events, ["a", "b", "<init>", "c", "deleted a"])
    }

    func testWatchOptions() async throws {
        let (_, store) = try await start()
        try await store.put(name: "a", string: "1")
        try await store.put(name: "a", string: "2")
        try await store.put(name: "b", string: "1")
        try await store.delete("b")

        // rollups keep only the latest info of each object
        let history = try await store.watch(options: ObjectWatchOptions(includeHistory: true))
        let historyEvents = try await collect(history, count: 3)
        XCTAssertEqual(historyEvents, ["a", "deleted b", "<init>"])

        let ignoreDeletes = try await store.watch(options: ObjectWatchOptions(ignoreDeletes: true))
        let ignoreDeletesEvents = try await collect(ignoreDeletes, count: 2)
        XCTAssertEqual(ignoreDeletesEvents, ["a", "<init>"])

        let updatesOnly = try await store.watch(options: ObjectWatchOptions(updatesOnly: true))
        _ = Task {
            try await Task.sleep(nanoseconds: 200_000_000)
            try await store.put(name: "c", string: "1")
        }
        let updatesOnlyEvents = try await collect(updatesOnly, count: 1)
        XCTAssertEqual(updatesOnlyEvents, ["c"])
    }

    func testUpdateMeta() async throws {
        let (_, store) = try await start()
        try await store.put(name: "a", string: "content")
        try await store.put(name: "b", string: "other")

        try await store.updateMeta(
            "a", meta: ObjectMeta(name: "a", description: "described", metadata: ["k": "v"]))
        let described = try await store.info("a")
        XCTAssertEqual(described?.meta.description, "described")
        XCTAssertEqual(described?.meta.metadata, ["k": "v"])

        // rename keeps the content
        try await store.updateMeta("a", meta: ObjectMeta(name: "renamed"))
        let old = try await store.info("a", showDeleted: true)
        XCTAssertNil(old)
        let renamed = try await store.getString("renamed")
        XCTAssertEqual(renamed, "content")

        do {
            try await store.updateMeta("renamed", meta: ObjectMeta(name: "b"))
            XCTFail("expected object already exists")
        } catch JetStreamError.ObjectStoreError.objectAlreadyExists {
            // success
        }
        try await store.delete("b")
        do {
            try await store.updateMeta("b", meta: ObjectMeta(name: "b"))
            XCTFail("expected update meta deleted")
        } catch JetStreamError.ObjectStoreError.updateMetaDeleted {
            // success
        }
        // names of deleted objects can be reused
        try await store.updateMeta("renamed", meta: ObjectMeta(name: "b"))
        let reused = try await store.getString("b")
        XCTAssertEqual(reused, "content")
    }

    func testLinks() async throws {
        let (ctx, store) = try await start()
        let other = try await ctx.createObjectStore(cfg: ObjectStoreConfig(bucket: "other"))
        let target = try await store.put(name: "target", string: "content")
        let remote = try await other.put(name: "remote", string: "remote content")

        let link = try await store.addLink("link", to: target)
        XCTAssertEqual(link.meta.options?.link, ObjectLink(bucket: "files", name: "target"))
        let linked = try await store.getString("link")
        XCTAssertEqual(linked, "content")

        // links to objects in other buckets
        try await store.addLink("remote-link", to: remote)
        let remoteLinked = try await store.getString("remote-link")
        XCTAssertEqual(remoteLinked, "remote content")

        // links to buckets cannot be read as objects
        try await store.addBucketLink("bucket-link", to: other)
        do {
            _ = try await store.getData("bucket-link")
            XCTFail("expected cant get bucket")
        } catch JetStreamError.ObjectStoreError.cantGetBucket {
            // success
        }

        do {
            try await store.addLink("link-to-link", to: link)
            XCTFail("expected no link to link")
        } catch JetStreamError.ObjectStoreError.noLinkToLink {
            // success
        }
        do {
            try await store.addLink("target", to: remote)
            XCTFail("expected object already exists")
        } catch JetStreamError.ObjectStoreError.objectAlreadyExists {
            // success
        }
        try await store.delete("target")
        let deleted = try await store.info("target", showDeleted: true)
        do {
            try await store.addLink("deleted-link", to: deleted!)
            XCTFail("expected no link to deleted")
        } catch JetStreamError.ObjectStoreError.noLinkToDeleted {
            // success
        }
    }

    func testSeal() async throws {
        let (_, store) = try await start()
        try await store.put(name: "a", string: "1")

        try await store.seal()
        let status = try await store.status()
        XCTAssertTrue(status.sealed)

        do {
            try await store.put(name: "b", string: "1")
            XCTFail("expected sealed bucket to reject objects")
        } catch {
            // success
        }
        let value = try await store.getString("a")
        XCTAssertEqual(value, "1")
    }

    func testListBuckets() async throws {
        let (ctx, _) = try await start()
        _ = try await ctx.createObjectStore(
            cfg: ObjectStoreConfig(bucket: "images", description: "pictures"))
        _ = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "kv"))

        var names: [String] = []
        for try await name in await ctx.objectStoreNames() {
            names.append(name)
        }
        XCTAssertEqual(names.sorted(), ["files", "images"])

        var descriptions: [String: String] = [:]
        for try await status in await ctx.objectStores() {
            descriptions[status.bucket] = status.description ?? ""
        }
        XCTAssertEqual(descriptions, ["files": "", "images": "pictures"])
    }
}
