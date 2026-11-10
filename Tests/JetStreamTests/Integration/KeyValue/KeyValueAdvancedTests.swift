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

class KeyValueAdvancedTests: XCTestCase {

    static let allTests = [
        ("testListBuckets", testListBuckets),
        ("testMirror", testMirror),
        ("testSources", testSources),
        ("testCrossDomainMirror", testCrossDomainMirror),
    ]

    var natsServer = NatsServer()
    var leafServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        leafServer.stop()
        natsServer.stop()
    }

    private func connect(
        _ server: NatsServer, domain: String? = nil
    ) async throws
        -> JetStreamContext
    {
        let client = NatsClientOptions().url(URL(string: server.clientURL)!).build()
        try await client.connect()
        if let domain {
            return JetStreamContext(client: client, domain: domain)
        }
        return JetStreamContext(client: client)
    }

    private func start() async throws -> JetStreamContext {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical
        return try await connect(natsServer)
    }

    /// Waits until the key has the expected value (nil meaning deleted) in the bucket.
    private func waitFor(
        _ kv: KeyValue, _ key: String, _ expected: String?, file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        var value: String? = "<unset>"
        for _ in 0..<100 {
            value = try await kv.get(key).flatMap { String(data: $0.value, encoding: .utf8) }
            if value == expected {
                return
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        XCTFail(
            "expected \(key)=\(expected ?? "nil"), got \(value ?? "nil")", file: file, line: line)
    }

    func testListBuckets() async throws {
        let ctx = try await start()
        _ = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "b"))
        _ = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "a", history: 3))
        _ = try await ctx.createStream(cfg: StreamConfig(name: "other", subjects: ["foo"]))

        var names: [String] = []
        for try await name in await ctx.keyValueStoreNames() {
            names.append(name)
        }
        XCTAssertEqual(names.sorted(), ["a", "b"])

        var histories: [String: Int64] = [:]
        for try await status in await ctx.keyValueStores() {
            histories[status.bucket] = status.history
        }
        XCTAssertEqual(histories, ["a": 3, "b": 1])
    }

    func testMirror() async throws {
        let ctx = try await start()
        let origin = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "origin"))
        try await origin.put("name", "derek")

        let mirror = try await ctx.createKeyValue(
            cfg: KeyValueConfig(bucket: "mirror", mirror: StreamSource(name: "origin")))
        let stream = try await ctx.getStream(name: "KV_mirror")
        XCTAssertEqual(stream?.info.config.mirror?.name, "KV_origin")
        XCTAssertEqual(stream?.info.config.mirrorDirect, true)
        XCTAssertNil(stream?.info.config.subjects)

        // reads are served by the mirror, writes go to the origin
        try await waitFor(mirror, "name", "derek")
        try await mirror.put("name", "ivan")
        try await waitFor(origin, "name", "ivan")
        try await waitFor(mirror, "name", "ivan")
        try await mirror.delete("name")
        try await waitFor(origin, "name", nil)

        let keys = try await mirror.keys()
        XCTAssertEqual(keys, [])
    }

    func testSources() async throws {
        let ctx = try await start()
        let first = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "first"))
        let second = try await ctx.createKeyValue(cfg: KeyValueConfig(bucket: "second"))
        try await first.put("a", "1")
        try await second.put("b", "2")

        let combined = try await ctx.createKeyValue(
            cfg: KeyValueConfig(
                bucket: "combined",
                sources: [StreamSource(name: "first"), StreamSource(name: "KV_second")]))
        let stream = try await ctx.getStream(name: "KV_combined")
        XCTAssertEqual(
            stream?.info.config.sources?.map { $0.name }, ["KV_first", "KV_second"])
        XCTAssertEqual(
            stream?.info.config.sources?.first?.subjectTransforms,
            [SubjectTransformConfig(source: "$KV.first.>", destination: "$KV.combined.>")])

        // values of sourced buckets appear as keys of this bucket
        try await waitFor(combined, "a", "1")
        try await waitFor(combined, "b", "2")
        try await combined.put("c", "3")
        let keys = try await combined.keys()
        XCTAssertEqual(keys, ["a", "b", "c"])
    }

    func testCrossDomainMirror() async throws {
        logger.logLevel = .critical
        let leafPort = Int.random(in: 20000..<30000)
        let dir = FileManager.default.temporaryDirectory
        let hubConfig = dir.appendingPathComponent("hub-\(UUID().uuidString).conf")
        try """
        jetstream: { domain: HUB }
        leafnodes: { port: \(leafPort) }
        """.write(to: hubConfig, atomically: true, encoding: .utf8)
        let leafConfig = dir.appendingPathComponent("leaf-\(UUID().uuidString).conf")
        try """
        jetstream: { domain: LEAF }
        leafnodes: { remotes: [ { url: "nats-leaf://127.0.0.1:\(leafPort)" } ] }
        """.write(to: leafConfig, atomically: true, encoding: .utf8)

        natsServer.start(cfg: hubConfig.path)
        leafServer.start(cfg: leafConfig.path)

        let hub = try await connect(natsServer)
        let origin = try await hub.createKeyValue(cfg: KeyValueConfig(bucket: "TEST"))
        try await origin.put("name", "derek")

        // a client of the leaf accessing the hub bucket through its domain
        var hubFromLeaf: KeyValue? = nil
        let leafToHub = try await connect(leafServer, domain: "HUB")
        for _ in 0..<50 where hubFromLeaf == nil {
            hubFromLeaf = try? await leafToHub.keyValue(bucket: "TEST")
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        let remote = try XCTUnwrap(hubFromLeaf)
        try await waitFor(remote, "name", "derek")
        try await remote.put("age", "22")
        try await waitFor(origin, "age", "22")

        // a mirror of the hub bucket in the leaf domain
        let leaf = try await connect(leafServer)
        let mirror = try await leaf.createKeyValue(
            cfg: KeyValueConfig(
                bucket: "MIRROR", mirror: StreamSource(name: "TEST", domain: "HUB")))
        let mirrorStream = try await leaf.getStream(name: "KV_MIRROR")
        XCTAssertEqual(mirrorStream?.info.config.mirror?.external?.apiPrefix, "$JS.HUB.API")

        try await waitFor(mirror, "name", "derek")
        try await mirror.put("name", "rip")
        try await waitFor(origin, "name", "rip")
        try await waitFor(mirror, "name", "rip")
    }
}
