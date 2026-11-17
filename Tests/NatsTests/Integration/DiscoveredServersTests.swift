// Copyright 2026 The NATS Authors
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

import Foundation
import Logging
import NatsServer
import XCTest

@testable import Nats

/// How the client uses the servers that a server reports in the `connect_urls` of its INFO,
/// against scripted servers and nats-server clusters.
class DiscoveredServersTests: XCTestCase {

    static var allTests = [
        ("testEntryWithSchemeIsSkipped", testEntryWithSchemeIsSkipped),
        ("testUserInfoInEntryIsDropped", testUserInfoInEntryIsDropped),
        ("testDiscoveredHostPortIsUsedForFailover", testDiscoveredHostPortIsUsedForFailover),
        (
            "testReconnectsToDiscoveredServerOfTlsCluster",
            testReconnectsToDiscoveredServerOfTlsCluster
        ),
    ]

    private var natsServers: [NatsServer] = []
    private var fakeServers: [FakeNatsServer] = []

    override func setUp() {
        super.setUp()
        logger.logLevel = .critical
    }

    override func tearDown() {
        for server in natsServers {
            server.stop()
        }
        natsServers = []
        for server in fakeServers {
            server.shutdown()
        }
        fakeServers = []
        super.tearDown()
    }

    private func pem(_ name: String) -> URL {
        Bundle.module.url(forResource: name, withExtension: "pem")!
    }

    /// Starts nats-server with the configuration `name` from the test resources, its `%@`
    /// filled with `arguments`. It is stopped in `tearDown`.
    private func natsServer(_ name: String, _ arguments: [String]) throws -> NatsServer {
        let template = try String(
            contentsOf: Bundle.module.url(forResource: name, withExtension: "conf")!,
            encoding: .utf8)
        let config = String(format: template, arguments: arguments.map { $0 as CVarArg })
        let file = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString).appendingPathExtension("conf")
        try config.write(to: file, atomically: true, encoding: .utf8)
        let server = NatsServer()
        server.start(cfg: file.path)
        natsServers.append(server)
        return server
    }

    /// Starts a scripted server that is stopped in `tearDown`.
    private func fake(
        _ script: FakeNatsServer.Script, host: String = "127.0.0.1"
    ) throws
        -> FakeNatsServer
    {
        let server = try FakeNatsServer(script, host: host)
        fakeServers.append(server)
        return server
    }

    /// A scripted server without TLS whose INFO after the first PONG lists `entries`.
    private func seed(listing entries: [String]) throws -> FakeNatsServer {
        try fake(
            .init(
                greeting: FakeNatsServer.info(),
                afterFirstPong: FakeNatsServer.info(connectUrls: entries)))
    }

    /// Connects `client`, waits for the INFO the server sends after the first PONG, then
    /// stops `seed` and waits up to 10 s for the client to connect to another server.
    private func reconnects(_ client: NatsClient, from seed: FakeNatsServer) async throws -> Bool {
        try await client.connect()
        // the INFO after the first PONG is read before the PONG of this PING
        guard case .success? = await result(within: 5, { try await client.rtt() }) else {
            XCTFail("no PONG from the seed")
            return false
        }
        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        seed.shutdown()
        return await XCTWaiter().fulfillment(of: [reconnected], timeout: 10) == .completed
    }

    func testEntryWithSchemeIsSkipped() async throws {
        let peer = try fake(.init(greeting: FakeNatsServer.info()))
        let other = try fake(.init(greeting: FakeNatsServer.info()))
        // with the servers in order, the entries with a scheme would be dialed first
        let seed = try seed(listing: [
            "nats://127.0.0.1:\(peer.port)", "NATS://127.0.0.1:\(peer.port)",
            "tls://127.0.0.1:\(peer.port)", "ws://127.0.0.1:\(peer.port)",
            "127.0.0.1:\(other.port)",
        ])
        let client = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(seed.port)")!)
            .retainServersOrder()
            .reconnectWait(0.1)
            .build()

        let reconnected = try await reconnects(client, from: seed)
        XCTAssertTrue(reconnected, "client did not reconnect to the entry without a scheme")
        XCTAssertEqual(client.connectedUrl?.port, other.port)
        XCTAssertEqual(peer.connections.count, 0, "an entry with a scheme was dialed")
        try? await client.close()
    }

    func testUserInfoInEntryIsDropped() async throws {
        let other = try fake(.init(greeting: FakeNatsServer.info()))
        let seed = try seed(listing: ["user:pass@127.0.0.1:\(other.port)"])
        let client = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(seed.port)")!)
            .usernameAndPassword("alice", "secret")
            .reconnectWait(0.1)
            .build()

        let reconnected = try await reconnects(client, from: seed)
        XCTAssertTrue(reconnected, "client did not reconnect to the entry with user info")
        XCTAssertEqual(client.connectedUrl?.port, other.port)
        XCTAssertNil(client.connectedUrl?.user)
        let connect = try XCTUnwrap(other.lines.first { $0.hasPrefix("CONNECT") })
        XCTAssertTrue(connect.contains("\"user\":\"alice\""), connect)
        XCTAssertTrue(connect.contains("\"pass\":\"secret\""), connect)
        try? await client.close()
    }

    func testDiscoveredHostPortIsUsedForFailover() async throws {
        // skipped, while the other entries are used
        let notHostPort = [
            "", "127.0.0.1", "127.0.0.1:", ":4222", "127.0.0.1:0", "127.0.0.1:65536",
            "127.0.0.1.:4222", " 127.0.0.1:4222", "host/path:4222", "h?x=1:4222", "h#f:4222",
            "[::1:4222", "[127.0.0.1]:4222", "::1:4222", "h%41:4222", "\u{00e9}:4222",
        ]
        for (host, listening) in [
            ("127.0.0.1", "127.0.0.1"), ("localhost", "127.0.0.1"), ("[::1]", "::1"),
        ] {
            let peer = try fake(.init(greeting: FakeNatsServer.info()), host: listening)
            let seed = try seed(listing: notHostPort + ["\(host):\(peer.port)"])
            let client = NatsClientOptions()
                .url(URL(string: "nats://127.0.0.1:\(seed.port)")!)
                .reconnectWait(0.1)
                .build()

            let reconnected = try await reconnects(client, from: seed)
            XCTAssertTrue(reconnected, "\(host): client did not reconnect to the entry")
            XCTAssertEqual(client.connectedUrl?.port, peer.port, host)
            try? await client.close()
        }
    }

    func testReconnectsToDiscoveredServerOfTlsCluster() async throws {
        let tls = [pem("server-cert").path, pem("server-key").path, pem("rootCA").path]
        let first = try natsServer("cluster_tls", tls + [""])
        let routePort = try XCTUnwrap(first.routePort)
        let second = try natsServer(
            "cluster_tls", tls + ["routes: [\"nats://127.0.0.1:\(routePort)\"]"])

        let client = NatsClientOptions()
            .url(URL(string: first.clientURL)!)
            .rootCertificates(pem("rootCA"))
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        try await waitForRoute(from: second, to: client)

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        first.stop()
        await fulfillment(of: [reconnected], timeout: 10)
        XCTAssertEqual(client.connectedUrl?.port, second.port)
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG after the reconnect")
        try? await client.close()
    }

    /// Waits up to 10 s until a message published on `server` reaches `client`, which is
    /// connected to another node of its cluster: the route is then up, and `client` has read
    /// every INFO its server sent before that message.
    private func waitForRoute(from server: NatsServer, to client: NatsClient) async throws {
        let subject = "route.\(UUID().uuidString)"
        let sub = try await client.subscribe(subject: subject)
        let publisher = NatsClientOptions()
            .url(URL(string: server.clientURL)!)
            .rootCertificates(pem("rootCA"))
            .build()
        try await publisher.connect()
        for _ in 0..<100 where sub.received == 0 {
            try await publisher.publish(Data("up".utf8), subject: subject)
            try await publisher.flush()
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        try await publisher.close()
        try await sub.unsubscribe()
        XCTAssertGreaterThan(sub.received, 0, "the cluster did not form within 10 s")
    }
}
