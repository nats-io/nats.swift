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
import NIOConcurrencyHelpers
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
        ("testServerLearnedOverTlsIsDialedWithTls", testServerLearnedOverTlsIsDialedWithTls),
        (
            "testRefusalOfServerLearnedOverTlsSaysHowItWasLearned",
            testRefusalOfServerLearnedOverTlsSaysHowItWasLearned
        ),
        ("testServerLearnedAgainOverTlsGainsTls", testServerLearnedAgainOverTlsGainsTls),
        (
            "testServerReportingItsOwnAddressIsStillDialedWithTls",
            testServerReportingItsOwnAddressIsStillDialedWithTls
        ),
        (
            "testConfiguredServerReportedOverTlsKeepsItsScheme",
            testConfiguredServerReportedOverTlsKeepsItsScheme
        ),
        (
            "testDiscoveredServerOfWebsocketClusterIsDialedOverWebsocket",
            testDiscoveredServerOfWebsocketClusterIsDialedOverWebsocket
        ),
        (
            "testDiscoveredServerOfTlsWebsocketClusterIsDialedOverWss",
            testDiscoveredServerOfTlsWebsocketClusterIsDialedOverWss
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
        try await waitForRoute(
            from: NatsClientOptions()
                .url(URL(string: second.clientURL)!)
                .rootCertificates(pem("rootCA")), to: client)

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        first.stop()
        await fulfillment(of: [reconnected], timeout: 10)
        XCTAssertEqual(client.connectedUrl?.port, second.port)
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG after the reconnect")
        // an attempt that waits for an INFO that does not come would keep close() waiting
        _ = await result(within: 5) { try await client.close() }
    }

    /// Waits up to 10 s until a message published by a client built from `options`, connected
    /// to one node of a cluster, reaches `client`, which is connected to another node: the
    /// route is then up, and `client` has read every INFO its server sent before that message.
    private func waitForRoute(
        from options: NatsClientOptions, to client: NatsClient
    )
        async throws
    {
        let subject = "route.\(UUID().uuidString)"
        let sub = try await client.subscribe(subject: subject)
        let publisher = options.build()
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

    /// Waits up to `seconds` for `condition`.
    private func eventually(
        _ seconds: TimeInterval = 5, _ condition: () -> Bool
    ) async throws -> Bool {
        for _ in 0..<Int(seconds * 20) where !condition() {
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        return condition()
    }

    /// A scripted server that requires TLS, whose INFO after the first PONG lists `entries`.
    private func tlsSeed(listing entries: [String]) throws -> FakeNatsServer {
        try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true),
                tls: try FakeNatsServer.tlsContext(),
                afterFirstPong: FakeNatsServer.info(connectUrls: entries)))
    }

    /// A scripted server that offers TLS without requiring it.
    private func offeringTls() throws -> FakeNatsServer {
        try fake(
            .init(
                greeting: FakeNatsServer.info(tlsAvailable: true),
                tls: try FakeNatsServer.tlsContext()))
    }

    // With tlsOptional() nothing asks for TLS for every server: TLS comes from the server.
    func testServerLearnedOverTlsIsDialedWithTls() async throws {
        let peer = try offeringTls()
        let seed = try tlsSeed(listing: ["localhost:\(peer.port)"])
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(seed.port)")!)
            .rootCertificates(pem("rootCA"))
            .tlsOptional()
            .usernameAndPassword("user", "secret")
            .reconnectWait(0.1)
            .build()

        let reconnected = try await reconnects(client, from: seed)
        let first = peer.connections.first?.firstBytes ?? []
        XCTAssertEqual(
            first.first, 0x16,
            "first bytes are not a TLS record: \(String(decoding: first, as: UTF8.self))")
        XCTAssertTrue(reconnected, "client did not reconnect to the discovered server")
        XCTAssertEqual(client.connectedUrl, URL(string: "tls://localhost:\(peer.port)"))
        try? await client.close()

        // learned over TLS, and offering none: nothing is sent to it
        let plain = try fake(.init(greeting: FakeNatsServer.info()))
        let other = try tlsSeed(listing: ["127.0.0.1:\(plain.port)"])
        let plainClient = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(other.port)")!)
            .rootCertificates(pem("rootCA"))
            .tlsOptional()
            .usernameAndPassword("user", "secret")
            .reconnectWait(0.1)
            .build()
        try await plainClient.connect()
        let otherRtt = await result(within: 5) { try await plainClient.rtt() }
        XCTAssertNotNil(try? otherRtt?.get(), "no PONG")
        other.shutdown()
        let dialed = try await eventually { plain.connections.first?.closed == true }
        XCTAssertTrue(dialed, "the discovered server was not dialed, or its connection left open")
        let sent = plain.connections.first?.firstBytes ?? []
        XCTAssertTrue(sent.isEmpty, "sent " + String(decoding: sent, as: UTF8.self))
        try? await plainClient.close()
    }

    // A server learned over TLS keeps TLS under tlsOptional() too, so the refusal names how it
    // was learned, and only what asks for TLS for every server points to tlsOptional().
    func testRefusalOfServerLearnedOverTlsSaysHowItWasLearned() async throws {
        let plain = try fake(.init(greeting: FakeNatsServer.info()))
        let url = "\"tls://127.0.0.1:\(plain.port)\""
        let variants = [
            (
                "tlsOptional()", true,
                "\(url) was discovered on a connection with TLS, but does not offer it"
            ),
            ("requireTls()", false, "requireTls() asks for TLS, but \(url) does not offer it"),
        ]
        for (name, optional, expected) in variants {
            let seed = try tlsSeed(listing: ["127.0.0.1:\(plain.port)"])
            let options = NatsClientOptions()
                .url(URL(string: "tls://localhost:\(seed.port)")!)
                .rootCertificates(pem("rootCA"))
                .retainServersOrder()
                .reconnectWait(0.1)
            let client = (optional ? options.tlsOptional() : options.requireTls()).build()
            let refusals = NIOLockedValueBox<[String]>([])
            client.on(.error) { event in
                if case .error(let error) = event,
                    case .tlsFailure(let cause)? = error as? NatsError.ConnectError
                {
                    refusals.withLockedValue { $0.append("\(cause)") }
                }
            }
            try await client.connect()
            let rtt = await result(within: 5) { try await client.rtt() }
            XCTAssertNotNil(try? rtt?.get(), "\(name): no PONG")
            seed.shutdown()
            let reported = try await eventually { !refusals.withLockedValue { $0 }.isEmpty }
            try? await client.close()
            XCTAssertTrue(reported, "\(name): the refusal was not reported")
            XCTAssertEqual(refusals.withLockedValue { $0.first }, expected, name)
        }
    }

    // Two configured servers, as tlsOptional() allows: one without TLS, one with. The server
    // learned through the first gains TLS when the second reports it too.
    func testServerLearnedAgainOverTlsGainsTls() async throws {
        let learned = try offeringTls()
        let plain = try seed(listing: ["localhost:\(learned.port)"])
        let tls = try tlsSeed(listing: ["localhost:\(learned.port)"])
        let client = NatsClientOptions()
            .urls([
                URL(string: "nats://127.0.0.1:\(plain.port)")!,
                URL(string: "tls://localhost:\(tls.port)")!,
            ])
            .retainServersOrder()
            .rootCertificates(pem("rootCA"))
            .tlsOptional()
            .reconnectWait(0.1)
            .build()

        let toTls = try await reconnects(client, from: plain)
        XCTAssertTrue(toTls, "client did not reconnect to the second configured server")
        XCTAssertEqual(client.connectedUrl?.port, tls.port, "setup: second server not used")
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG from the second server")

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        tls.shutdown()
        await fulfillment(of: [reconnected], timeout: 10)
        let first = learned.connections.first?.firstBytes ?? []
        XCTAssertEqual(
            first.first, 0x16,
            "first bytes are not a TLS record: \(String(decoding: first, as: UTF8.self))")
        XCTAssertEqual(client.connectedUrl, URL(string: "tls://localhost:\(learned.port)"))
        try? await client.close()
    }

    // The server reports its own address over TLS: every connection to that address has TLS.
    func testServerReportingItsOwnAddressIsStillDialedWithTls() async throws {
        // a free port, for a server that reports its own address
        let placeholder = try FakeNatsServer(.init())
        let port = placeholder.port
        placeholder.shutdown()
        let reporting = try FakeNatsServer(
            .init(
                greeting: FakeNatsServer.info(tlsAvailable: true),
                tls: try FakeNatsServer.tlsContext(),
                afterFirstPong: FakeNatsServer.info(connectUrls: ["localhost:\(port)"])),
            port: port)
        fakeServers.append(reporting)
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(port)")!)
            .rootCertificates(pem("rootCA"))
            .tlsOptional()
            .usernameAndPassword("user", "secret")
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG")

        // the server comes back refusing CONNECT, so that the client tries every server it has
        reporting.shutdown()
        let refusing = try FakeNatsServer(
            .init(
                greeting: FakeNatsServer.info(tlsAvailable: true),
                tls: try FakeNatsServer.tlsContext(),
                firstPingReply: "-ERR 'maximum connections exceeded'\r\n"),
            port: port)
        fakeServers.append(refusing)
        let retried = try await eventually { refusing.connections.count >= 4 }
        try? await client.close()
        XCTAssertTrue(retried, "setup: the client did not retry the server")
        for (index, connection) in refusing.connections.enumerated() {
            XCTAssertEqual(
                connection.firstBytes.first, 0x16,
                "connection \(index): "
                    + String(decoding: connection.firstBytes, as: UTF8.self))
        }
    }

    // With tlsOptional() nothing asks for TLS for every server. The configured tls:// server
    // reports the configured nats:// one, which stays nats://: it is not dialed with TLS.
    func testConfiguredServerReportedOverTlsKeepsItsScheme() async throws {
        // offers TLS, and fails every client that sends it plaintext
        let plain = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsAvailable: true),
                tls: try FakeNatsServer.tlsContext()))
        let reporting = try tlsSeed(listing: ["127.0.0.1:\(plain.port)"])
        let client = NatsClientOptions()
            .urls([
                URL(string: "nats://127.0.0.1:\(plain.port)")!,
                URL(string: "tls://localhost:\(reporting.port)")!,
            ])
            .retainServersOrder()
            .rootCertificates(pem("rootCA"))
            .tlsOptional()
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        XCTAssertEqual(client.connectedUrl?.port, reporting.port, "setup: not on the tls:// server")
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG")

        reporting.shutdown()
        let retried = try await eventually { plain.connections.count >= 4 }
        try? await client.close()
        XCTAssertTrue(retried, "setup: the client did not retry the nats:// server")
        for (index, connection) in plain.connections.enumerated() {
            XCTAssertNotEqual(
                connection.firstBytes.first, 0x16, "connection \(index) to nats:// has TLS")
        }
    }

    func testDiscoveredServerOfWebsocketClusterIsDialedOverWebsocket() async throws {
        let first = try natsServer("cluster_ws", [""])
        let routePort = try XCTUnwrap(first.routePort)
        let second = try natsServer(
            "cluster_ws", ["routes: [\"nats://127.0.0.1:\(routePort)\"]"])

        let client = NatsClientOptions()
            .url(URL(string: first.clientWebsocketURL)!)
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        try await waitForRoute(
            from: NatsClientOptions().url(URL(string: second.clientURL)!), to: client)

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        first.stop()
        await fulfillment(of: [reconnected], timeout: 10)
        XCTAssertEqual(client.connectedUrl, URL(string: second.clientWebsocketURL))
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG after the reconnect")
        // an attempt that waits for an INFO that does not come would keep close() waiting
        _ = await result(within: 5) { try await client.close() }
    }

    func testDiscoveredServerOfTlsWebsocketClusterIsDialedOverWss() async throws {
        let tls = [pem("server-cert").path, pem("server-key").path]
        let first = try natsServer("cluster_wss", tls + [""])
        let routePort = try XCTUnwrap(first.routePort)
        let second = try natsServer(
            "cluster_wss", tls + ["routes: [\"nats://127.0.0.1:\(routePort)\"]"])

        let client = NatsClientOptions()
            .url(URL(string: first.clientWebsocketURL)!)
            .rootCertificates(pem("rootCA"))
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        try await waitForRoute(
            from: NatsClientOptions()
                .url(URL(string: second.clientWebsocketURL)!)
                .rootCertificates(pem("rootCA")), to: client)

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        first.stop()
        await fulfillment(of: [reconnected], timeout: 10)
        XCTAssertEqual(client.connectedUrl, URL(string: second.clientWebsocketURL))
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG after the reconnect")
        _ = await result(within: 5) { try await client.close() }
    }
}
