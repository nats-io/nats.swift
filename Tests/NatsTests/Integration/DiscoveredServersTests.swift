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
import NIOSSL
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
        (
            "testServersOfFirstInfoAreUsedWithoutTlsUpgrade",
            testServersOfFirstInfoAreUsedWithoutTlsUpgrade
        ),
        (
            "testFirstConnectTriesServersOfAFailedAttempt",
            testFirstConnectTriesServersOfAFailedAttempt
        ),
        (
            "testFirstConnectLeadsFromAFullServerToItsPeer",
            testFirstConnectLeadsFromAFullServerToItsPeer
        ),
        ("testServersOfInfoBeforeTlsAreNotUsed", testServersOfInfoBeforeTlsAreNotUsed),
        ("testFailedTlsAttemptAddsNoServers", testFailedTlsAttemptAddsNoServers),
        (
            "testWaitsForInfoAfterFirstPongFromServer212",
            testWaitsForInfoAfterFirstPongFromServer212
        ),
        ("testDoesNotWaitForServerBefore212", testDoesNotWaitForServerBefore212),
        ("testInfoAfterTheWaitIsApplied", testInfoAfterTheWaitIsApplied),
        (
            "testInfoBeforePongIsNotUsedWhenTheWaitFails",
            testInfoBeforePongIsNotUsedWhenTheWaitFails
        ),
        (
            "testInfoAfterTheErrorOfAFailedAttemptIsNotUsed",
            testInfoAfterTheErrorOfAFailedAttemptIsNotUsed
        ),
        ("testLaterInfoReplacesDiscoveredServers", testLaterInfoReplacesDiscoveredServers),
        ("testConfiguredAndCurrentServersAreKept", testConfiguredAndCurrentServersAreKept),
        ("testIgnoreDiscoveredServers", testIgnoreDiscoveredServers),
        (
            "testFirstConnectEndsWhenAListedServerDoesNotAnswer",
            testFirstConnectEndsWhenAListedServerDoesNotAnswer
        ),
        (
            "testCloseReturnsWhileReconnectingToAServerThatSendsNothing",
            testCloseReturnsWhileReconnectingToAServerThatSendsNothing
        ),
        ("testTlsAttemptEndsAtTheConnectionTimeout", testTlsAttemptEndsAtTheConnectionTimeout),
        (
            "testCloseDuringATlsAttemptDoesNotWaitForTheServer",
            testCloseDuringATlsAttemptDoesNotWaitForTheServer
        ),
        (
            "testConnectionDroppedForUnansweredPingsIsClosedAtOnce",
            testConnectionDroppedForUnansweredPingsIsClosedAtOnce
        ),
        ("testCloseDuringFirstConnectStopsIt", testCloseDuringFirstConnectStopsIt),
        (
            "testCloseDuringAReconnectRoundDialsNoFurtherServer",
            testCloseDuringAReconnectRoundDialsNoFurtherServer
        ),
        (
            "testCloseDuringTheWaitBeforeAnAttemptDialsNothing",
            testCloseDuringTheWaitBeforeAnAttemptDialsNothing
        ),
        (
            "testDiscoveredIpIsVerifiedAgainstTheNameOfItsReporter",
            testDiscoveredIpIsVerifiedAgainstTheNameOfItsReporter
        ),
        (
            "testDiscoveredIpIsVerifiedAgainstTheNameOfItsReporterOverTwoHops",
            testDiscoveredIpIsVerifiedAgainstTheNameOfItsReporterOverTwoHops
        ),
        (
            "testDiscoveredIpIsNotAcceptedOnItsIpEntryAlone",
            testDiscoveredIpIsNotAcceptedOnItsIpEntryAlone
        ),
        ("testDiscoveredIpWithoutANameIsSkipped", testDiscoveredIpWithoutANameIsSkipped),
        (
            "testDiscoveredHostNameIsVerifiedAgainstItself",
            testDiscoveredHostNameIsVerifiedAgainstItself
        ),
        (
            "testDiscoveredIpOfWssClusterIsVerifiedAgainstTheNameOfItsReporter",
            testDiscoveredIpOfWssClusterIsVerifiedAgainstTheNameOfItsReporter
        ),
        (
            "testDiscoveredIpLearnedWithoutTlsIsVerifiedAgainstTheConfiguredName",
            testDiscoveredIpLearnedWithoutTlsIsVerifiedAgainstTheConfiguredName
        ),
        (
            "testDiscoveredIpBehindAHostNameLearnedWithoutTlsIsVerifiedAgainstTheConfiguredName",
            testDiscoveredIpBehindAHostNameLearnedWithoutTlsIsVerifiedAgainstTheConfiguredName
        ),
        (
            "testHostNameLearnedWithoutTlsIsNotConnectedOverTls",
            testHostNameLearnedWithoutTlsIsNotConnectedOverTls
        ),
        ("testReconnectToAServerWithNoNameReportsWhy", testReconnectToAServerWithNoNameReportsWhy),
        ("testTlsServerNameIsUsedForEveryServer", testTlsServerNameIsUsedForEveryServer),
        ("testNameErrorsPointAtTlsServerName", testNameErrorsPointAtTlsServerName),
        ("testVerifyDiscoveredServersByIp", testVerifyDiscoveredServersByIp),
        (
            "testNameErrorsOfADiscoveredIpPointAtVerifyDiscoveredServersByIp",
            testNameErrorsOfADiscoveredIpPointAtVerifyDiscoveredServersByIp
        ),
        (
            "testRootedNamesAreSentWithoutTheTrailingDot",
            testRootedNamesAreSentWithoutTheTrailingDot
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
        return await reconnects(client, after: seed)
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

    /// A scripted server that requires TLS after INFO, with `script` changed by `configure`.
    private func tlsAfterInfo(
        version: String = "2.11.0", connectUrls: [String] = [],
        _ configure: (inout FakeNatsServer.Script) -> Void = { _ in }
    ) throws -> FakeNatsServer {
        var script = FakeNatsServer.Script(
            greeting: FakeNatsServer.info(
                tlsRequired: true, connectUrls: connectUrls, version: version),
            tls: try FakeNatsServer.tlsContext())
        configure(&script)
        return try fake(script)
    }

    /// A client of the `tls://localhost` server at `port`, which trusts the test CA.
    private func tlsClient(_ port: Int) -> NatsClientOptions {
        NatsClientOptions()
            .url(URL(string: "tls://localhost:\(port)")!)
            .rootCertificates(pem("rootCA"))
            .reconnectWait(0.1)
    }

    func testServersOfFirstInfoAreUsedWithoutTlsUpgrade() async throws {
        let plainPeer = try fake(.init(greeting: FakeNatsServer.info()))
        let plainSeed = try fake(
            .init(greeting: FakeNatsServer.info(connectUrls: ["127.0.0.1:\(plainPeer.port)"])))
        let plainClient = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(plainSeed.port)")!)
            .reconnectWait(0.1)
            .build()
        let plainReconnected = try await reconnects(plainClient, from: plainSeed)
        XCTAssertTrue(plainReconnected, "plaintext: server of the first INFO not used")
        XCTAssertEqual(plainClient.connectedUrl?.port, plainPeer.port, "plaintext")
        try? await plainClient.close()

        let tls = try FakeNatsServer.tlsContext()
        let tlsFirstPeer = try fake(
            .init(greeting: FakeNatsServer.info(tlsRequired: true), tls: tls, tlsFirst: true))
        let tlsFirstSeed = try fake(
            .init(
                greeting: FakeNatsServer.info(
                    tlsRequired: true, connectUrls: ["localhost:\(tlsFirstPeer.port)"]),
                tls: tls, tlsFirst: true))
        let tlsFirstClient = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(tlsFirstSeed.port)")!)
            .withTlsFirst()
            .rootCertificates(pem("rootCA"))
            .reconnectWait(0.1)
            .build()
        let tlsFirstReconnected = try await reconnects(tlsFirstClient, from: tlsFirstSeed)
        XCTAssertTrue(tlsFirstReconnected, "TLS first: server of the first INFO not used")
        XCTAssertEqual(tlsFirstClient.connectedUrl?.port, tlsFirstPeer.port, "TLS first")
        try? await tlsFirstClient.close()
    }

    // The only configured server sends its INFO, then answers CONNECT with -ERR.
    func testFirstConnectTriesServersOfAFailedAttempt() async throws {
        let peer = try fake(.init(greeting: FakeNatsServer.info()))
        let seed = try fake(
            .init(
                greeting: FakeNatsServer.info(connectUrls: ["127.0.0.1:\(peer.port)"]),
                firstPingReply: "-ERR 'maximum connections exceeded'\r\n"))
        let client = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(seed.port)")!)
            .build()

        let connected = await result(within: 10) { try await client.connect() }
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        XCTAssertEqual(client.connectedUrl?.port, peer.port)
        try await client.close()
    }

    // A full nats-server writes its INFO, then -ERR, and closes the connection, all before
    // CONNECT.
    func testFirstConnectLeadsFromAFullServerToItsPeer() async throws {
        let full = try natsServer("cluster", ["max_connections: 1", ""])
        let routePort = try XCTUnwrap(full.routePort)
        let peer = try natsServer(
            "cluster", ["", "routes: [\"nats://127.0.0.1:\(routePort)\"]"])
        // the one connection the full server takes
        let holder = NatsClientOptions().url(URL(string: full.clientURL)!).build()
        try await holder.connect()
        try await waitForRoute(
            from: NatsClientOptions().url(URL(string: peer.clientURL)!), to: holder)

        // the close can come before or after the client has sent CONNECT
        for attempt in 0..<5 {
            let client = NatsClientOptions().url(URL(string: full.clientURL)!).build()
            let connected = await result(within: 10) { try await client.connect() }
            guard case .success? = connected else {
                XCTFail("\(attempt): connect: \(String(describing: connected))")
                continue
            }
            XCTAssertEqual(client.connectedUrl?.port, peer.port, "\(attempt)")
            try await client.close()
        }
        try await holder.close()
    }

    func testServersOfInfoBeforeTlsAreNotUsed() async throws {
        let peer = try fake(.init(greeting: FakeNatsServer.info()))
        for version in ["2.11.0", "2.14.0"] {
            // nats-server 2.12.0 and later send an INFO after the first PONG
            let seed = try tlsAfterInfo(
                version: version, connectUrls: ["127.0.0.1:\(peer.port)"]
            ) {
                $0.afterFirstPong = FakeNatsServer.info()
            }
            let client = tlsClient(seed.port).build()
            try await client.connect()
            seed.shutdown()
            try await Task.sleep(nanoseconds: 1_500_000_000)
            try? await client.close()
            XCTAssertGreaterThan(seed.connections.count, 0, "\(version): setup")
            XCTAssertEqual(
                peer.connections.count, 0, "\(version): dialed a server of the INFO before TLS")
        }
    }

    func testFailedTlsAttemptAddsNoServers() async throws {
        let peer = try fake(.init(greeting: FakeNatsServer.info()))
        let listing = FakeNatsServer.info(connectUrls: ["127.0.0.1:\(peer.port)"])
        // an INFO inside TLS before the -ERR that refuses CONNECT
        let seed = try tlsAfterInfo(connectUrls: ["127.0.0.1:\(peer.port)"]) {
            $0.firstPingReply = listing + "-ERR 'Authorization Violation'\r\n"
        }
        let client = tlsClient(seed.port).retryOnfailedConnect().build()

        try await client.connect()
        try await Task.sleep(nanoseconds: 1_500_000_000)
        try? await client.close()
        XCTAssertGreaterThan(seed.connections.count, 1, "setup: the client did not retry")
        XCTAssertEqual(peer.connections.count, 0, "dialed a server of a failed attempt")
    }

    func testWaitsForInfoAfterFirstPongFromServer212() async throws {
        let peer = try tlsAfterInfo()
        let seed = try tlsAfterInfo(version: "2.12.0") {
            // a PING the client answers while it waits
            $0.afterFirstPong =
                "PING\r\n" + FakeNatsServer.info(connectUrls: ["localhost:\(peer.port)"])
            $0.afterFirstPongDelay = 1
        }
        let client = tlsClient(seed.port).build()

        let start = Date()
        try await client.connect()
        XCTAssertGreaterThan(Date().timeIntervalSince(start), 0.9, "connected before the INFO")
        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        seed.shutdown()
        await fulfillment(of: [reconnected], timeout: 10)
        XCTAssertEqual(client.connectedUrl?.port, peer.port, "server of that INFO not used")
        XCTAssertTrue(seed.lines.contains("PONG"), "PING not answered: \(seed.lines)")
        try? await client.close()
    }

    func testDoesNotWaitForServerBefore212() async throws {
        for version in ["2.11.9", "2.12", "v2.14.0", ""] {
            let seed = try tlsAfterInfo(version: version)
            let client = tlsClient(seed.port).build()
            let start = Date()
            let connected = await result(within: 10) { try await client.connect() }
            guard case .success? = connected else {
                XCTFail("\(version): connect: \(String(describing: connected))")
                continue
            }
            XCTAssertLessThan(Date().timeIntervalSince(start), 2, "\(version): waited")
            try await client.close()
        }
    }

    func testInfoAfterTheWaitIsApplied() async throws {
        let peer = try tlsAfterInfo()
        let seed = try tlsAfterInfo(version: "2.14.0") {
            $0.afterFirstPong = FakeNatsServer.info(connectUrls: ["localhost:\(peer.port)"])
            $0.afterFirstPongDelay = 6
        }
        let client = tlsClient(seed.port).build()

        let start = Date()
        let connected = await result(within: 10) { try await client.connect() }
        let elapsed = Date().timeIntervalSince(start)
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        XCTAssertTrue((4.5..<6).contains(elapsed), "connected after \(elapsed) s")
        let learned = try await eventually(3) {
            client.connectionHandler?.servers.contains { $0.key.port == peer.port } == true
        }
        XCTAssertTrue(learned, "INFO that arrived after the wait was not applied")
        try? await client.close()
    }

    // Against nats-server 2.12.0 and later the client takes servers only from the INFO after
    // the first PONG: an INFO before that PONG is not used, also when the wait then fails.
    func testInfoBeforePongIsNotUsedWhenTheWaitFails() async throws {
        let peer = try fake(.init(greeting: FakeNatsServer.info()))
        for name in ["-ERR during the wait", "connection lost during the wait"] {
            let seed = try tlsAfterInfo(version: "2.12.0") {
                $0.beforeFirstPong = FakeNatsServer.info(connectUrls: ["127.0.0.1:\(peer.port)"])
                if name.hasPrefix("-ERR") {
                    $0.afterFirstPong = "-ERR 'Authorization Violation'\r\n"
                }
            }
            let client = tlsClient(seed.port).build()
            let connecting = Task { await result(within: 10) { try await client.connect() } }
            if !name.hasPrefix("-ERR") {
                let answered = try await eventually { seed.lines.contains("PING") }
                XCTAssertTrue(answered, "\(name): setup: no PING")
                seed.closeConnection(0)
            }
            let connected = await connecting.value
            guard case .failure? = connected else {
                XCTFail("\(name): connect: \(String(describing: connected))")
                continue
            }
            XCTAssertEqual(peer.connections.count, 0, "\(name): dialed a server of that INFO")
            try? await client.close()
        }
    }

    // The -ERR that refuses CONNECT ends the attempt: an INFO after it is not used.
    func testInfoAfterTheErrorOfAFailedAttemptIsNotUsed() async throws {
        // TLS after INFO: a failed attempt adds no servers
        let peer = try fake(.init(greeting: FakeNatsServer.info()))
        let seed = try tlsAfterInfo {
            $0.firstPingReply =
                "-ERR 'Authorization Violation'\r\n"
                + FakeNatsServer.info(connectUrls: ["127.0.0.1:\(peer.port)"])
        }
        let client = tlsClient(seed.port).retryOnfailedConnect().build()
        try await client.connect()
        try await Task.sleep(nanoseconds: 1_500_000_000)
        try? await client.close()
        XCTAssertGreaterThan(seed.connections.count, 1, "TLS: setup: the client did not retry")
        XCTAssertEqual(peer.connections.count, 0, "TLS: dialed a server of a failed attempt")

        // no TLS upgrade: the servers of the first INFO are used, not those of the later INFO
        let listed = try fake(.init(greeting: FakeNatsServer.info()))
        let later = try fake(.init(greeting: FakeNatsServer.info()))
        let plainSeed = try fake(
            .init(
                greeting: FakeNatsServer.info(connectUrls: ["127.0.0.1:\(listed.port)"]),
                firstPingReply: "-ERR 'Authorization Violation'\r\n"
                    + FakeNatsServer.info(connectUrls: ["127.0.0.1:\(later.port)"])))
        let plainClient = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(plainSeed.port)")!)
            .build()
        let connected = await result(within: 10) { try await plainClient.connect() }
        guard case .success? = connected else {
            return XCTFail("plaintext: connect: \(String(describing: connected))")
        }
        XCTAssertEqual(plainClient.connectedUrl?.port, listed.port, "plaintext")
        XCTAssertEqual(later.connections.count, 0, "plaintext: dialed a server of the later INFO")
        try await plainClient.close()
    }

    func testLaterInfoReplacesDiscoveredServers() async throws {
        let dropped = try fake(.init(greeting: FakeNatsServer.info()))
        let listed = try fake(.init(greeting: FakeNatsServer.info()))
        // after the list, INFOs without servers, which change nothing
        let seed = try fake(
            .init(
                greeting: FakeNatsServer.info(),
                afterFirstPong: FakeNatsServer.info(connectUrls: ["127.0.0.1:\(dropped.port)"]),
                afterSecondPong: FakeNatsServer.info(connectUrls: ["127.0.0.1:\(listed.port)"])
                    + FakeNatsServer.info(connectUrls: nil) + FakeNatsServer.info(connectUrls: [])))
        let client = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(seed.port)")!)
            .retainServersOrder()
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        // the INFOs after the second PONG are read before the third
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG")

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        seed.shutdown()
        await fulfillment(of: [reconnected], timeout: 10)
        XCTAssertEqual(client.connectedUrl?.port, listed.port)
        XCTAssertEqual(dropped.connections.count, 0, "dialed a server the later list dropped")
        try? await client.close()
    }

    func testConfiguredAndCurrentServersAreKept() async throws {
        // a configured server that is not running
        let down = try fake(.init())
        down.shutdown()
        let later = try fake(.init(greeting: FakeNatsServer.info()))
        let current = try seed(listing: ["127.0.0.1:\(later.port)"])
        let dropped = try fake(.init(greeting: FakeNatsServer.info()))
        let seed = try seed(listing: [
            "127.0.0.1:\(current.port)", "127.0.0.1:\(dropped.port)",
        ])
        let configured = [
            URL(string: "nats://127.0.0.1:\(seed.port)")!,
            URL(string: "nats://127.0.0.1:\(down.port)")!,
        ]
        let client = NatsClientOptions()
            .urls(configured)
            .retainServersOrder()
            .reconnectWait(0.1)
            .build()

        let reconnected = try await reconnects(client, from: seed)
        XCTAssertTrue(reconnected, "client did not reconnect to the discovered server")
        XCTAssertEqual(client.connectedUrl?.port, current.port, "setup: not on that server")
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG")
        // its list leaves out itself and the other server of the first list
        XCTAssertEqual(
            client.connectionHandler?.servers.map { $0.url },
            configured + [
                URL(string: "nats://127.0.0.1:\(current.port)")!,
                URL(string: "nats://127.0.0.1:\(later.port)")!,
            ])
        try? await client.close()
    }

    func testIgnoreDiscoveredServers() async throws {
        let peer = try fake(.init(greeting: FakeNatsServer.info()))
        let listing = FakeNatsServer.info(connectUrls: ["127.0.0.1:\(peer.port)"])
        let seed = try fake(.init(greeting: listing, afterFirstPong: listing))
        let client = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(seed.port)")!)
            .ignoreDiscoveredServers()
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG")
        XCTAssertEqual(client.connectionHandler?.servers.count, 1)
        seed.shutdown()
        try await Task.sleep(nanoseconds: 1_500_000_000)
        try? await client.close()
        XCTAssertEqual(peer.connections.count, 0, "dialed a discovered server")

        // no wait for the INFO after the first PONG
        let tlsSeed = try tlsAfterInfo(version: "2.14.0") {
            $0.afterFirstPong = FakeNatsServer.info(connectUrls: ["localhost:\(peer.port)"])
            $0.afterFirstPongDelay = 6
        }
        let clientOverTls = tlsClient(tlsSeed.port).ignoreDiscoveredServers().build()
        let start = Date()
        let connected = await result(within: 10) { try await clientOverTls.connect() }
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        XCTAssertLessThan(Date().timeIntervalSince(start), 2, "waited for the INFO")
        try await clientOverTls.close()
    }

    // An attempt has the connection timeout from the start of the connect, name lookup
    // included, up to its first PONG.
    func testFirstConnectEndsWhenAListedServerDoesNotAnswer() async throws {
        let silent = try fake(.init())
        let withoutPong = try fake(.init(greeting: FakeNatsServer.info(), answersPing: false))
        let variants = [
            ("sends nothing", silent, false),
            ("answers no PING", withoutPong, false),
            ("answers no websocket upgrade", silent, true),
        ]
        for (name, listed, websocket) in variants {
            let dialed = listed.connections.count
            // the only configured server lists it, then refuses the client
            let seed = try fake(
                .init(
                    greeting: FakeNatsServer.info(connectUrls: ["127.0.0.1:\(listed.port)"]),
                    firstPingReply: "-ERR 'maximum connections exceeded'\r\n",
                    websocket: websocket))
            let scheme = websocket ? "ws" : "nats"
            let client = NatsClientOptions()
                .url(URL(string: "\(scheme)://127.0.0.1:\(seed.port)")!)
                .build()

            let connected = await result(within: 10) { try await client.connect() }
            guard case .failure(NatsError.ConnectError.timeout)? = connected else {
                XCTFail("\(name): connect: \(String(describing: connected))")
                continue
            }
            XCTAssertGreaterThan(listed.connections.count, dialed, "\(name): setup: not dialed")
        }
    }

    func testCloseReturnsWhileReconnectingToAServerThatSendsNothing() async throws {
        let silent = try fake(.init())
        let seed = try fake(
            .init(greeting: FakeNatsServer.info(connectUrls: ["127.0.0.1:\(silent.port)"])))
        let client = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(seed.port)")!)
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        seed.shutdown()
        let dialed = try await eventually { silent.connections.count > 0 }
        XCTAssertTrue(dialed, "setup: the client did not dial the listed server")

        let closed = await result(within: 8) { try await client.close() }
        XCTAssertNotNil(closed, "close() did not return")
    }

    /// A scripted server that requires TLS and drops every byte the client sends after the TLS
    /// handshake: it answers neither CONNECT nor a TLS close.
    private func tlsServerThatStopsReading(tlsFirst: Bool = false) throws -> FakeNatsServer {
        // TLS 1.2: the server's handshake is done before the client's, so the server drops all
        // the client sends after its handshake. With TLS 1.3, CONNECT and PING can arrive in
        // the same read as the client's Finished, and the server answers them.
        try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true),
                tls: try FakeNatsServer.tlsContext(maximumTLSVersion: .tlsv12),
                tlsFirst: tlsFirst, afterTlsHandshake: .dropsBytes))
    }

    func testTlsAttemptEndsAtTheConnectionTimeout() async throws {
        for tlsFirst in [false, true] {
            let name = tlsFirst ? "TLS first" : "TLS after INFO"
            let server = try tlsServerThatStopsReading(tlsFirst: tlsFirst)
            var options = NatsClientOptions()
                .url(URL(string: "tls://localhost:\(server.port)")!)
                .rootCertificates(pem("rootCA"))
            if tlsFirst {
                options = options.withTlsFirst()
            }
            let client = options.build()

            let start = Date()
            let connected = await result(within: 15) { try await client.connect() }
            let elapsed = Date().timeIntervalSince(start)
            guard case .failure(NatsError.ConnectError.timeout)? = connected else {
                XCTFail("\(name): connect: \(String(describing: connected))")
                continue
            }
            // the connection timeout is 5 s
            XCTAssertLessThan(elapsed, 7.5, "\(name): the attempt took \(elapsed) s")
        }
    }

    func testCloseDuringATlsAttemptDoesNotWaitForTheServer() async throws {
        let listed = try tlsServerThatStopsReading()
        let seed = try tlsSeed(listing: ["localhost:\(listed.port)"])
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(seed.port)")!)
            .rootCertificates(pem("rootCA"))
            .retainServersOrder()
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        // the INFO after the first PONG is read before the PONG of this PING
        guard case .success? = await result(within: 5, { try await client.rtt() }) else {
            return XCTFail("setup: no PONG from the seed")
        }
        seed.shutdown()
        let dialed = try await eventually { listed.connections.count > 0 }
        XCTAssertTrue(dialed, "setup: the client did not dial the listed server")
        // the attempt is past its TLS handshake and waits for the PONG
        try await Task.sleep(nanoseconds: 500_000_000)

        let start = Date()
        let closed = await result(within: 10) { try await client.close() }
        let elapsed = Date().timeIntervalSince(start)
        XCTAssertNotNil(closed, "close() did not return")
        XCTAssertLessThan(elapsed, 2.5, "close() took \(elapsed) s")
    }

    // The client drops a connection whose server answers no PING, then connects to the next
    // server. A TLS close of the dropped connection would keep it open, and read, until the
    // server answers that close or for 5 s.
    func testConnectionDroppedForUnansweredPingsIsClosedAtOnce() async throws {
        let listed = try fake(.init(greeting: FakeNatsServer.info()))
        let dropped = try FakeNatsServer(scripts: [
            // after its first PONG it reads nothing more: no PING and no TLS close is answered
            .init(
                greeting: FakeNatsServer.info(tlsAvailable: true),
                tls: try FakeNatsServer.tlsContext(), stopsReadingAfterFirstPong: true),
            .init(greeting: FakeNatsServer.info(tlsAvailable: true), closeAfterGreeting: true),
        ])
        fakeServers.append(dropped)
        // a reconnect keeps the count of unanswered PINGs: the last two PONGs answer them
        let next = try fake(
            .init(
                greeting: FakeNatsServer.info(), greetingDelay: 1,
                firstPingReply: "PONG\r\nPONG\r\nPONG\r\n"))
        let client = NatsClientOptions()
            .urls([
                URL(string: "tls://localhost:\(dropped.port)")!,
                URL(string: "nats://127.0.0.1:\(next.port)")!,
            ])
            .rootCertificates(pem("rootCA"))
            .tlsOptional()
            .retainServersOrder()
            .pingInterval(0.25)
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        // the PINGs go unanswered after 0.75 s
        let dialed = try await eventually(2) { next.connections.count > 0 }
        XCTAssertTrue(dialed, "the client did not dial the next server within 2 s")

        // inside TLS on the dropped connection, before the next server's INFO
        let entry = "127.0.0.1:\(listed.port)"
        dropped.write(FakeNatsServer.info(tlsAvailable: true, connectUrls: [entry]), on: 0)
        let reconnected = try await eventually(10) {
            client.connectionHandler?.currentState == .connected
        }
        XCTAssertTrue(reconnected, "setup: the client did not reconnect")
        XCTAssertEqual(client.connectedUrl?.port, next.port)
        XCTAssertFalse(
            client.connectionHandler?.servers.contains { $0.key.port == listed.port } == true,
            "used a server listed on the dropped connection")

        let used = next.connections.count
        // longer than a TLS close waits for the server
        try await Task.sleep(nanoseconds: 6_000_000_000)
        XCTAssertEqual(next.connections.count, used, "the client reconnected")
        XCTAssertEqual(next.connections.last?.closed, false, "the connection in use was closed")
        try await client.close()
        dropped.closeConnection(0)
    }

    // After close() returned nothing is dialed, connect() returns, and the client stays closed.
    func testCloseDuringFirstConnectStopsIt() async throws {
        for configured in [false, true] {
            let name = configured ? "second configured server" : "listed server"
            let other = try fake(.init(greeting: FakeNatsServer.info()))
            // it answers no PING, so the attempt still waits when close() is called
            let seed = try fake(
                .init(
                    greeting: FakeNatsServer.info(
                        connectUrls: configured ? [] : ["127.0.0.1:\(other.port)"]),
                    answersPing: false))
            var urls = [URL(string: "nats://127.0.0.1:\(seed.port)")!]
            if configured {
                urls.append(URL(string: "nats://127.0.0.1:\(other.port)")!)
            }
            let client = NatsClientOptions().urls(urls).retainServersOrder().build()
            let connecting = Task { await result(within: 10) { try await client.connect() } }
            let waiting = try await eventually { seed.lines.contains("PING") }
            XCTAssertTrue(waiting, "\(name): setup: no PING")

            let closed = await result(within: 5) { try await client.close() }
            XCTAssertNotNil(closed, "\(name): close() did not return")
            try await Task.sleep(nanoseconds: 1_000_000_000)
            XCTAssertEqual(other.connections.count, 0, "\(name): dialed after close()")
            let connected = await connecting.value
            guard case .failure(NatsError.ClientError.connectionClosed)? = connected else {
                XCTFail("\(name): connect: \(String(describing: connected))")
                continue
            }
            XCTAssertEqual(client.connectionHandler?.currentState, .closed, name)
        }
    }

    // close() and suspend() during a reconnect round: the round ends with the current attempt
    func testCloseDuringAReconnectRoundDialsNoFurtherServer() async throws {
        for suspends in [false, true] {
            let name = suspends ? "suspend()" : "close()"
            // they accept the connection and send nothing
            let silent = try (0..<3).map { _ in try fake(.init()) }
            let seed = try fake(
                .init(
                    greeting: FakeNatsServer.info(
                        connectUrls: silent.map { "127.0.0.1:\($0.port)" })))
            let client = NatsClientOptions()
                .url(URL(string: "nats://127.0.0.1:\(seed.port)")!)
                .retainServersOrder()
                .reconnectWait(0.1)
                .build()
            try await client.connect()
            seed.shutdown()
            let dialed = try await eventually { silent[0].connections.count > 0 }
            XCTAssertTrue(dialed, "\(name): setup: the first listed server was not dialed")

            let start = Date()
            let done = await result(within: 20) {
                if suspends {
                    try await client.suspend()
                } else {
                    try await client.close()
                }
            }
            let elapsed = Date().timeIntervalSince(start)
            XCTAssertNotNil(done, "\(name) did not return")
            // the current attempt ends at its deadline, 5 s after it started
            XCTAssertLessThan(elapsed, 10, "\(name) took \(elapsed) s")
            let others = silent.dropFirst()
            XCTAssertEqual(
                others.map { $0.connections.count }, [0, 0], "\(name): dialed before it returned")
            try await Task.sleep(nanoseconds: 2_000_000_000)
            XCTAssertEqual(
                others.map { $0.connections.count }, [0, 0], "\(name): dialed after it returned")
            XCTAssertEqual(
                client.connectionHandler?.currentState, suspends ? .suspended : .closed, name)
            if suspends {
                try? await client.close()
            }
        }
    }

    // close() while a connect() after a failed one waits before its attempt: nothing is dialed
    func testCloseDuringTheWaitBeforeAnAttemptDialsNothing() async throws {
        for tlsFirst in [false, true] {
            let name = tlsFirst ? "TLS first" : "plaintext"
            let server = try fake(
                .init(
                    greeting: FakeNatsServer.info(tlsRequired: tlsFirst),
                    tls: tlsFirst ? try FakeNatsServer.tlsContext() : nil, tlsFirst: tlsFirst,
                    firstPingReply: "-ERR 'Authorization Violation'\r\n"))
            let url =
                tlsFirst ? "tls://localhost:\(server.port)" : "nats://127.0.0.1:\(server.port)"
            var options = NatsClientOptions().url(URL(string: url)!).reconnectWait(2)
            if tlsFirst {
                options = options.withTlsFirst().rootCertificates(pem("rootCA"))
            }
            let client = options.build()
            // after a failed connect(), the next one waits before its attempt
            let first = await result(within: 10) { try await client.connect() }
            guard case .failure? = first else {
                XCTFail("\(name): setup: first connect: \(String(describing: first))")
                continue
            }
            let connecting = Task { await result(within: 10) { try await client.connect() } }
            try await Task.sleep(nanoseconds: 500_000_000)
            let dialed = server.connections.count

            let closed = await result(within: 5) { try await client.close() }
            XCTAssertNotNil(closed, "\(name): close() did not return")
            try await Task.sleep(nanoseconds: 3_000_000_000)
            XCTAssertEqual(server.connections.count, dialed, "\(name): dialed after close()")
            let connected = await connecting.value
            guard case .failure(NatsError.ClientError.connectionClosed)? = connected else {
                XCTFail("\(name): connect: \(String(describing: connected))")
                continue
            }
            XCTAssertEqual(client.connectionHandler?.currentState, .closed, name)
        }
    }

    /// Waits up to 10 s for `client` to connect again after `server` is stopped.
    private func reconnects(_ client: NatsClient, after server: FakeNatsServer) async -> Bool {
        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        server.shutdown()
        return await XCTWaiter().fulfillment(of: [reconnected], timeout: 10) == .completed
    }

    /// A TLS context of the scripted servers with `ip-only-cert.pem`, which the test CA issued
    /// for 127.0.0.1 and ::1 and for no host name.
    private func ipOnlyCertificate() throws -> NIOSSLContext {
        try FakeNatsServer.tlsContext(cert: "ip-only-cert", key: "ip-only-key")
    }

    /// Connects a TLS-first client to `localhost`, whose server lists a TLS-first server at
    /// 127.0.0.1 with `ip-only-cert.pem` and then refuses the client, so that the first connect
    /// tries that server. Returns its port and the TLS failure of connect(), or nil after a
    /// test failure when connect() ends otherwise.
    private func nameErrorOfDiscoveredIp(
        file: StaticString = #filePath, line: UInt = #line
    ) async throws -> (port: Int, message: String)? {
        let peer = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true), tls: try ipOnlyCertificate(),
                tlsFirst: true))
        let seed = try fake(
            .init(
                greeting: FakeNatsServer.info(
                    tlsRequired: true, connectUrls: ["127.0.0.1:\(peer.port)"]),
                tls: try FakeNatsServer.tlsContext(), tlsFirst: true,
                firstPingReply: "-ERR 'maximum connections exceeded'\r\n"))
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(seed.port)")!)
            .withTlsFirst()
            .rootCertificates(pem("rootCA"))
            .build()
        let connected = await result(within: 10) { try await client.connect() }
        guard case .failure(NatsError.ConnectError.tlsFailure(let cause))? = connected else {
            try? await client.close()
            XCTFail(
                "expected a TLS failure, got \(String(describing: connected))", file: file,
                line: line)
            return nil
        }
        return (peer.port, "\(cause)")
    }

    // The certificate of the discovered server is issued for localhost and ::1, not 127.0.0.1.
    func testDiscoveredIpIsVerifiedAgainstTheNameOfItsReporter() async throws {
        let peer = try tlsAfterInfo()
        let seed = try tlsSeed(listing: ["127.0.0.1:\(peer.port)"])
        let client = tlsClient(seed.port).build()

        let reconnected = try await reconnects(client, from: seed)
        XCTAssertTrue(reconnected, "client did not reconnect to the discovered server")
        XCTAssertEqual(client.connectedUrl, URL(string: "tls://127.0.0.1:\(peer.port)"))
        XCTAssertEqual(peer.connections.first?.serverName, "localhost", "SNI")
        try? await client.close()
    }

    func testDiscoveredIpIsVerifiedAgainstTheNameOfItsReporterOverTwoHops() async throws {
        let last = try tlsAfterInfo()
        let middle = try tlsSeed(listing: ["127.0.0.1:\(last.port)"])
        let seed = try tlsSeed(listing: ["127.0.0.1:\(middle.port)"])
        let client = tlsClient(seed.port).build()

        let toMiddle = try await reconnects(client, from: seed)
        XCTAssertTrue(toMiddle, "client did not reconnect to the first discovered server")
        XCTAssertEqual(client.connectedUrl?.port, middle.port, "setup: on another server")
        // its INFO after the first PONG is read before the PONG of this PING
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG from the first discovered server")
        let toLast = await reconnects(client, after: middle)
        XCTAssertTrue(toLast, "client did not reconnect to the server it learned there")
        XCTAssertEqual(client.connectedUrl, URL(string: "tls://127.0.0.1:\(last.port)"))
        XCTAssertEqual(last.connections.first?.serverName, "localhost", "SNI")
        try? await client.close()
    }

    func testDiscoveredIpIsNotAcceptedOnItsIpEntryAlone() async throws {
        let ipOnly = try ipOnlyCertificate()
        let peer = try tlsAfterInfo { $0.tls = ipOnly }
        let seed = try tlsSeed(listing: ["127.0.0.1:\(peer.port)"])
        let client = tlsClient(seed.port).usernameAndPassword("user", "secret").build()
        try await client.connect()
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG")
        seed.shutdown()
        let refused = try await eventually { peer.connections.first?.closed == true }
        try? await client.close()
        XCTAssertTrue(refused, "the discovered server was not dialed, or its connection left open")
        XCTAssertFalse(peer.lines.contains { $0.hasPrefix("CONNECT") }, "\(peer.lines)")

        // The first connect tries the servers learned on a failed TLS-first attempt, so the
        // error of that server is the error of connect().
        guard case (let port, let message)? = try await nameErrorOfDiscoveredIp() else {
            return
        }
        XCTAssertTrue(
            message.contains(
                "the certificate of \"tls://127.0.0.1:\(port)\" is not valid for "
                    + "\"localhost\": it is issued for \"127.0.0.1\", \"::1\""), message)
        XCTAssertTrue(message.contains("(client_advertise in nats-server)"), message)
    }

    // Connected to a server by its address, the client has no name for the addresses that
    // server reports.
    func testDiscoveredIpWithoutANameIsSkipped() async throws {
        let ipOnly = try ipOnlyCertificate()
        let peer = try tlsAfterInfo { $0.tls = ipOnly }
        let seed = try tlsAfterInfo {
            $0.tls = ipOnly
            $0.afterFirstPong = FakeNatsServer.info(connectUrls: ["127.0.0.1:\(peer.port)"])
        }
        let client = NatsClientOptions()
            .url(URL(string: "tls://127.0.0.1:\(seed.port)")!)
            .rootCertificates(pem("rootCA"))
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG")
        XCTAssertEqual(client.connectionHandler?.servers.count, 2, "setup: server not learned")
        seed.shutdown()
        try await Task.sleep(nanoseconds: 1_500_000_000)
        try? await client.close()
        XCTAssertEqual(peer.connections.count, 0, "dialed a server with no name to verify")
    }

    // The seed is configured as LOCALHOST and verified under that name. The host name it
    // reports is verified against itself, and the address that host name reports against the
    // name it was verified under.
    func testDiscoveredHostNameIsVerifiedAgainstItself() async throws {
        let last = try tlsAfterInfo()
        let middle = try tlsSeed(listing: ["127.0.0.1:\(last.port)"])
        let seed = try tlsSeed(listing: ["localhost:\(middle.port)"])
        let client = NatsClientOptions()
            .url(URL(string: "tls://LOCALHOST:\(seed.port)")!)
            .rootCertificates(pem("rootCA"))
            .reconnectWait(0.1)
            .build()

        let toMiddle = try await reconnects(client, from: seed)
        XCTAssertEqual(seed.connections.first?.serverName, "LOCALHOST", "setup: SNI of the seed")
        XCTAssertTrue(toMiddle, "client did not reconnect to the discovered host name")
        XCTAssertEqual(middle.connections.first?.serverName, "localhost", "SNI of the host name")
        // its INFO after the first PONG is read before the PONG of this PING
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG from the discovered host name")
        let toLast = await reconnects(client, after: middle)
        XCTAssertTrue(toLast, "client did not reconnect to the address the host name reported")
        XCTAssertEqual(client.connectedUrl, URL(string: "tls://127.0.0.1:\(last.port)"))
        XCTAssertEqual(last.connections.first?.serverName, "localhost", "SNI of the address")
        try? await client.close()
    }

    // The nodes advertise 127.0.0.1 for their websocket listeners, and their certificate is
    // issued for localhost and ::1, not 127.0.0.1.
    func testDiscoveredIpOfWssClusterIsVerifiedAgainstTheNameOfItsReporter() async throws {
        let tls = [pem("server-cert").path, pem("server-key").path]
        let first = try natsServer("cluster_wss_ip", tls + [""])
        let routePort = try XCTUnwrap(first.routePort)
        let second = try natsServer(
            "cluster_wss_ip", tls + ["routes: [\"nats://127.0.0.1:\(routePort)\"]"])
        let secondPort = try XCTUnwrap(URL(string: second.clientWebsocketURL)?.port)

        let client = NatsClientOptions()
            .url(URL(string: first.clientWebsocketURL)!)
            .rootCertificates(pem("rootCA"))
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        let learned = try await eventually(10) {
            client.connectionHandler?.servers.contains {
                $0.url == URL(string: "wss://127.0.0.1:\(secondPort)")
            } == true
        }
        XCTAssertTrue(learned, "setup: the second node was not learned")

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        first.stop()
        await fulfillment(of: [reconnected], timeout: 10)
        XCTAssertEqual(client.connectedUrl, URL(string: "wss://127.0.0.1:\(secondPort)"))
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG after the reconnect")
        _ = await result(within: 5) { try await client.close() }
    }

    /// A client of the server without TLS at `port`, as `nats://localhost`, that trusts the
    /// test CA and leaves TLS to each server.
    private func plaintextClient(_ port: Int) -> NatsClientOptions {
        NatsClientOptions()
            .url(URL(string: "nats://localhost:\(port)")!)
            .rootCertificates(pem("rootCA"))
            .tlsOptional()
            .reconnectWait(0.1)
    }

    /// A scripted server without TLS that lists `entries` in its INFO, then refuses the client,
    /// so that the first connect tries those servers.
    private func refusingSeed(listing entries: [String]) throws -> FakeNatsServer {
        try fake(
            .init(
                greeting: FakeNatsServer.info(connectUrls: entries),
                firstPingReply: "-ERR 'maximum connections exceeded'\r\n"))
    }

    // The client reaches the cluster without TLS, and the discovered server requires TLS. Its
    // certificate is issued for localhost and ::1, not 127.0.0.1.
    func testDiscoveredIpLearnedWithoutTlsIsVerifiedAgainstTheConfiguredName() async throws {
        let peer = try tlsAfterInfo()
        let seed = try seed(listing: ["127.0.0.1:\(peer.port)"])
        let client = plaintextClient(seed.port).build()

        let reconnected = try await reconnects(client, from: seed)
        XCTAssertTrue(reconnected, "client did not reconnect to the discovered server")
        XCTAssertEqual(client.connectedUrl, URL(string: "nats://127.0.0.1:\(peer.port)"))
        XCTAssertEqual(peer.connections.first?.serverName, "localhost", "SNI")
        try? await client.close()

        // over two hops: the server in between has no TLS
        let last = try tlsAfterInfo()
        let middle = try self.seed(listing: ["127.0.0.1:\(last.port)"])
        let first = try self.seed(listing: ["127.0.0.1:\(middle.port)"])
        let hopping = plaintextClient(first.port).build()
        let toMiddle = try await reconnects(hopping, from: first)
        XCTAssertTrue(toMiddle, "client did not reconnect to the first discovered server")
        let rtt = await result(within: 5) { try await hopping.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG from the first discovered server")
        let toLast = await reconnects(hopping, after: middle)
        XCTAssertTrue(toLast, "client did not reconnect to the server it learned there")
        XCTAssertEqual(hopping.connectedUrl, URL(string: "nats://127.0.0.1:\(last.port)"))
        XCTAssertEqual(last.connections.first?.serverName, "localhost", "SNI")
        try? await hopping.close()
    }

    // The same through a discovered host name: the server in between, localhost, has no TLS.
    // The configured server is given in upper case, so that the name the address is verified
    // against is told apart from the host name it was learned through.
    func testDiscoveredIpBehindAHostNameLearnedWithoutTlsIsVerifiedAgainstTheConfiguredName()
        async throws
    {
        let last = try tlsAfterInfo()
        let middle = try seed(listing: ["127.0.0.1:\(last.port)"])
        let first = try seed(listing: ["localhost:\(middle.port)"])
        let client = NatsClientOptions()
            .url(URL(string: "nats://LOCALHOST:\(first.port)")!)
            .rootCertificates(pem("rootCA"))
            .tlsOptional()
            .reconnectWait(0.1)
            .build()

        let toMiddle = try await reconnects(client, from: first)
        XCTAssertTrue(toMiddle, "client did not reconnect to the discovered host name")
        XCTAssertEqual(client.connectedUrl, URL(string: "nats://localhost:\(middle.port)"))
        let rtt = await result(within: 5) { try await client.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG from the discovered host name")
        let toLast = await reconnects(client, after: middle)
        XCTAssertTrue(toLast, "client did not reconnect to the server it learned there")
        XCTAssertEqual(client.connectedUrl, URL(string: "nats://127.0.0.1:\(last.port)"))
        XCTAssertEqual(last.connections.first?.serverName, "LOCALHOST", "SNI")
        try? await client.close()
    }

    // The configured server sends its INFO, then refuses the client, so that the first connect
    // tries the server it lists, which requires TLS.
    func testHostNameLearnedWithoutTlsIsNotConnectedOverTls() async throws {
        let peer = try tlsAfterInfo()
        let seed = try refusingSeed(listing: ["localhost:\(peer.port)"])
        let client = plaintextClient(seed.port).usernameAndPassword("user", "secret").build()

        let connected = await result(within: 10) { try await client.connect() }
        guard case .failure(NatsError.ConnectError.tlsFailure(let cause))? = connected else {
            try? await client.close()
            return XCTFail("expected a TLS failure, got \(String(describing: connected))")
        }
        let message = "\(cause)"
        XCTAssertTrue(
            message.contains(
                "\"nats://localhost:\(peer.port)\": a discovered server with no name to verify "
                    + "its certificate against"), message)
        let closed = try await eventually { peer.connections.first?.closed == true }
        XCTAssertTrue(closed, "the discovered server was not dialed, or its connection left open")
        let sent = peer.connections.first?.firstBytes ?? []
        XCTAssertTrue(sent.isEmpty, "sent " + String(decoding: sent, as: UTF8.self))
    }

    // The client reaches the cluster without TLS and learns a host name that requires TLS. Each
    // reconnect attempt to it fails at its INFO, and an error event says why.
    func testReconnectToAServerWithNoNameReportsWhy() async throws {
        let peer = try tlsAfterInfo()
        let seed = try seed(listing: ["localhost:\(peer.port)"])
        // the peer comes last in each round: a round reports the error of its last attempt
        let client = plaintextClient(seed.port).retainServersOrder().build()
        let reported = XCTestExpectation(description: "no error event says why")
        reported.assertForOverFulfill = false
        client.on(.error) { event in
            if case .error(let error) = event, "\(error)".contains("with no name to verify") {
                reported.fulfill()
            }
        }
        try await client.connect()
        guard case .success? = await result(within: 5, { try await client.rtt() }) else {
            return XCTFail("no PONG from the seed")
        }
        XCTAssertEqual(client.connectionHandler?.servers.count, 2, "setup: host name not learned")

        seed.shutdown()
        await fulfillment(of: [reported], timeout: 10)
        let sent = peer.connections.flatMap { $0.firstBytes }
        XCTAssertTrue(sent.isEmpty, "sent " + String(decoding: sent, as: UTF8.self))
        try? await client.close()
    }

    // The certificates are issued for localhost and ::1, and the client is given the address
    // 127.0.0.1.
    func testTlsServerNameIsUsedForEveryServer() async throws {
        let peer = try tlsAfterInfo()
        let seed = try tlsSeed(listing: ["127.0.0.1:\(peer.port)"])
        let client = NatsClientOptions()
            .url(URL(string: "tls://127.0.0.1:\(seed.port)")!)
            .rootCertificates(pem("rootCA"))
            .tlsServerName("localhost")
            .reconnectWait(0.1)
            .build()

        let reconnected = try await reconnects(client, from: seed)
        XCTAssertEqual(seed.connections.first?.serverName, "localhost", "SNI of the seed")
        XCTAssertTrue(reconnected, "client did not reconnect to the discovered server")
        XCTAssertEqual(client.connectedUrl, URL(string: "tls://127.0.0.1:\(peer.port)"))
        XCTAssertEqual(peer.connections.first?.serverName, "localhost", "SNI of the peer")
        try? await client.close()

        // a host name learned without TLS, which has no name of its own
        let named = try tlsAfterInfo()
        let plain = try refusingSeed(listing: ["localhost:\(named.port)"])
        let plainClient = plaintextClient(plain.port).tlsServerName("localhost").build()
        let connected = await result(within: 10) { try await plainClient.connect() }
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        XCTAssertEqual(plainClient.connectedUrl?.port, named.port)
        XCTAssertEqual(named.connections.first?.serverName, "localhost", "SNI")
        try await plainClient.close()
    }

    func testNameErrorsPointAtTlsServerName() async throws {
        let named = try tlsAfterInfo()
        let plain = try refusingSeed(listing: ["localhost:\(named.port)"])
        let client = plaintextClient(plain.port).build()
        let connected = await result(within: 10) { try await client.connect() }
        guard case .failure(NatsError.ConnectError.tlsFailure(let cause))? = connected else {
            try? await client.close()
            return XCTFail("expected a TLS failure, got \(String(describing: connected))")
        }
        XCTAssertTrue(
            "\(cause)".contains(
                "with no name to verify its certificate against; tlsServerName(_:) sets the "
                    + "name every server is verified against"), "\(cause)")

        // a discovered server whose certificate is not issued for the name it is verified
        // against: the first connect tries it after the attempt that learned it fails
        guard let mismatch = try await nameErrorOfDiscoveredIp()?.message else {
            return
        }
        XCTAssertTrue(
            mismatch.contains(
                "; tlsServerName(_:) sets the name every server is verified against, or each "
                    + "server of the cluster can advertise a name its certificate is issued for"),
            mismatch)
    }

    // ip-only-cert.pem is issued for 127.0.0.1 and ::1 and for no host name.
    func testVerifyDiscoveredServersByIp() async throws {
        let ipOnly = try ipOnlyCertificate()
        let peer = try tlsAfterInfo { $0.tls = ipOnly }
        let seed = try tlsSeed(listing: ["127.0.0.1:\(peer.port)"])
        let client = tlsClient(seed.port).verifyDiscoveredServersByIp().build()
        let reconnected = try await reconnects(client, from: seed)
        XCTAssertTrue(reconnected, "client did not reconnect to the discovered server")
        XCTAssertEqual(client.connectedUrl, URL(string: "tls://127.0.0.1:\(peer.port)"))
        XCTAssertEqual(peer.connections.count, 1, "setup: one connection")
        XCTAssertNil(peer.connections.first?.serverName, "SNI")
        try? await client.close()

        // a discovered address that has no name to inherit
        let unnamed = try tlsAfterInfo { $0.tls = ipOnly }
        let byIp = try tlsAfterInfo {
            $0.tls = ipOnly
            $0.afterFirstPong = FakeNatsServer.info(connectUrls: ["127.0.0.1:\(unnamed.port)"])
        }
        let byIpClient = NatsClientOptions()
            .url(URL(string: "tls://127.0.0.1:\(byIp.port)")!)
            .rootCertificates(pem("rootCA"))
            .verifyDiscoveredServersByIp()
            .reconnectWait(0.1)
            .build()
        let toUnnamed = try await reconnects(byIpClient, from: byIp)
        XCTAssertTrue(toUnnamed, "client did not reconnect to the address without a name")
        XCTAssertEqual(byIpClient.connectedUrl, URL(string: "tls://127.0.0.1:\(unnamed.port)"))
        try? await byIpClient.close()

        // no effect while tlsServerName(_:) is set
        let overridden = try tlsAfterInfo { $0.tls = ipOnly }
        let named = try tlsSeed(listing: ["127.0.0.1:\(overridden.port)"])
        let namedClient = tlsClient(named.port)
            .tlsServerName("localhost")
            .verifyDiscoveredServersByIp()
            .usernameAndPassword("user", "secret")
            .build()
        try await namedClient.connect()
        let rtt = await result(within: 5) { try await namedClient.rtt() }
        XCTAssertNotNil(try? rtt?.get(), "no PONG")
        named.shutdown()
        let refused = try await eventually { overridden.connections.first?.closed == true }
        try? await namedClient.close()
        XCTAssertTrue(refused, "the discovered server was not dialed, or its connection left open")
        XCTAssertEqual(overridden.connections.first?.serverName, "localhost", "SNI")
        XCTAssertFalse(overridden.lines.contains { $0.hasPrefix("CONNECT") }, "\(overridden.lines)")
    }

    func testNameErrorsOfADiscoveredIpPointAtVerifyDiscoveredServersByIp() async throws {
        let remedy =
            "verifyDiscoveredServersByIp() verifies a discovered IP address against itself, which "
            + "is safe only where the CA that issued the certificates is not shared with others"
        // reached by its address and without TLS, the seed lends no name to what it lists
        let unnamed = try tlsAfterInfo()
        let plain = try refusingSeed(listing: ["127.0.0.1:\(unnamed.port)"])
        let client = NatsClientOptions().url(URL(string: "nats://127.0.0.1:\(plain.port)")!).build()
        let connected = await result(within: 10) { try await client.connect() }
        guard case .failure(NatsError.ConnectError.tlsFailure(let cause))? = connected else {
            try? await client.close()
            return XCTFail("expected a TLS failure, got \(String(describing: connected))")
        }
        XCTAssertTrue("\(cause)".contains("with no name to verify"), "\(cause)")
        XCTAssertTrue("\(cause)".contains(", or " + remedy), "\(cause)")

        // a certificate issued for the address, verified against the inherited name
        guard let mismatch = try await nameErrorOfDiscoveredIp()?.message else {
            return
        }
        XCTAssertTrue(
            mismatch.contains("(client_advertise in nats-server), or " + remedy), mismatch)
    }

    func testRootedNamesAreSentWithoutTheTrailingDot() async throws {
        let peer = try tlsAfterInfo()
        let seed = try tlsSeed(listing: ["localhost.:\(peer.port)"])
        let client = tlsClient(seed.port).build()
        let reconnected = try await reconnects(client, from: seed)
        XCTAssertTrue(reconnected, "client did not reconnect to the discovered server")
        XCTAssertEqual(client.connectedUrl, URL(string: "tls://localhost.:\(peer.port)"))
        XCTAssertEqual(peer.connections.first?.serverName, "localhost", "SNI of a discovered name")
        try? await client.close()

        // an address discovered through a rooted configured name inherits the name without it
        let ipPeer = try tlsAfterInfo()
        let rootedSeed = try tlsSeed(listing: ["127.0.0.1:\(ipPeer.port)"])
        let rootedClient = NatsClientOptions()
            .url(URL(string: "tls://localhost.:\(rootedSeed.port)")!)
            .rootCertificates(pem("rootCA"))
            .reconnectWait(0.1)
            .build()
        let toIp = try await reconnects(rootedClient, from: rootedSeed)
        XCTAssertEqual(rootedSeed.connections.first?.serverName, "localhost", "SNI of the URL")
        XCTAssertTrue(toIp, "client did not reconnect to the discovered address")
        XCTAssertEqual(ipPeer.connections.first?.serverName, "localhost", "SNI of the address")
        try? await rootedClient.close()

        let named = try tlsAfterInfo()
        let namedClient = NatsClientOptions()
            .url(URL(string: "tls://127.0.0.1:\(named.port)")!)
            .rootCertificates(pem("rootCA"))
            .tlsServerName("localhost.")
            .build()
        let connected = await result(within: 10) { try await namedClient.connect() }
        XCTAssertEqual(named.connections.first?.serverName, "localhost", "SNI of tlsServerName(_:)")
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        try await namedClient.close()
    }
}
