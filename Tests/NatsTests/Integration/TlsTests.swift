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

/// When the client uses TLS, against nats-server and against scripted servers.
class TlsTests: XCTestCase {

    static var allTests = [
        ("testTlsToIpv4Address", testTlsToIpv4Address),
        ("testTlsToIpv6Address", testTlsToIpv6Address),
        ("testTlsToIpAddressVerifiesTheAddress", testTlsToIpAddressVerifiesTheAddress),
        ("testUppercaseWebsocketSchemeConnects", testUppercaseWebsocketSchemeConnects),
        (
            "testTlsUrlUsesTlsWhenServerDoesNotRequireIt",
            testTlsUrlUsesTlsWhenServerDoesNotRequireIt
        ),
        ("testUppercaseTlsSchemeUsesTls", testUppercaseTlsSchemeUsesTls),
        ("testTlsUrlUsesTlsOnReconnect", testTlsUrlUsesTlsOnReconnect),
        ("testTlsFirstAloneDoesTlsFirst", testTlsFirstAloneDoesTlsFirst),
        ("testRootCertificatesUseTls", testRootCertificatesUseTls),
        ("testClientCertificateUsesTls", testClientCertificateUsesTls),
        ("testTlsServerNameUsesTls", testTlsServerNameUsesTls),
        ("testVerifyDiscoveredServersByIpUsesTls", testVerifyDiscoveredServersByIpUsesTls),
        ("testTlsUrlMakesEveryListedServerTls", testTlsUrlMakesEveryListedServerTls),
        ("testWssUrlWithTlsFirstConnects", testWssUrlWithTlsFirstConnects),
        ("testTlsOptionalDialsEachUrlByItsScheme", testTlsOptionalDialsEachUrlByItsScheme),
        (
            "testTlsOptionalWithRequireTlsIsAConfigurationError",
            testTlsOptionalWithRequireTlsIsAConfigurationError
        ),
        (
            "testTlsOptionalWithTlsFirstIsAConfigurationError",
            testTlsOptionalWithTlsFirstIsAConfigurationError
        ),
        ("testWsUrlIsRefusedWhenTlsIsAskedFor", testWsUrlIsRefusedWhenTlsIsAskedFor),
        ("testWsUrlIsNotUpgradedToTlsAfterInfo", testWsUrlIsNotUpgradedToTlsAfterInfo),
        (
            "testReportedWsUrlIsNotDialedWhenTlsIsAskedFor",
            testReportedWsUrlIsNotDialedWhenTlsIsAskedFor
        ),
        (
            "testServerWithoutTlsFailsBeforeConnectNamingTheSetting",
            testServerWithoutTlsFailsBeforeConnectNamingTheSetting
        ),
        (
            "testServerWithoutTlsIsReportedOnReconnect",
            testServerWithoutTlsIsReportedOnReconnect
        ),
        (
            "testHostNameUrlRefusesCertificateWithOnlyAnIpEntry",
            testHostNameUrlRefusesCertificateWithOnlyAnIpEntry
        ),
        (
            "testIpAddressUrlAcceptsCertificateWithThatIpEntry",
            testIpAddressUrlAcceptsCertificateWithThatIpEntry
        ),
        ("testNothingIsSentBeforeTheNameCheck", testNothingIsSentBeforeTheNameCheck),
        (
            "testNameMismatchIsReportedWhenTheServerClosesAtOnce",
            testNameMismatchIsReportedWhenTheServerClosesAtOnce
        ),
        (
            "testRefusedNameFailsWhenTheServerWritesAndResets",
            testRefusedNameFailsWhenTheServerWritesAndResets
        ),
        (
            "testNextServerIsTriedAtOnceAfterANameMismatch",
            testNextServerIsTriedAtOnceAfterANameMismatch
        ),
        (
            "testNameMismatchIsReportedWhileCredentialsAreRead",
            testNameMismatchIsReportedWhileCredentialsAreRead
        ),
        ("testNameMismatchIsReportedOnReconnect", testNameMismatchIsReportedOnReconnect),
    ]

    private var natsServers: [NatsServer] = []
    private var fakeServers: [FakeNatsServer] = []

    private var rootCA: URL {
        pem("rootCA")
    }

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
    /// filled with the paths of the PEM files `pems`. It is stopped in `tearDown`.
    private func natsServer(_ name: String, pems: [String] = []) throws -> NatsServer {
        let template = try String(
            contentsOf: Bundle.module.url(forResource: name, withExtension: "conf")!,
            encoding: .utf8)
        let config = String(format: template, arguments: pems.map { pem($0).path as CVarArg })
        let file = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString).appendingPathExtension("conf")
        try config.write(to: file, atomically: true, encoding: .utf8)
        let server = NatsServer()
        server.start(cfg: file.path)
        natsServers.append(server)
        return server
    }

    /// Starts a scripted server that is stopped in `tearDown`.
    private func fake(_ script: FakeNatsServer.Script) throws -> FakeNatsServer {
        try fake(scripts: [script])
    }

    /// Starts a scripted server whose connection `i` follows `scripts[i]`, stopped in `tearDown`.
    private func fake(scripts: [FakeNatsServer.Script]) throws -> FakeNatsServer {
        let server = try FakeNatsServer(scripts: scripts)
        fakeServers.append(server)
        return server
    }

    /// Waits up to 2 s for `server` to see its first connection closed.
    private func firstConnectionClosed(on server: FakeNatsServer) async throws -> Bool {
        for _ in 0..<40 {
            if server.connections.first?.closed == true {
                return true
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        return false
    }

    /// A scripted server whose INFO offers TLS without requiring it.
    private func fakeOfferingTls() throws -> FakeNatsServer {
        try fake(
            .init(
                greeting: FakeNatsServer.info(tlsAvailable: true),
                tls: try FakeNatsServer.tlsContext()))
    }

    /// Lets a client built from `options` try to connect, then checks that the first bytes
    /// `server` received are a TLS record and that it read no CONNECT in plaintext. The client
    /// does not trust the server's certificate, so its connect fails.
    private func assertStartsTls(
        _ options: NatsClientOptions, on server: FakeNatsServer, _ message: String = "",
        file: StaticString = #filePath, line: UInt = #line
    ) async {
        let client = options.usernameAndPassword("user", "secret").build()
        let connected = await result(within: 10) { try await client.connect() }
        if case .success? = connected {
            try? await client.close()
        }
        let first = server.connections.first?.firstBytes ?? []
        XCTAssertEqual(
            first.first, 0x16,
            "\(message) first bytes are not a TLS record: \(String(decoding: first, as: UTF8.self))",
            file: file, line: line)
        XCTAssertFalse(
            server.lines.contains { $0.hasPrefix("CONNECT") }, "\(message) CONNECT in plaintext",
            file: file, line: line)
    }

    /// Checks that `connect()` of a client built from `options()` fails at once with a
    /// configuration error that names every string in `naming`, also with
    /// `retryOnfailedConnect()`, and that `server`, when given, was not connected to, also
    /// after `rtt()` calls.
    private func assertConfigurationError(
        _ options: () -> NatsClientOptions, naming: [String], server: FakeNatsServer? = nil,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        for retry in [false, true] {
            let client = (retry ? options().retryOnfailedConnect() : options())
                .reconnectWait(0.05)
                .build()
            let connected = await result(within: 5) { try await client.connect() }
            guard case .failure(NatsError.ConnectError.invalidConfig(let reason))? = connected
            else {
                XCTFail(
                    "retry \(retry): expected a configuration error, got "
                        + String(describing: connected), file: file, line: line)
                try? await client.close()
                continue
            }
            for name in naming {
                XCTAssertTrue(
                    reason.contains(name), "retry \(retry): \(name) not in: \(reason)", file: file,
                    line: line)
            }
            // pings that get no answer start a reconnect
            for _ in 0..<3 {
                _ = await result(within: 1) { try await client.rtt() }
            }
            if let server {
                try await Task.sleep(nanoseconds: 300_000_000)
                XCTAssertEqual(
                    server.connections.count, 0, "retry \(retry): connected after rtt()",
                    file: file, line: line)
            }
            try? await client.close()
        }
        if let server {
            try await Task.sleep(nanoseconds: 300_000_000)
            XCTAssertEqual(
                server.connections.count, 0, "connected although the settings are refused",
                file: file, line: line)
        }
    }

    /// The error text for a `localhost` URL and `ip-only-cert.pem`, which is issued for
    /// 127.0.0.1 and ::1 and for no DNS name.
    private static let ipOnlyRefusal =
        "is not valid for \"localhost\": it is issued for \"127.0.0.1\", \"::1\""

    /// Lets a client built from `options` try to connect to a `localhost` URL of a server with
    /// `ip-only-cert.pem`, then checks that it fails within `timeout` with an error that says what
    /// the client expected and what the certificate is issued for. The server is a configured
    /// one, so the error points at `tlsServerName(_:)` and not at the cluster's advertised names.
    private func assertNotValidForName(
        _ options: NatsClientOptions, within timeout: TimeInterval = 5, _ message: String = "",
        file: StaticString = #filePath, line: UInt = #line
    ) async {
        let client = options.build()
        let connected = await result(within: timeout) { try await client.connect() }
        if case .success? = connected {
            try? await client.close()
        }
        guard case .failure(NatsError.ConnectError.tlsFailure(let cause))? = connected else {
            return XCTFail(
                "\(message) expected a TLS failure, got \(String(describing: connected))",
                file: file, line: line)
        }
        XCTAssertTrue(
            "\(cause)".contains(Self.ipOnlyRefusal), "\(message) \(cause)", file: file, line: line)
        XCTAssertTrue(
            "\(cause)".contains(
                "; tlsServerName(_:) sets the name every server is verified against"),
            "\(message) \(cause)", file: file, line: line)
        XCTAssertFalse(
            "\(cause)".contains("client_advertise"), "\(message) \(cause)", file: file,
            line: line)
    }

    /// Connects a client built from `options`, flushes and closes it, within 10 s.
    private func assertConnects(
        _ options: NatsClientOptions, _ message: String = "", file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        let client = options.build()
        let connected = await result(within: 10) {
            try await client.connect()
            try await client.flush()
        }
        guard case .success? = connected else {
            return XCTFail(
                "\(message) connect: \(String(describing: connected))", file: file, line: line)
        }
        try? await client.close()
    }

    func testTlsToIpv4Address() async throws {
        let server = try natsServer("tls_ip", pems: ["ip-cert", "ip-key", "ip-ca"])
        await assertConnects(
            NatsClientOptions()
                .url(URL(string: "tls://127.0.0.1:\(server.port!)")!)
                .rootCertificates(pem("ip-ca")))
    }

    func testTlsToIpv6Address() async throws {
        let server = try natsServer("tls_ip", pems: ["ip-cert", "ip-key", "ip-ca"])
        await assertConnects(
            NatsClientOptions()
                .url(URL(string: "tls://[::1]:\(server.port!)")!)
                .rootCertificates(pem("ip-ca")))
    }

    // The certificate names localhost and ::1, but not 127.0.0.1.
    func testTlsToIpAddressVerifiesTheAddress() async throws {
        let server = try natsServer("tls_ip", pems: ["server-cert", "server-key", "rootCA"])
        let client = NatsClientOptions()
            .url(URL(string: "tls://127.0.0.1:\(server.port!)")!)
            .rootCertificates(rootCA)
            .build()

        let connected = await result(within: 10) { try await client.connect() }
        guard case .failure(NatsError.ConnectError.tlsFailure)? = connected else {
            try? await client.close()
            return XCTFail(
                "connect must fail on the certificate, got \(String(describing: connected))")
        }
    }

    func testUppercaseWebsocketSchemeConnects() async throws {
        let ws = try natsServer("ws")
        let wsUrl = ws.clientWebsocketURL.replacingOccurrences(of: "ws://", with: "WS://")
        await assertConnects(NatsClientOptions().url(URL(string: wsUrl)!), wsUrl)
        ws.stop()

        let wss = try natsServer("wss", pems: ["server-cert", "server-key", "rootCA"])
        let wssUrl = wss.clientWebsocketURL.replacingOccurrences(of: "wss://", with: "WSS://")
        await assertConnects(
            NatsClientOptions()
                .url(URL(string: wssUrl)!)
                .rootCertificates(rootCA)
                .clientCertificate(pem("client-cert"), pem("client-key")), wssUrl)
    }

    func testTlsUrlUsesTlsWhenServerDoesNotRequireIt() async throws {
        let server = try fakeOfferingTls()
        await assertStartsTls(
            NatsClientOptions().url(URL(string: "tls://localhost:\(server.port)")!), on: server)
    }

    func testUppercaseTlsSchemeUsesTls() async throws {
        let server = try fakeOfferingTls()
        await assertStartsTls(
            NatsClientOptions().url(URL(string: "TLS://localhost:\(server.port)")!), on: server)
    }

    func testTlsUrlUsesTlsOnReconnect() async throws {
        // only the INFO of the first connection requires TLS
        let server = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true),
                laterGreeting: FakeNatsServer.info(tlsAvailable: true),
                tls: try FakeNatsServer.tlsContext()))
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(server.port)")!)
            .rootCertificates(rootCA)
            .usernameAndPassword("user", "secret")
            .reconnectWait(0.1)
            .build()
        try await client.connect()

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        server.closeConnection(0)
        await fulfillment(of: [reconnected], timeout: 5)
        let connections = server.connections
        XCTAssertGreaterThan(connections.count, 1, "client did not reconnect")
        for (index, connection) in connections.enumerated().dropFirst() {
            XCTAssertEqual(
                connection.firstBytes.first, 0x16,
                "reconnect \(index): "
                    + String(decoding: connection.firstBytes, as: UTF8.self))
        }
        try? await client.close()
    }

    func testTlsFirstAloneDoesTlsFirst() async throws {
        let server = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true),
                tls: try FakeNatsServer.tlsContext(), tlsFirst: true))
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(server.port)")!)
            .rootCertificates(rootCA)
            .withTlsFirst()
            .build()

        let connected = await result(within: 5) { try await client.connect() }
        XCTAssertEqual(server.connections.first?.firstBytes.first, 0x16, "no TLS first")
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        try await client.close()
    }

    func testRootCertificatesUseTls() async throws {
        let server = try fakeOfferingTls()
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(server.port)")!)
            .rootCertificates(rootCA)
            .usernameAndPassword("user", "secret")
            .build()

        let connected = await result(within: 10) { try await client.connect() }
        let first = server.connections.first?.firstBytes ?? []
        XCTAssertEqual(
            first.first, 0x16,
            "first bytes are not a TLS record: \(String(decoding: first, as: UTF8.self))")
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        try await client.close()
    }

    func testClientCertificateUsesTls() async throws {
        let server = try fakeOfferingTls()
        await assertStartsTls(
            NatsClientOptions()
                .url(URL(string: "nats://localhost:\(server.port)")!)
                .clientCertificate(pem("client-cert"), pem("client-key")), on: server)
    }

    func testTlsServerNameUsesTls() async throws {
        let server = try fakeOfferingTls()
        await assertStartsTls(
            NatsClientOptions()
                .url(URL(string: "nats://localhost:\(server.port)")!)
                .tlsServerName("localhost"), on: server)
    }

    func testVerifyDiscoveredServersByIpUsesTls() async throws {
        let server = try fakeOfferingTls()
        await assertStartsTls(
            NatsClientOptions()
                .url(URL(string: "nats://localhost:\(server.port)")!)
                .verifyDiscoveredServersByIp(), on: server)
    }

    func testTlsUrlMakesEveryListedServerTls() async throws {
        for scheme in ["tls", "wss", "TLS", "WSS"] {
            let server = try fakeOfferingTls()
            await assertStartsTls(
                NatsClientOptions()
                    .urls([
                        URL(string: "nats://localhost:\(server.port)")!,
                        // nothing listens there; it is in the list for its scheme
                        URL(string: "\(scheme)://127.0.0.1:1")!,
                    ])
                    .retainServersOrder(), on: server, scheme)
        }
    }

    func testWssUrlWithTlsFirstConnects() async throws {
        let wss = try natsServer("wss", pems: ["server-cert", "server-key", "rootCA"])
        let variants = [
            ("withTlsFirst()", NatsClientOptions().withTlsFirst()),
            ("withTlsFirst() and requireTls()", NatsClientOptions().withTlsFirst().requireTls()),
        ]
        for (name, options) in variants {
            await assertConnects(
                options
                    .url(URL(string: wss.clientWebsocketURL)!)
                    .rootCertificates(rootCA)
                    .clientCertificate(pem("client-cert"), pem("client-key")), name)
        }
    }

    func testTlsOptionalDialsEachUrlByItsScheme() async throws {
        let plain = try fake(.init(greeting: FakeNatsServer.info()))
        let tls = try fakeOfferingTls()
        let client = NatsClientOptions()
            .urls([
                URL(string: "nats://127.0.0.1:\(plain.port)")!,
                URL(string: "TLS://localhost:\(tls.port)")!,
            ])
            .retainServersOrder()
            .rootCertificates(rootCA)
            .tlsOptional()
            .usernameAndPassword("user", "secret")
            .reconnectWait(0.1)
            .build()

        let connected = await result(within: 5) { try await client.connect() }
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        XCTAssertTrue(
            plain.lines.contains { $0.hasPrefix("CONNECT") }, "nats:// URL: \(plain.lines)")

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        plain.shutdown()
        await fulfillment(of: [reconnected], timeout: 5)
        let first = tls.connections.first?.firstBytes ?? []
        XCTAssertEqual(
            first.first, 0x16,
            "tls:// URL: first bytes are not a TLS record: "
                + String(decoding: first, as: UTF8.self))
        try? await client.close()
    }

    func testTlsOptionalWithRequireTlsIsAConfigurationError() async throws {
        let server = try fake(.init(greeting: FakeNatsServer.info()))
        try await assertConfigurationError(
            {
                NatsClientOptions()
                    .url(URL(string: "nats://127.0.0.1:\(server.port)")!)
                    .requireTls()
                    .tlsOptional()
            }, naming: ["tlsOptional()", "requireTls()"], server: server)
    }

    func testTlsOptionalWithTlsFirstIsAConfigurationError() async throws {
        let server = try fake(.init(greeting: FakeNatsServer.info()))
        try await assertConfigurationError(
            {
                NatsClientOptions()
                    .url(URL(string: "nats://127.0.0.1:\(server.port)")!)
                    .withTlsFirst()
                    .tlsOptional()
            }, naming: ["tlsOptional()", "withTlsFirst()"], server: server)
    }

    func testWsUrlIsRefusedWhenTlsIsAskedFor() async throws {
        let ws = try natsServer("ws")
        let wsUrl = URL(string: ws.clientWebsocketURL)!
        let refused = "\"\(wsUrl.absoluteString)\" is a ws:// URL"
        let variants: [(String, () -> NatsClientOptions)] = [
            (
                "rootCertificates(_:)",
                { NatsClientOptions().url(wsUrl).rootCertificates(self.rootCA) }
            ),
            ("requireTls()", { NatsClientOptions().url(wsUrl).requireTls() }),
            ("withTlsFirst()", { NatsClientOptions().url(wsUrl).withTlsFirst() }),
            (
                "the URL \"wss://localhost:1\"",
                {
                    NatsClientOptions()
                        .urls([URL(string: "wss://localhost:1")!, wsUrl])
                        .retainServersOrder()
                }
            ),
        ]
        for (askedBy, options) in variants {
            try await assertConfigurationError(
                options, naming: [askedBy, refused, "wss:// is the TLS form"])
        }

        // refused before any attempt, in any case of the scheme
        for scheme in ["ws", "WS"] {
            let server = try fake(.init(greeting: FakeNatsServer.info()))
            try await assertConfigurationError(
                {
                    NatsClientOptions()
                        .url(URL(string: "\(scheme)://127.0.0.1:\(server.port)")!)
                        .requireTls()
                }, naming: ["requireTls()", "is a ws:// URL"], server: server)
        }

        // with tlsOptional() the URL is dialed as written
        await assertConnects(
            NatsClientOptions().url(wsUrl).rootCertificates(rootCA).tlsOptional(), "tlsOptional()")
    }

    // TLS after INFO would start below the websocket framing after the upgrade, so a ws://
    // connection goes on without it, also when its INFO requires TLS.
    func testWsUrlIsNotUpgradedToTlsAfterInfo() async throws {
        let variants: [(String, (URL) -> NatsClientOptions)] = [
            ("no TLS setting", { NatsClientOptions().url($0) }),
            (
                "tlsOptional()",
                { NatsClientOptions().url($0).rootCertificates(self.rootCA).tlsOptional() }
            ),
        ]
        for (name, options) in variants {
            let server = try fake(
                .init(greeting: FakeNatsServer.info(tlsRequired: true), websocket: true))
            await assertConnects(options(URL(string: "ws://127.0.0.1:\(server.port)")!), name)
            let first = server.connections.first?.firstBytes ?? []
            XCTAssertNotEqual(first.first, 0x16, "\(name): TLS started after the websocket upgrade")
            XCTAssertTrue(
                server.lines.contains { $0.hasPrefix("CONNECT") },
                "\(name): no CONNECT in a websocket frame: \(server.lines)")
        }
    }

    // A ws:// URL that a server reports is skipped, like any entry with a scheme.
    func testReportedWsUrlIsNotDialedWhenTlsIsAskedFor() async throws {
        let ws = try fake(.init(greeting: FakeNatsServer.info(), websocket: true))
        let server = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true),
                tls: try FakeNatsServer.tlsContext(),
                afterFirstPong: FakeNatsServer.info(
                    tlsRequired: true, connectUrls: ["ws://127.0.0.1:\(ws.port)"])))
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(server.port)")!)
            .requireTls()
            .rootCertificates(rootCA)
            .usernameAndPassword("user", "secret")
            .retainServersOrder()
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        // the INFO with the ws:// URL came before the PONG of this flush
        try await client.flush()
        server.shutdown()

        // an entry with a scheme is skipped: the client reconnects only to its configured
        // server, and never dials the ws:// URL
        try await Task.sleep(nanoseconds: 1_000_000_000)
        try? await client.close()
        XCTAssertEqual(ws.connections.count, 0, "the reported ws:// URL was dialed")
    }

    func testServerWithoutTlsFailsBeforeConnectNamingTheSetting() async throws {
        let optOut = "tlsOptional() leaves TLS to each server"
        let variants: [(String, String, Bool, (NatsClientOptions) -> NatsClientOptions)] = [
            ("nats", "rootCertificates(_:)", true, { $0.rootCertificates(self.rootCA) }),
            (
                "nats", "clientCertificate(_:_:)", true,
                { $0.clientCertificate(self.pem("client-cert"), self.pem("client-key")) }
            ),
            ("nats", "requireTls()", true, { $0.requireTls() }),
            ("tls", "the URL \"tls://", true, { $0 }),
            // the URL alone asks for TLS, so the opt-out would change nothing
            ("tls", "the URL \"tls://", false, { $0.tlsOptional() }),
        ]
        for (scheme, askedBy, namesOptOut, configure) in variants {
            let server = try fake(.init(greeting: FakeNatsServer.info()))
            let url = URL(string: "\(scheme)://u:urlsecret@127.0.0.1:\(server.port)")!
            let client = configure(NatsClientOptions().url(url))
                .usernameAndPassword("user", "secret")
                .build()
            let name = namesOptOut ? askedBy : "\(askedBy) with tlsOptional()"

            let connected = await result(within: 5) { try await client.connect() }
            guard case .failure(NatsError.ConnectError.tlsFailure(let cause))? = connected else {
                XCTFail("\(name): expected a TLS failure, got \(String(describing: connected))")
                continue
            }
            let message = "\(cause)"
            XCTAssertTrue(message.contains(askedBy), "\(name): \(message)")
            XCTAssertFalse(message.contains("urlsecret"), "\(name): \(message)")
            XCTAssertTrue(
                message.contains("\"\(scheme)://127.0.0.1:\(server.port)\" does not offer it"),
                "\(name): \(message)")
            XCTAssertEqual(message.contains(optOut), namesOptOut, "\(name): \(message)")
            let isClosed = try await firstConnectionClosed(on: server)
            XCTAssertTrue(isClosed, "\(name): connection of the failed attempt left open")
            let sent = server.connections.first?.firstBytes ?? []
            XCTAssertTrue(
                sent.isEmpty, "\(name): sent " + String(decoding: sent, as: UTF8.self))
        }
    }

    // On a reconnect, and on every attempt under retryOnfailedConnect(), no connect() call
    // returns the failure: an error event reports it.
    func testServerWithoutTlsIsReportedOnReconnect() async throws {
        let withoutTls = try fake(.init(greeting: FakeNatsServer.info()))
        // only the INFO of the first connection offers TLS
        let laterWithoutTls = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true),
                laterGreeting: FakeNatsServer.info(), tls: try FakeNatsServer.tlsContext()))
        let variants = [
            ("retryOnfailedConnect()", withoutTls, true),
            ("reconnect", laterWithoutTls, false),
        ]
        for (name, server, retry) in variants {
            let url = URL(string: "nats://localhost:\(server.port)")!
            let options = NatsClientOptions()
                .url(url)
                .requireTls()
                .rootCertificates(rootCA)
                .reconnectWait(0.2)
            let client = (retry ? options.retryOnfailedConnect() : options).build()
            let reported = XCTestExpectation(description: "\(name): the failure was not reported")
            reported.assertForOverFulfill = false
            client.on(.error) { event in
                guard case .error(let error) = event else { return }
                let message = "\(error)"
                if message.contains("requireTls()")
                    && message.contains("\"\(url.absoluteString)\" does not offer it")
                    && message.contains("tlsOptional()")
                {
                    reported.fulfill()
                }
            }
            try await client.connect()
            if !retry {
                server.closeConnection(0)
            }
            await fulfillment(of: [reported], timeout: 2)
            try? await client.close()
        }
    }

    // ip-only-cert.pem is issued by rootCA for the addresses 127.0.0.1 and ::1 only.
    func testHostNameUrlRefusesCertificateWithOnlyAnIpEntry() async throws {
        let server = try natsServer("tls_ip", pems: ["ip-only-cert", "ip-only-key", "rootCA"])
        await assertNotValidForName(
            NatsClientOptions()
                .url(URL(string: "tls://localhost:\(server.port!)")!)
                .rootCertificates(rootCA), within: 10, "tls://")

        let wss = try natsServer("wss", pems: ["ip-only-cert", "ip-only-key", "rootCA"])
        await assertNotValidForName(
            NatsClientOptions()
                .url(URL(string: wss.clientWebsocketURL)!)
                .rootCertificates(rootCA)
                .clientCertificate(pem("client-cert"), pem("client-key")), within: 10, "wss://")
    }

    func testIpAddressUrlAcceptsCertificateWithThatIpEntry() async throws {
        let server = try natsServer("tls_ip", pems: ["ip-only-cert", "ip-only-key", "rootCA"])
        for host in ["127.0.0.1", "[::1]"] {
            await assertConnects(
                NatsClientOptions()
                    .url(URL(string: "tls://\(host):\(server.port!)")!)
                    .rootCertificates(rootCA), host)
        }
    }

    func testNothingIsSentBeforeTheNameCheck() async throws {
        let tls = try FakeNatsServer.tlsContext(cert: "ip-only-cert", key: "ip-only-key")
        let variants: [(String, FakeNatsServer.Script, (Int) -> NatsClientOptions)] = [
            (
                "TLS after INFO",
                .init(greeting: FakeNatsServer.info(tlsRequired: true), tls: tls),
                { NatsClientOptions().url(URL(string: "tls://localhost:\($0)")!) }
            ),
            (
                "TLS first",
                .init(greeting: FakeNatsServer.info(tlsRequired: true), tls: tls, tlsFirst: true),
                { NatsClientOptions().url(URL(string: "nats://localhost:\($0)")!).withTlsFirst() }
            ),
            (
                // the scripted server writes nothing: the upgrade request would be its first line
                "wss://",
                .init(tls: tls, tlsFirst: true),
                { NatsClientOptions().url(URL(string: "wss://localhost:\($0)")!) }
            ),
        ]
        for (name, script, options) in variants {
            let server = try fake(script)
            await assertNotValidForName(
                options(server.port).rootCertificates(rootCA).usernameAndPassword("user", "secret"),
                name)
            XCTAssertEqual(server.lines, [], "\(name): the server received lines")
            let isClosed = try await firstConnectionClosed(on: server)
            XCTAssertTrue(isClosed, "\(name): connection of the failed attempt left open")
        }
    }

    func testNameMismatchIsReportedWhenTheServerClosesAtOnce() async throws {
        let tls = try FakeNatsServer.tlsContext(cert: "ip-only-cert", key: "ip-only-key")
        let variants: [(String, FakeNatsServer.Script, (Int) -> NatsClientOptions)] = [
            (
                "TLS after INFO",
                .init(
                    greeting: FakeNatsServer.info(tlsRequired: true), tls: tls,
                    afterTlsHandshake: .closes),
                { NatsClientOptions().url(URL(string: "tls://localhost:\($0)")!) }
            ),
            (
                "TLS first",
                .init(
                    greeting: FakeNatsServer.info(tlsRequired: true), tls: tls, tlsFirst: true,
                    afterTlsHandshake: .closes),
                { NatsClientOptions().url(URL(string: "nats://localhost:\($0)")!).withTlsFirst() }
            ),
        ]
        for (name, script, options) in variants {
            let server = try fake(script)
            await assertNotValidForName(options(server.port).rootCertificates(rootCA), name)
        }
    }

    func testRefusedNameFailsWhenTheServerWritesAndResets() async throws {
        // TLS 1.2: the server's handshake is done before the client's, so its line and the
        // reset can arrive right after the end of the client's handshake
        let tls = try FakeNatsServer.tlsContext(
            cert: "ip-only-cert", key: "ip-only-key", maximumTLSVersion: .tlsv12)
        let variants: [(String, FakeNatsServer.Script, (Int) -> NatsClientOptions)] = [
            (
                "TLS after INFO",
                .init(
                    greeting: FakeNatsServer.info(tlsRequired: true), tls: tls,
                    afterTlsHandshake: .writesAndResets("PONG\r\n")),
                { NatsClientOptions().url(URL(string: "tls://localhost:\($0)")!) }
            ),
            (
                "TLS first",
                .init(
                    tls: tls, tlsFirst: true,
                    afterTlsHandshake: .writesAndResets(FakeNatsServer.info(tlsRequired: true))),
                { NatsClientOptions().url(URL(string: "nats://localhost:\($0)")!).withTlsFirst() }
            ),
        ]
        for (name, script, options) in variants {
            let server = try fake(script)
            // what the client reads before the reset varies from one attempt to the next
            for attempt in 1...5 {
                await assertNotValidForName(
                    options(server.port).rootCertificates(rootCA), within: 2,
                    "\(name), attempt \(attempt):")
            }
        }
    }

    func testNextServerIsTriedAtOnceAfterANameMismatch() async throws {
        // TLS 1.2: the server's handshake is done before the client's, so the server drops all
        // the client sends after its handshake and never answers the close of the refused
        // connection
        let refused = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true),
                tls: try FakeNatsServer.tlsContext(
                    cert: "ip-only-cert", key: "ip-only-key", maximumTLSVersion: .tlsv12),
                afterTlsHandshake: .dropsBytes))
        let good = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true),
                tls: try FakeNatsServer.tlsContext()))
        let client = NatsClientOptions()
            .urls([
                URL(string: "tls://localhost:\(refused.port)")!,
                URL(string: "tls://localhost:\(good.port)")!,
            ])
            .retainServersOrder()
            .rootCertificates(rootCA)
            .build()

        let connected = await result(within: 2) { try await client.connect() }
        if case .success? = connected {
            try? await client.close()
        }
        guard case .success? = connected else {
            return XCTFail("connect within 2 s: \(String(describing: connected))")
        }
        XCTAssertEqual(refused.connections.count, 1, "the refused server was not tried first")
        XCTAssertEqual(refused.lines, [], "the refused server received lines")
        XCTAssertTrue(
            good.lines.contains { $0.hasPrefix("CONNECT") }, "no CONNECT to the next server")
    }

    func testNameMismatchIsReportedWhileCredentialsAreRead() async throws {
        // the INFO has a nonce, which the client signs with the key from the file it reads
        let server = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true, nonce: "PJnVGfQrFXwVEmAb"),
                tls: try FakeNatsServer.tlsContext(cert: "ip-only-cert", key: "ip-only-key")))
        let keyFiles: [(String, URL, (NatsClientOptions, URL) -> NatsClientOptions)] = [
            (
                "credentialsFile(_:)",
                Bundle.module.url(forResource: "TestUser", withExtension: "creds")!,
                { $0.credentialsFile($1) }
            ),
            (
                "nkeyFile(_:)", Bundle.module.url(forResource: "nkey", withExtension: "")!,
                { $0.nkeyFile($1) }
            ),
        ]
        for (name, file, withKeyFile) in keyFiles {
            // the file arrives well after the TLS handshake has failed
            let body = try String(contentsOf: file, encoding: .utf8)
            let files = try fake(
                .init(
                    greeting: "HTTP/1.1 200 OK\r\nContent-Length: \(body.utf8.count)\r\n"
                        + "Connection: close\r\n\r\n\(body)",
                    greetingDelay: 1, closeAfterGreeting: true))
            let options = NatsClientOptions()
                .url(URL(string: "tls://localhost:\(server.port)")!)
                .rootCertificates(rootCA)
            await assertNotValidForName(
                withKeyFile(
                    options,
                    URL(string: "http://127.0.0.1:\(files.port)/\(file.lastPathComponent)")!),
                name)
        }
        XCTAssertEqual(server.lines, [], "the server received lines")
    }

    // On a reconnect, and on every attempt under retryOnfailedConnect(), no connect() call
    // returns the refusal: an error event reports it.
    func testNameMismatchIsReportedOnReconnect() async throws {
        let ipOnly = FakeNatsServer.Script(
            greeting: FakeNatsServer.info(tlsRequired: true),
            tls: try FakeNatsServer.tlsContext(cert: "ip-only-cert", key: "ip-only-key"))
        let refused = try fake(ipOnly)
        // only the first connection gets a certificate issued for localhost
        let laterRefused = try fake(
            scripts: [
                .init(
                    greeting: FakeNatsServer.info(tlsRequired: true),
                    tls: try FakeNatsServer.tlsContext()),
                ipOnly,
            ])
        let variants = [
            ("retryOnfailedConnect()", refused, true),
            ("reconnect", laterRefused, false),
        ]
        for (name, server, retry) in variants {
            let options = NatsClientOptions()
                .url(URL(string: "tls://localhost:\(server.port)")!)
                .rootCertificates(rootCA)
                .reconnectWait(0.2)
            let client = (retry ? options.retryOnfailedConnect() : options).build()
            let reported = XCTestExpectation(description: "\(name): the refusal was not reported")
            reported.assertForOverFulfill = false
            client.on(.error) { event in
                guard case .error(let error) = event else { return }
                if "\(error)".contains(Self.ipOnlyRefusal) {
                    reported.fulfill()
                }
            }
            try await client.connect()
            if !retry {
                server.closeConnection(0)
            }
            await fulfillment(of: [reported], timeout: 2)
            try? await client.close()
            XCTAssertEqual(
                server.connections.dropFirst(retry ? 0 : 1).flatMap { $0.lines }, [],
                "\(name): a refused connection received lines")
        }
    }
}
