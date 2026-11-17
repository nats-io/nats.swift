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
        ("testTlsUrlMakesEveryListedServerTls", testTlsUrlMakesEveryListedServerTls),
        ("testWssUrlWithTlsFirstConnects", testWssUrlWithTlsFirstConnects),
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
        let server = try FakeNatsServer(script)
        fakeServers.append(server)
        return server
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
}
