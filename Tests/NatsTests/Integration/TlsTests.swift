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
    ]

    private var natsServers: [NatsServer] = []

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
}
