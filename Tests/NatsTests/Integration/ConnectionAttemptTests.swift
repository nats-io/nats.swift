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
import XCTest

@testable import Nats

/// What the client does with the bytes a server sends while it connects, against scripted
/// servers.
class ConnectionAttemptTests: XCTestCase {

    static var allTests = [
        ("testFirstLineOtherThanInfoFailsTheAttempt", testFirstLineOtherThanInfoFailsTheAttempt),
        (
            "testLineThatDoesNotParseWhileConnectingIsAnIoError",
            testLineThatDoesNotParseWhileConnectingIsAnIoError
        ),
        (
            "testFirstInfoThatDoesNotDecodeFailsTheAttempt",
            testFirstInfoThatDoesNotDecodeFailsTheAttempt
        ),
    ]

    private var servers: [FakeNatsServer] = []

    private var rootCA: URL {
        Bundle.module.url(forResource: "rootCA", withExtension: "pem")!
    }

    override func setUp() {
        super.setUp()
        logger.logLevel = .critical
    }

    override func tearDown() {
        for server in servers {
            server.shutdown()
        }
        servers = []
        super.tearDown()
    }

    /// Starts a scripted server that is stopped in `tearDown`.
    private func fake(_ script: FakeNatsServer.Script) throws -> FakeNatsServer {
        let server = try FakeNatsServer(script)
        servers.append(server)
        return server
    }

    /// Waits up to 2 s for the server to see connection `index` closed.
    private func closed(_ index: Int, on server: FakeNatsServer) async throws -> Bool {
        for _ in 0..<40 {
            let connections = server.connections
            if connections.indices.contains(index) && connections[index].closed {
                return true
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        return false
    }

    func testFirstLineOtherThanInfoFailsTheAttempt() async throws {
        for (label, first) in [
            ("PING", "PING\r\n"),
            ("-ERR", "-ERR 'Authorization Violation'\r\n"),
        ] {
            let server = try fake(
                .init(
                    greeting: first + FakeNatsServer.info(tlsRequired: true),
                    tls: try FakeNatsServer.tlsContext()))
            let client = NatsClientOptions()
                .url(URL(string: "tls://localhost:\(server.port)")!)
                .rootCertificates(rootCA)
                .build()
            let reported = XCTestExpectation(description: "\(label): no error event")
            client.on(.error) { event in
                if case .error(let error) = event, error is NatsError.ProtocolError {
                    reported.fulfill()
                }
            }

            let connected = await result(within: 10) { try await client.connect() }
            guard case .failure(let error)? = connected else {
                XCTFail("\(label): connect must fail, got \(String(describing: connected))")
                try? await client.close()
                continue
            }
            await fulfillment(of: [reported], timeout: 5.0)
            XCTAssertFalse(
                error is NatsError.ServerError, "\(label): reported as a server error: \(error)")
            XCTAssertTrue("\(error)".contains("expected INFO"), "\(label): \(error)")
            let sent = String(decoding: server.connections.first?.firstBytes ?? [], as: UTF8.self)
            XCTAssertFalse(sent.contains("PONG"), "\(label): answered a line sent before INFO")
            let isClosed = try await closed(0, on: server)
            XCTAssertTrue(isClosed, "\(label): connection of the failed attempt left open")
        }
    }

    func testLineThatDoesNotParseWhileConnectingIsAnIoError() async throws {
        // written in answer to the client's first PING: an unknown operation, an INFO that does
        // not decode
        for (label, reply) in [
            ("unknown operation", "FOO\r\n"),
            ("INFO that does not decode", "INFO {\"server_id\":1}\r\n"),
        ] {
            let server = try fake(.init(greeting: FakeNatsServer.info(), firstPingReply: reply))
            let client = NatsClientOptions()
                .url(URL(string: "nats://127.0.0.1:\(server.port)")!)
                .build()

            let connected = await result(within: 10) { try await client.connect() }
            guard case .failure(let error)? = connected else {
                XCTFail("\(label): connect must fail, got \(String(describing: connected))")
                try? await client.close()
                continue
            }
            guard case NatsError.ConnectError.io = error else {
                XCTFail("\(label): not reported as an I/O error: \(error)")
                continue
            }
        }
    }

    func testFirstInfoThatDoesNotDecodeFailsTheAttempt() async throws {
        let server = try fake(.init(greeting: "INFO {\"server_id\":\r\n"))
        let client = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(server.port)")!)
            .build()
        let reported = XCTestExpectation(description: "no error event")
        client.on(.error) { event in
            if case .error(let error) = event, error is NatsError.ProtocolError {
                reported.fulfill()
            }
        }

        let connected = await result(within: 10) { try await client.connect() }
        guard case .failure(let error)? = connected else {
            return XCTFail("connect must fail, got \(String(describing: connected))")
        }
        await fulfillment(of: [reported], timeout: 5.0)
        if case NatsError.ConnectError.tlsFailure = error {
            XCTFail("reported as a TLS error on a connection without TLS: \(error)")
        }
        XCTAssertTrue("\(error)".contains("INFO does not parse"), "\(error)")
        let isClosed = try await closed(0, on: server)
        XCTAssertTrue(isClosed, "connection of the failed attempt left open")
    }
}
