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
import NIO
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
        ("testPingAfterInfoIsNotAnsweredBeforeTls", testPingAfterInfoIsNotAnsweredBeforeTls),
        ("testInfoAfterInfoIsNotAppliedBeforeTls", testInfoAfterInfoIsNotAppliedBeforeTls),
        (
            "testMessageBeforeTlsIsNotDeliveredOnReconnect",
            testMessageBeforeTlsIsNotDeliveredOnReconnect
        ),
        (
            "testLinesOfThePreviousConnectionDoNotCompleteTheReconnect",
            testLinesOfThePreviousConnectionDoNotCompleteTheReconnect
        ),
        ("testLaterReadsBeforeTlsAreDropped", testLaterReadsBeforeTlsAreDropped),
        (
            "testPartialLineBeforeTlsIsNotJoinedToTlsData",
            testPartialLineBeforeTlsIsNotJoinedToTlsData
        ),
        (
            "testServersFromInfoBeforeFailedTlsAreNotDialed",
            testServersFromInfoBeforeFailedTlsAreNotDialed
        ),
        (
            "testServerThatClosesAfterInfoFailsTheAttempt",
            testServerThatClosesAfterInfoFailsTheAttempt
        ),
        (
            "testNextServerIsTriedWhenTheFirstClosesAfterInfo",
            testNextServerIsTriedWhenTheFirstClosesAfterInfo
        ),
        (
            "testLineThatDoesNotParseAfterInfoFailsTheAttempt",
            testLineThatDoesNotParseAfterInfoFailsTheAttempt
        ),
        ("testConnectionClosedBeforeTlsIsATlsFailure", testConnectionClosedBeforeTlsIsATlsFailure),
        (
            "testCloseShortlyAfterInfoAndErrorIsATlsFailure",
            testCloseShortlyAfterInfoAndErrorIsATlsFailure
        ),
        (
            "testHandshakeFailureWhileReadingTheKeyFileIsATlsFailure",
            testHandshakeFailureWhileReadingTheKeyFileIsATlsFailure
        ),
        ("testLinesAfterInfoAreProcessedOnPlaintext", testLinesAfterInfoAreProcessedOnPlaintext),
        ("testFailedAttemptConnectionIsClosed", testFailedAttemptConnectionIsClosed),
        (
            "testAttemptEndedByPreviousConnectionClosingClosesItsConnection",
            testAttemptEndedByPreviousConnectionClosingClosesItsConnection
        ),
        (
            "testAttemptEndedByLineOnPreviousConnectionClosesItsConnection",
            testAttemptEndedByLineOnPreviousConnectionClosesItsConnection
        ),
        (
            "testNextServerIsUsedAfterTlsFirstAttemptFailsOnFirstLine",
            testNextServerIsUsedAfterTlsFirstAttemptFailsOnFirstLine
        ),
        (
            "testPublishDuringReconnectIsNotSentBeforeTls",
            testPublishDuringReconnectIsNotSentBeforeTls
        ),
        ("testLateHappyEyeballsWinnerIsUsed", testLateHappyEyeballsWinnerIsUsed),
        ("testResumeAfterFailedAttempts", testResumeAfterFailedAttempts),
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

    func testPingAfterInfoIsNotAnsweredBeforeTls() async throws {
        // The server must read no PONG for these PINGs, before TLS or inside it. The client
        // creates its write buffer only when it adds the TLS handler, so a PING handled before
        // that is not answered at once; the hold before TLS is checked by
        // testInfoAfterInfoIsNotAppliedBeforeTls and testMessageBeforeTlsIsNotDeliveredOnReconnect.
        let server = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true) + "PING\r\nPING\r\n",
                greetingDelay: 0.3,
                tls: try FakeNatsServer.tlsContext()))
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(server.port)")!)
            .rootCertificates(rootCA)
            .build()

        let connected = await result(within: 10) { try await client.connect() }
        let sent = try XCTUnwrap(server.connections.first?.firstBytes)
        XCTAssertEqual(
            sent.first, 0x16,
            "first bytes from the client are not a TLS record: \(String(decoding: sent, as: UTF8.self))"
        )
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        // The server sends no PING inside TLS, so a PONG it reads there, sent after the TLS
        // handler was added, answers a PING received before TLS.
        try await Task.sleep(nanoseconds: 500_000_000)
        XCTAssertFalse(
            server.lines.contains("PONG"), "PING received before TLS answered: \(server.lines)")
        try await client.close()
    }

    func testInfoAfterInfoIsNotAppliedBeforeTls() async throws {
        let other = try fake(.init(greeting: FakeNatsServer.info()))
        let otherInfo = FakeNatsServer.info(
            tlsRequired: true,
            connectUrls: ["nats://127.0.0.1:\(other.port)", "127.0.0.1:\(other.port)"])
        // sent twice, so that one is left over if the first is taken as the reply to CONNECT
        let seed = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true) + otherInfo + otherInfo,
                tls: try FakeNatsServer.tlsContext()))
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(seed.port)")!)
            .retainServersOrder()
            .rootCertificates(rootCA)
            .reconnectWait(0.1)
            .build()

        let connected = await result(within: 10) { try await client.connect() }
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        // with the seed gone, the client reconnects to every server it knows
        seed.shutdown()
        try await Task.sleep(nanoseconds: 2_000_000_000)
        XCTAssertEqual(
            other.connections.count, 0, "dialed a server from an INFO received before TLS")
        try? await client.close()
    }

    func testMessageBeforeTlsIsNotDeliveredOnReconnect() async throws {
        let first = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true),
                tls: try FakeNatsServer.tlsContext()))
        // sent twice, so that one is left over if the first is taken as the reply to CONNECT
        let message = "MSG foo 1 6\r\nbefore\r\n"
        let second = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true) + message + message,
                tls: try FakeNatsServer.tlsContext()))
        let client = NatsClientOptions()
            .urls([
                URL(string: "tls://localhost:\(first.port)")!,
                URL(string: "tls://localhost:\(second.port)")!,
            ])
            .retainServersOrder()
            .rootCertificates(rootCA)
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "foo")
        XCTAssertEqual(sub.sid, 1)

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        first.shutdown()
        await fulfillment(of: [reconnected], timeout: 10)
        XCTAssertGreaterThan(second.connections.count, 0, "second server was never dialed")

        let received = await result(within: 2) { try await sub.makeAsyncIterator().next() }
        if case .success(let msg?)? = received {
            XCTFail(
                "message received before TLS was delivered: "
                    + String(decoding: msg.payload ?? Data(), as: UTF8.self))
        }
        try? await client.close()
    }

    func testLinesOfThePreviousConnectionDoNotCompleteTheReconnect() async throws {
        let tls = try FakeNatsServer.tlsContext()
        let server = try FakeNatsServer(scripts: [
            // after the first PONG the client times out its PINGs and closes this connection;
            // that TLS close waits for the server
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true), tls: tls,
                stopsReadingAfterFirstPong: true),
            // the reconnect: nothing answers the PING after CONNECT
            .init(greeting: FakeNatsServer.info(tlsRequired: true), tls: tls, firstPingReply: ""),
        ])
        servers.append(server)
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(server.port)")!)
            .rootCertificates(rootCA)
            .pingInterval(0.25)
            .build()
        try await client.connect()
        let early = XCTestExpectation(
            description: "line of the previous connection completed CONNECT")
        early.isInverted = true
        let reconnected = XCTestExpectation(description: "client did not reconnect")
        early.assertForOverFulfill = false
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in
            early.fulfill()
            reconnected.fulfill()
        }

        let attemptSentPing = {
            server.connections.dropFirst().first?.lines.contains("PING") == true
        }
        for _ in 0..<100 where !attemptSentPing() {
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        XCTAssertTrue(attemptSentPing(), "setup: no reconnect attempt")
        // read inside TLS on the previous connection while its close waits
        server.write("PONG\r\n", on: 0)
        // then a read on the reconnect's connection, with which that line must not be parsed
        try await Task.sleep(nanoseconds: 200_000_000)
        server.write("PO", on: 1)
        await fulfillment(of: [early], timeout: 1)

        // the reconnect's own server answers CONNECT; the other two PONGs answer the PINGs that
        // the previous connection left unanswered
        server.write("NG\r\nPONG\r\nPONG\r\n", on: 1)
        await fulfillment(of: [reconnected], timeout: 5)
        try await client.close()
        // lets the TLS close of the previous connection complete
        server.closeConnection(0)
    }

    func testLaterReadsBeforeTlsAreDropped() async throws {
        // On the reconnect the server writes INFO alone, then a MSG for the live subscription
        // every 50 µs below its TLS handler, in separate writes, until the connection closes.
        // The client's CA file holds 400 copies of the CA, so that building its TLS context takes
        // long enough for some of those writes to arrive in reads before the client adds TLS.
        // Once TLS is added, the same bytes break the handshake; the attempt after that gets
        // none of them.
        let server = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true),
                tls: try FakeNatsServer.tlsContext(),
                spray: (text: "MSG foo 1 6\r\nbefore\r\n", interval: 0.000_05, count: .max),
                sprayOnConnection: 1))
        let bundle = FileManager.default.temporaryDirectory
            .appendingPathComponent("ca-bundle-\(UUID().uuidString).pem")
        let ca = try String(contentsOf: rootCA, encoding: .utf8)
        try String(repeating: ca, count: 400).write(to: bundle, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(at: bundle) }

        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(server.port)")!)
            .rootCertificates(bundle)
            .reconnectWait(0.1)
            .build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "foo")
        XCTAssertEqual(sub.sid, 1)
        for _ in 0..<60 where !server.lines.contains(where: { $0.hasPrefix("SUB foo") }) {
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        XCTAssertTrue(server.lines.contains { $0.hasPrefix("SUB foo") }, "setup: SUB not sent")

        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }
        server.closeConnection(0)
        await fulfillment(of: [reconnected], timeout: 20)
        XCTAssertGreaterThan(
            server.connections.count, 2, "setup: the attempt with the extra writes did not fail")

        let received = await result(within: 3) { try await sub.makeAsyncIterator().next() }
        if case .success(let msg?)? = received {
            XCTFail(
                "message from a later read before TLS was delivered: "
                    + String(decoding: msg.payload ?? Data(), as: UTF8.self))
        }
        try? await client.close()
    }

    func testPartialLineBeforeTlsIsNotJoinedToTlsData() async throws {
        let server = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true) + "-ERR 'Authorization Vio",
                tls: try FakeNatsServer.tlsContext()))
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(server.port)")!)
            .rootCertificates(rootCA)
            .build()

        let connected = await result(within: 10) {
            try await client.connect()
            _ = try await client.rtt()
        }
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        try await client.close()
    }

    func testServersFromInfoBeforeFailedTlsAreNotDialed() async throws {
        let other = try fake(.init(greeting: FakeNatsServer.info()))
        let otherInfo = FakeNatsServer.info(
            tlsRequired: true,
            connectUrls: ["nats://127.0.0.1:\(other.port)", "127.0.0.1:\(other.port)"])
        // the certificate is issued by a CA the client does not trust, so every handshake fails
        let seed = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true) + otherInfo + otherInfo,
                tls: try FakeNatsServer.tlsContext(cert: "ip-cert", key: "ip-key")))
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(seed.port)")!)
            .retainServersOrder()
            .rootCertificates(rootCA)
            .reconnectWait(0.1)
            .retryOnfailedConnect()
            .build()

        try await client.connect()
        try await Task.sleep(nanoseconds: 2_000_000_000)
        try? await client.close()
        XCTAssertGreaterThan(seed.connections.count, 1, "setup: the client did not retry")
        XCTAssertEqual(
            other.connections.count, 0,
            "dialed a server from an INFO received before a TLS handshake that failed")
    }

    func testServerThatClosesAfterInfoFailsTheAttempt() async throws {
        // a full server writes INFO and an error, then closes the connection
        let greetings = [
            ("INFO", FakeNatsServer.info()),
            ("INFO and -ERR", FakeNatsServer.info() + "-ERR 'maximum connections exceeded'\r\n"),
        ]
        for (label, greeting) in greetings {
            let server = try fake(.init(greeting: greeting, closeAfterGreeting: true))
            for attempt in 0..<10 {
                let client = NatsClientOptions()
                    .url(URL(string: "nats://127.0.0.1:\(server.port)")!)
                    .build()

                let connected = await result(within: 5) { try await client.connect() }
                guard case .failure? = connected else {
                    XCTFail(
                        "\(label), attempt \(attempt): connect must fail, got "
                            + String(describing: connected))
                    try? await client.close()
                    continue
                }
            }
        }
    }

    func testNextServerIsTriedWhenTheFirstClosesAfterInfo() async throws {
        let closing = try fake(.init(greeting: FakeNatsServer.info(), closeAfterGreeting: true))
        let next = try fake(.init(greeting: FakeNatsServer.info()))
        let client = NatsClientOptions()
            .urls([
                URL(string: "nats://127.0.0.1:\(closing.port)")!,
                URL(string: "nats://127.0.0.1:\(next.port)")!,
            ])
            .retainServersOrder()
            .build()

        let connected = await result(within: 10) { try await client.connect() }
        guard case .success? = connected else {
            try? await client.close()
            return XCTFail("next server not tried: \(String(describing: connected))")
        }
        XCTAssertEqual(closing.connections.count, 1)
        XCTAssertEqual(next.connections.count, 1)
        try await client.close()
    }

    func testLineThatDoesNotParseAfterInfoFailsTheAttempt() async throws {
        // the server keeps the connection open
        let server = try fake(.init(greeting: FakeNatsServer.info() + "XYZ\r\n"))
        let client = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(server.port)")!)
            .build()

        let connected = await result(within: 5) { try await client.connect() }
        guard case .failure(let error)? = connected else {
            try? await client.close()
            return XCTFail("connect must fail, got \(String(describing: connected))")
        }
        guard case NatsError.ConnectError.io = error else {
            return XCTFail("not reported as an I/O error: \(error)")
        }
    }

    func testConnectionClosedBeforeTlsIsATlsFailure() async throws {
        // a full server writes INFO and an error in plaintext, then closes the connection
        let server = try fake(
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true)
                    + "-ERR 'maximum connections exceeded'\r\n",
                closeAfterGreeting: true))
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(server.port)")!)
            .rootCertificates(rootCA)
            .build()

        let connected = await result(within: 5) { try await client.connect() }
        guard case .failure(let error)? = connected else {
            try? await client.close()
            return XCTFail("connect must fail, got \(String(describing: connected))")
        }
        guard case NatsError.ConnectError.tlsFailure = error else {
            return XCTFail("not reported as a TLS failure: \(error)")
        }
    }

    func testCloseShortlyAfterInfoAndErrorIsATlsFailure() async throws {
        // the close is processed before, while or after the client adds its TLS handler,
        // depending on the delay; which delays hit which case depends on the machine
        let greeting =
            FakeNatsServer.info(tlsRequired: true) + "-ERR 'maximum connections exceeded'\r\n"
        for micros in stride(from: 0, through: 1000, by: 25) {
            let server = try fake(
                .init(
                    greeting: greeting, closeAfterGreeting: true,
                    closeDelay: TimeInterval(micros) / 1_000_000))
            for attempt in 0..<2 {
                let client = NatsClientOptions()
                    .url(URL(string: "tls://localhost:\(server.port)")!)
                    .rootCertificates(rootCA)
                    .build()

                let connected = await result(within: 5) { try await client.connect() }
                guard case .failure(let error)? = connected else {
                    XCTFail(
                        "close after \(micros) us, attempt \(attempt): connect must fail, got "
                            + String(describing: connected))
                    try? await client.close()
                    continue
                }
                guard case NatsError.ConnectError.tlsFailure = error else {
                    XCTFail(
                        "close after \(micros) us, attempt \(attempt): not reported as a TLS "
                            + "failure: \(error)")
                    continue
                }
            }
            server.shutdown()
        }
    }

    func testHandshakeFailureWhileReadingTheKeyFileIsATlsFailure() async throws {
        // plaintext below the server's TLS from right after INFO on breaks the handshake, which
        // then fails while the client reads its key file
        let nkey = Bundle.module.url(forResource: "nkey", withExtension: "")!
        let creds = Bundle.module.url(forResource: "TestUser", withExtension: "creds")!
        let keyFiles: [(String, (NatsClientOptions) -> NatsClientOptions)] = [
            ("nkey file", { $0.nkeyFile(nkey) }),
            ("credentials file", { $0.credentialsFile(creds) }),
        ]
        for (label, withKeyFile) in keyFiles {
            let server = try fake(
                .init(
                    greeting: FakeNatsServer.info(tlsRequired: true, nonce: "PJnVGfQrFXwVEmAb"),
                    tls: try FakeNatsServer.tlsContext(),
                    spray: (
                        text: "-ERR 'maximum connections exceeded'\r\n", interval: 0.000_05,
                        count: .max
                    )))
            let options = NatsClientOptions()
                .url(URL(string: "tls://localhost:\(server.port)")!)
                .rootCertificates(rootCA)
            let client = withKeyFile(options).build()

            let connected = await result(within: 5) { try await client.connect() }
            guard case .failure(let error)? = connected else {
                XCTFail("\(label): connect must fail, got \(String(describing: connected))")
                try? await client.close()
                continue
            }
            guard case NatsError.ConnectError.tlsFailure = error else {
                XCTFail("\(label): not reported as a TLS failure: \(error)")
                continue
            }
        }
    }

    func testLinesAfterInfoAreProcessedOnPlaintext() async throws {
        let server = try fake(.init(greeting: FakeNatsServer.info() + "PING\r\n"))
        let client = NatsClientOptions()
            .url(URL(string: "nats://127.0.0.1:\(server.port)")!)
            .build()

        let connected = await result(within: 10) { try await client.connect() }
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        for _ in 0..<40 where !server.lines.contains("PONG") {
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        XCTAssertTrue(
            server.lines.contains("PONG"), "PING after INFO not answered: \(server.lines)")
        try await client.close()
    }

    func testFailedAttemptConnectionIsClosed() async throws {
        // answers the client's first PING with an error and leaves the connection open
        let refusing = try fake(
            .init(
                greeting: FakeNatsServer.info(),
                firstPingReply: "-ERR 'Authorization Violation'\r\n"))
        let server = try fake(.init(greeting: FakeNatsServer.info()))
        let client = NatsClientOptions()
            .urls([
                URL(string: "nats://127.0.0.1:\(refusing.port)")!,
                URL(string: "nats://127.0.0.1:\(server.port)")!,
            ])
            .retainServersOrder()
            .build()
        let disconnected = XCTestExpectation(description: "client was disconnected")
        disconnected.isInverted = true
        client.on(.disconnected) { _ in disconnected.fulfill() }

        let connected = await result(within: 10) { try await client.connect() }
        guard case .success? = connected else {
            return XCTFail("connect: \(String(describing: connected))")
        }
        let isClosed = try await closed(0, on: refusing)
        XCTAssertTrue(isClosed, "connection of the failed attempt left open")

        // the end of that connection must not touch the connection the client uses
        refusing.shutdown()
        await fulfillment(of: [disconnected], timeout: 0.5)
        XCTAssertEqual(server.connections.count, 1)
        XCTAssertEqual(server.connections.first?.closed, false, "connection in use was closed")
        try await client.close()
    }

    func testAttemptEndedByPreviousConnectionClosingClosesItsConnection() async throws {
        try await checkAttemptEndedThroughPreviousConnection { server in
            server.closeConnection(0)
        }
    }

    func testAttemptEndedByLineOnPreviousConnectionClosesItsConnection() async throws {
        try await checkAttemptEndedThroughPreviousConnection { server in
            // read inside TLS while the client closes that connection
            server.write("PING\r\n", on: 0)
            // then let that close complete, well before the next attempt starts
            try await Task.sleep(nanoseconds: 200_000_000)
            server.closeConnection(0)
        }
    }

    /// The client uses TLS after INFO on connection 0, stops getting PONGs there and closes it;
    /// that TLS close waits for the server. Connection 1, the reconnect attempt, gets no INFO, and
    /// `endAttempt` ends the attempt through connection 0. The client must close connection 1,
    /// and what is written on connection 1 once the client has reconnected must not be acted on.
    private func checkAttemptEndedThroughPreviousConnection(
        _ endAttempt: (FakeNatsServer) async throws -> Void
    ) async throws {
        let other = try fake(.init(greeting: FakeNatsServer.info()))
        let tls = try FakeNatsServer.tlsContext()
        let server = try FakeNatsServer(scripts: [
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true), tls: tls,
                stopsReadingAfterFirstPong: true),
            .init(),
            // a reconnect keeps the count of unanswered PINGs: the second PONG resets it
            .init(
                greeting: FakeNatsServer.info(tlsRequired: true), tls: tls,
                firstPingReply: "PONG\r\nPONG\r\n"),
        ])
        servers.append(server)
        let client = NatsClientOptions()
            .url(URL(string: "tls://localhost:\(server.port)")!)
            .rootCertificates(rootCA)
            .pingInterval(0.25)
            .reconnectWait(0.5)
            .build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "foo")
        let reconnected = XCTestExpectation(description: "client did not reconnect")
        reconnected.assertForOverFulfill = false
        client.on(.connected) { _ in reconnected.fulfill() }

        for _ in 0..<100 where server.connections.count < 2 {
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        XCTAssertEqual(server.connections.count, 2, "setup: no reconnect attempt")
        try await endAttempt(server)
        await fulfillment(of: [reconnected], timeout: 10)

        let isClosed = try await closed(1, on: server)
        XCTAssertTrue(isClosed, "connection of the failed attempt left open")

        // the server as host:port and with a scheme, so that either form of entry counts
        let urls = ["127.0.0.1:\(other.port)", "nats://127.0.0.1:\(other.port)"]
        server.write(FakeNatsServer.info(connectUrls: urls) + "MSG foo 1 5\r\nlater\r\n", on: 1)
        let received = await result(within: 1) { try await sub.makeAsyncIterator().next() }
        if case .success(let msg?)? = received {
            XCTFail(
                "message read on the connection of the failed attempt was delivered: "
                    + String(decoding: msg.payload ?? Data(), as: UTF8.self))
        }
        // once the server is gone the client tries the servers it knows
        server.shutdown()
        try await Task.sleep(nanoseconds: 1_000_000_000)
        XCTAssertEqual(
            other.connections.count, 0,
            "dialed a server named on the connection of the failed attempt")
        try? await client.close()
    }

    // With TLS first, the close of the failed attempt's connection takes a TLS close; the next
    // server is tried only once it is done.
    func testNextServerIsUsedAfterTlsFirstAttemptFailsOnFirstLine() async throws {
        let tls = try FakeNatsServer.tlsContext()
        let firstLines = [
            ("PING", "PING\r\n" + FakeNatsServer.info(tlsRequired: true)),
            ("INFO that does not decode", "INFO {\"server_id\":\r\n"),
        ]
        for (label, first) in firstLines {
            let failing = try fake(.init(greeting: first, tls: tls, tlsFirst: true))
            let next = try fake(
                .init(greeting: FakeNatsServer.info(tlsRequired: true), tls: tls, tlsFirst: true))
            let client = NatsClientOptions()
                .urls([
                    URL(string: "tls://localhost:\(failing.port)")!,
                    URL(string: "tls://localhost:\(next.port)")!,
                ])
                .retainServersOrder()
                .requireTls()
                .withTlsFirst()
                .rootCertificates(rootCA)
                .build()

            let connected = await result(within: 10) { try await client.connect() }
            guard case .success? = connected else {
                XCTFail("\(label): next server not used: \(String(describing: connected))")
                try? await client.close()
                continue
            }
            XCTAssertEqual(next.connections.count, 1, label)
            try await client.close()
        }
    }

    func testPublishDuringReconnectIsNotSentBeforeTls() async throws {
        for requireTls in [false, true] {
            // INFO comes 1 s after accept, so on the reconnect the client waits for it
            let server = try fake(
                .init(
                    greeting: FakeNatsServer.info(tlsRequired: true), greetingDelay: 1,
                    tls: try FakeNatsServer.tlsContext()))
            let options = NatsClientOptions()
                .url(URL(string: "tls://localhost:\(server.port)")!)
                .rootCertificates(rootCA)
                .reconnectWait(0.1)
            if requireTls {
                _ = options.requireTls()
            }
            let client = options.build()
            try await client.connect()
            let reconnected = XCTestExpectation(description: "client did not reconnect")
            reconnected.assertForOverFulfill = false
            client.on(.connected) { _ in reconnected.fulfill() }

            server.closeConnection(0)
            // publish while the client reconnects, waits for INFO and starts TLS
            for _ in 0..<75 {
                try? await client.publish(Data("payload".utf8), subject: "foo")
                try await Task.sleep(nanoseconds: 20_000_000)
            }
            await fulfillment(of: [reconnected], timeout: 10)

            let later = server.connections.dropFirst().filter { !$0.firstBytes.isEmpty }
            XCTAssertFalse(later.isEmpty, "requireTls \(requireTls): setup: no reconnect")
            for connection in later where connection.firstBytes.first != 0x16 {
                XCTFail(
                    "requireTls \(requireTls): first bytes on the reconnect are not a TLS record: "
                        + String(decoding: connection.firstBytes, as: UTF8.self))
            }
            try await client.close()
        }
    }

    // NIO dials every address of a host (Happy Eyeballs) through the same channel initializer.
    // Here the ::1 connect completes only after the 127.0.0.1 attempt was started and refused;
    // the channel that wins must be the one the client uses.
    func testLateHappyEyeballsWinnerIsUsed() async throws {
        let server = try FakeNatsServer(
            .init(greeting: FakeNatsServer.info()), host: "::1", accepting: false)
        servers.append(server)
        let dummies = server.fillBacklog()
        defer {
            for dummy in dummies {
                dummy.close(promise: nil)
            }
        }
        XCTAssertFalse(dummies.isEmpty, "setup: could not fill the accept queue")
        // The client's connect to ::1 must stay pending while the queue is full. Some systems
        // reset a connect to a full queue instead; this case cannot be built there.
        do {
            let extra = try await ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
                .connectTimeout(.milliseconds(300))
                .connect(host: "::1", port: server.port).get()
            extra.close(promise: nil)
            throw XCTSkip("a connect to the full accept queue completed")
        } catch ChannelError.connectTimeout(_) {
            // pending, as needed
        } catch let skip as XCTSkip {
            throw skip
        } catch {
            throw XCTSkip("a connect to the full accept queue did not stay pending: \(error)")
        }

        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(server.port)")!)
            .build()
        async let connected = result(within: 15) { try await client.connect() }
        // later than NIO's 250 ms delay before it tries the next address
        try await Task.sleep(nanoseconds: 700_000_000)
        server.startAccepting()
        let outcome = await connected
        guard case .success? = outcome else {
            return XCTFail(
                "the connection that won on the first address was not used: "
                    + "\(String(describing: outcome)), accepted \(server.connections.count)")
        }
        try await client.close()
    }

    // suspend(), resume() and close() work while the client reconnects to a server that is
    // gone: they need the channel of the last connection, which failed attempts leave in place.
    func testResumeAfterFailedAttempts() async throws {
        for round in 0..<8 {
            let server = try fake(.init(greeting: FakeNatsServer.info()))
            let client = NatsClientOptions()
                .url(URL(string: "nats://127.0.0.1:\(server.port)")!)
                .reconnectWait(0.05)
                .build()
            try await client.connect()
            server.shutdown()
            // lands in an attempt in some rounds and in the wait between attempts in others
            try await Task.sleep(nanoseconds: UInt64(300_000_000 + 13_000_000 * round))

            do {
                try await client.suspend()
            } catch is CancellationError {
                // suspend() rethrows how the reconnect task it cancelled ended when that task
                // was sleeping between rounds; this test is not about that
                try await client.close()
                continue
            } catch {
                XCTFail("round \(round): suspend: \(error)")
            }
            do {
                try await client.resume()
                try await client.close()
            } catch {
                XCTFail("round \(round): \(error)")
            }
        }
    }
}
