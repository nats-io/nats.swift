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

import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOSSL
import NIOTLS
import XCTest

@testable import Nats

class TlsNameCheckTests: XCTestCase {

    static var allTests = [
        ("testDnsNameMatching", testDnsNameMatching),
        ("testCertificateIsIssuedForName", testCertificateIsIssuedForName),
        ("testMismatchNamesExpectedAndPresentedNames", testMismatchNamesExpectedAndPresentedNames),
        ("testWritesAreHeldUntilTheNameCheck", testWritesAreHeldUntilTheNameCheck),
        ("testNothingIsReadUnlessTheNameCheckPasses", testNothingIsReadUnlessTheNameCheckPasses),
    ]

    func testDnsNameMatching() {
        let matching = [
            ("localhost", "localhost"),
            ("LocalHost", "localhost"),
            ("localhost", "LOCALHOST"),
            ("localhost.", "localhost"),
            ("localhost", "localhost."),
            ("*.example.com", "nats.example.com"),
            ("*.Example.com", "NATS.example.COM"),
        ]
        for (pattern, host) in matching {
            XCTAssertTrue(
                TlsNameCheckHandler.dnsName(Array(pattern.utf8), matches: host),
                "\(pattern) \(host)")
        }
        let notMatching = [
            ("localhost", "localhost2"),
            ("localhost", "nats.localhost"),
            ("*.example.com", "example.com"),
            ("*.example.com", "a.b.example.com"),
            ("*.example.com", ".example.com"),
            ("n*.example.com", "nats.example.com"),
            ("nats.*.com", "nats.example.com"),
            // a wildcard needs more labels after it
            ("*", "localhost"),
            ("*.", "localhost"),
            ("", "localhost"),
            ("localhost", ""),
            // the Kelvin sign is not an ASCII K
            ("\u{212A}.example.com", "k.example.com"),
        ]
        for (pattern, host) in notMatching {
            XCTAssertFalse(
                TlsNameCheckHandler.dnsName(Array(pattern.utf8), matches: host),
                "\(pattern) \(host)")
        }
    }

    func testCertificateIsIssuedForName() throws {
        // DNS localhost, IP ::1
        let server = try certificate("server-cert")
        XCTAssertEqual(TlsNameCheckHandler.names(of: server), ["localhost", "::1"])
        XCTAssertTrue(TlsNameCheckHandler.certificate(server, isIssuedFor: "localhost"))
        XCTAssertTrue(TlsNameCheckHandler.certificate(server, isIssuedFor: "::1"))
        XCTAssertTrue(TlsNameCheckHandler.certificate(server, isIssuedFor: "0:0:0:0:0:0:0:1"))
        XCTAssertFalse(TlsNameCheckHandler.certificate(server, isIssuedFor: "127.0.0.1"))
        XCTAssertFalse(TlsNameCheckHandler.certificate(server, isIssuedFor: "nats.example.com"))

        // IP 127.0.0.1 and ::1 only
        let ipOnly = try certificate("ip-only-cert")
        XCTAssertEqual(TlsNameCheckHandler.names(of: ipOnly), ["127.0.0.1", "::1"])
        XCTAssertTrue(TlsNameCheckHandler.certificate(ipOnly, isIssuedFor: "127.0.0.1"))
        XCTAssertTrue(TlsNameCheckHandler.certificate(ipOnly, isIssuedFor: "::1"))
        XCTAssertFalse(TlsNameCheckHandler.certificate(ipOnly, isIssuedFor: "localhost"))
        XCTAssertFalse(TlsNameCheckHandler.certificate(ipOnly, isIssuedFor: "127.0.0.2"))
    }

    func testMismatchNamesExpectedAndPresentedNames() {
        let server = "\"tls://localhost:4222\""
        XCTAssertEqual(
            TlsNameCheckHandler.mismatch(
                server: server, name: "localhost", names: ["127.0.0.1", "::1"]
            ).description,
            "the certificate of \"tls://localhost:4222\" is not valid for \"localhost\": "
                + "it is issued for \"127.0.0.1\", \"::1\"")
        XCTAssertEqual(
            TlsNameCheckHandler.mismatch(server: server, name: "localhost", names: []).description,
            "the certificate of \"tls://localhost:4222\" is not valid for \"localhost\": "
                + "it has no DNS name or IP address entry, and the common name of its subject is "
                + "not checked")
        XCTAssertEqual(
            TlsNameCheckHandler.mismatch(
                server: server, name: "localhost", names: ["a", "b", "c", "d", "e", "f"]
            ).description,
            "the certificate of \"tls://localhost:4222\" is not valid for \"localhost\": "
                + "it is issued for \"a\", \"b\", \"c\", \"d\" and 2 more")
    }

    func testWritesAreHeldUntilTheNameCheck() throws {
        // without a TLS handler there is no certificate, so the check fails
        let channel = EmbeddedChannel(
            handler: TlsNameCheckHandler(server: "\"tls://localhost:4222\"", name: "localhost"))
        let writeResult = NIOLockedValueBox<Result<Void, Error>?>(nil)
        channel.writeAndFlush(ByteBuffer(string: "CONNECT {}\r\n")).whenComplete { result in
            writeResult.withLockedValue { $0 = result }
        }
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self), "sent before the check")

        channel.pipeline.fireUserInboundEventTriggered(
            TLSUserEvent.handshakeCompleted(negotiatedProtocol: nil))
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self), "sent after a failed check")
        // observed, not waited for: a write passed on without a flush never completes here
        let written = writeResult.withLockedValue { $0 }
        guard case .failure(let error)? = written else {
            return XCTFail("the held write did not fail: \(String(describing: written))")
        }
        XCTAssertTrue(error is NatsError.CertificateNameMismatch, "\(error)")
        XCTAssertThrowsError(try channel.throwIfErrorCaught()) { error in
            XCTAssertTrue(error is NatsError.CertificateNameMismatch, "\(error)")
        }
    }

    func testNothingIsReadUnlessTheNameCheckPasses() throws {
        // without a TLS handler there is no certificate, so the check fails
        let channel = EmbeddedChannel(
            handler: TlsNameCheckHandler(server: "\"tls://localhost:4222\"", name: "localhost"))
        try channel.writeInbound(ByteBuffer(string: "INFO {}\r\n"))
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self), "read before the check")

        channel.pipeline.fireUserInboundEventTriggered(
            TLSUserEvent.handshakeCompleted(negotiatedProtocol: nil))
        // a read that reaches the handler after a failed check is dropped
        channel.pipeline.syncOperations.fireChannelRead(NIOAny(ByteBuffer(string: "PONG\r\n")))
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self), "read after a failed check")
    }

    private func certificate(_ name: String) throws -> NIOSSLCertificate {
        let url = Bundle.module.url(forResource: name, withExtension: "pem")!
        return try NIOSSLCertificate.fromPEMFile(url.path)[0]
    }
}
