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

import XCTest

@testable import Nats

class ErrorsTests: XCTestCase {

    static var allTests = [
        ("testServerErrorPermissionsDenied", testServerErrorPermissionsDenied),
        ("testServerTextIsEscapedAndCut", testServerTextIsEscapedAndCut),
        ("testErrorsShowServerTextEscaped", testErrorsShowServerTextEscaped),
    ]

    func testServerErrorPermissionsDenied() {
        var err = NatsError.ServerError(
            "Permissions Violation for Subscription to \"events.A.B.*\"]")
        XCTAssertEqual(
            err, NatsError.ServerError.permissionsViolation(.subscribe, "events.A.B.*", nil))

        err = NatsError.ServerError("Permissions Violation for Publish to \"events.A.B.*\"")
        XCTAssertEqual(
            err, NatsError.ServerError.permissionsViolation(.publish, "events.A.B.*", nil))

        err = NatsError.ServerError(
            "Permissions Violation for Publish to \"events.A.B.*\" using queue \"q\"")
        XCTAssertEqual(
            err, NatsError.ServerError.permissionsViolation(.publish, "events.A.B.*", "q"))

        err = NatsError.ServerError("Some other error")
        XCTAssertEqual(err, NatsError.ServerError.proto("Some other error"))
    }

    func testServerTextIsEscapedAndCut() {
        XCTAssertEqual(
            NatsError.escaped("a\"b\\\r\n\t\u{1B}[2J\u{9B}\u{7F}\u{0}é"),
            #"a"b\\r\n\t\u{1B}[2J\u{009B}\u{7F}\0é"#)
        XCTAssertEqual(
            NatsError.escaped(String(repeating: "x", count: 300)),
            String(repeating: "x", count: 256))
        // the cut counts the escaped bytes and does not split an escape
        XCTAssertEqual(
            NatsError.escaped(String(repeating: "\u{1B}", count: 300)),
            String(repeating: #"\u{1B}"#, count: 42))
        // one character of 1 + 2 * 1000 bytes is cut at a whole scalar
        let character = "e" + String(repeating: "\u{301}", count: 1000)
        XCTAssertEqual(NatsError.escaped(character).utf8.count, 255)
    }

    func testErrorsShowServerTextEscaped() {
        XCTAssertThrowsError(try ServerOp.parse(from: Data("FOO \u{1B}[2J".utf8))) { error in
            XCTAssertEqual("\(error)", #"nats: unknown server operation: FOO \u{1B}[2J"#)
            XCTAssertEqual(
                error as? NatsError.ProtocolError, .invalidOperation("FOO \u{1B}[2J"))
        }
        XCTAssertThrowsError(try ServerOp.parse(from: Data("PUB \u{9B}2J".utf8))) { error in
            XCTAssertEqual(
                "\(error)", #"nats: unknown server operation: unknown server op: PUB \u{009B}2J"#)
        }
        XCTAssertThrowsError(try ServerOp.parse(from: Data([0x1B, 0x63]))) { error in
            XCTAssertEqual(
                "\(error)", #"nats: parser failure: unable to parse inbound message: \u{1B}c"#)
        }
        XCTAssertThrowsError(try ServerOp.parse(from: Data("INFO {\"a\":\u{1B}[2J}".utf8))) {
            error in
            guard case .parserFailure = error as? NatsError.ProtocolError else {
                return XCTFail("not a parser failure")
            }
            XCTAssertFalse(
                "\(error)".unicodeScalars.contains { $0.properties.generalCategory == .control })
        }
        XCTAssertEqual(
            NatsError.ServerError(" 'x\u{1B}[2J'").description, #"nats:  'x\u{1B}[2J'"#)
        XCTAssertEqual(
            NatsError.ServerError("Permissions Violation for Publish to \"a\u{1B}[2J\"")
                .description,
            #"nats: permissions violation for operation "publish" on subject "a\u{1B}[2J""#)
        // the server quotes the subject and the queue as Go's %q does; that text is kept
        XCTAssertEqual(
            NatsError.ServerError(
                #"Permissions Violation for Subscription to "a\\b" using queue "q\"1""#
            ).description,
            #"nats: permissions violation for operation "subscribe" on subject "a\\b" using queue "q\"1""#
        )
        XCTAssertEqual(
            NatsError.ServerError(
                "Permissions Violation for Subscription to \"a\" using queue \"q\u{1B}[2J\""
            ).description,
            #"nats: permissions violation for operation "subscribe" on subject "a" using queue "q\u{1B}[2J""#
        )
    }
}
