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

class ParserTests: XCTestCase {

    static var allTests = [
        ("testParseOutMessages", testParseOutMessages)
    ]

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
    }

    func testParseOutMessages() {
        struct TestCase {
            let name: String
            let givenChunks: [String]
            let expectedOps: [ServerOp]
        }

        let fail: ((Int, String) -> String) = { index, name in
            return "Test case: \(index)\n Input: \(name)"
        }
        var hm = NatsHeaderMap()
        hm.append(try! NatsHeaderName("h1"), NatsHeaderValue("X"))
        hm.append(try! NatsHeaderName("h1"), NatsHeaderValue("Y"))
        hm.append(try! NatsHeaderName("h2"), NatsHeaderValue("Z"))

        let testCases = [
            TestCase(
                name: "Single chunk, different operations",
                givenChunks: ["MSG foo 1 5\r\nhello\r\n+OK\r\nPONG\r\n"],
                expectedOps: [
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)
                    ),
                    .ok,
                    .pong,
                ]
            ),
            TestCase(
                name: "Chunked messages",
                givenChunks: [
                    "MSG foo 1 5\r\nhello\r\nMSG foo 1 5\r\nwo",
                    "rld\r\nMSG f",
                    "oo 1 5\r\nhello\r\nMSG foo 1 5\r\nworld",
                    "\r\nMSG foo 1 5\r\nhello\r",
                    "\nMSG foo 1 5\r\nworld\r\nMSG foo 1 5\r\n",
                    "hello\r\n",
                ],
                expectedOps: [
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)
                    ),
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "world".data(using: .utf8)!, length: 5)
                    ),
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)
                    ),
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "world".data(using: .utf8)!, length: 5)
                    ),
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)
                    ),
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "world".data(using: .utf8)!, length: 5)
                    ),
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)
                    ),
                ]
            ),
            TestCase(
                name: "Message with headers only",
                givenChunks: [
                    "HMSG foo 1 30 30\r\nNATS/1.0\r\nh1:X\r\nh1:Y\r\nh2:Z\r\n\r\n\r\n"
                ],
                expectedOps: [
                    .hMessage(
                        HMessageInbound(
                            subject: "foo", sid: 1, payload: nil, headers: hm, headersLength: 30,
                            length: 30)
                    )
                ]
            ),
            TestCase(
                name: "Message with headers and payload",
                givenChunks: [
                    "HMSG foo 1 30 35\r\nNATS/1.0\r\nh1:X\r\nh1:Y\r\nh2:Z\r\n\r\nhello\r\n"
                ],
                expectedOps: [
                    .hMessage(
                        HMessageInbound(
                            subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!,
                            headers: hm, headersLength: 30, length: 35)
                    )
                ]
            ),
            TestCase(
                name: "Message with status and no other headers",
                givenChunks: [
                    "HMSG foo 1 30 30\r\nNATS/1.0 503 no responders\r\n\r\n\r\n"
                ],
                expectedOps: [
                    .hMessage(
                        HMessageInbound(
                            subject: "foo", sid: 1, payload: nil, headers: NatsHeaderMap(),
                            headersLength: 30, length: 30, status: StatusCode.noResponders,
                            description: "no responders"
                        )
                    )
                ]
            ),
            TestCase(
                name: "Message with status, headers and payload",
                givenChunks: [
                    "HMSG foo 1 48 53\r\nNATS/1.0 503 no responders\r\nh1:X\r\nh1:Y\r\nh2:Z\r\n\r\nhello\r\n"
                ],
                expectedOps: [
                    .hMessage(
                        HMessageInbound(
                            subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!,
                            headers: hm, headersLength: 48, length: 53,
                            status: StatusCode.noResponders,
                            description: "no responders")
                    )
                ]
            ),
            TestCase(
                name: "With empty lines",
                givenChunks: [
                    "MSG foo 1 5\r\nhello\r\nMSG foo 1 5\r\nwo",
                    "",
                    "",
                    "rld\r\nMSG f",
                    "oo 1 5\r\nhello\r\n",
                ],
                expectedOps: [
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)
                    ),
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "world".data(using: .utf8)!, length: 5)
                    ),
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)
                    ),
                ]
            ),
            TestCase(
                name: "With crlf in payload",
                givenChunks: [
                    "MSG foo 1 7\r\nhe\r\nllo\r\n"
                ],
                expectedOps: [
                    .message(
                        MessageInbound(
                            subject: "foo", sid: 1, payload: Data("he\r\nllo".utf8), length: 7))
                ]
            ),
        ]

        for (tn, tc) in testCases.enumerated() {
            logger.logLevel = .critical
            var ops = [ServerOp]()
            var prevRemainder: Data?
            for chunk in tc.givenChunks {
                var chunkData = Data(chunk.utf8)
                if let prevRemainder {
                    chunkData.prepend(prevRemainder)
                }
                let res = try! chunkData.parseOutMessages()
                prevRemainder = res.remainder
                ops.append(contentsOf: res.ops)
            }
            XCTAssertEqual(ops.count, tc.expectedOps.count)
            for (i, op) in ops.enumerated() {
                switch op {
                case .ok:
                    if case .ok = tc.expectedOps[i] {
                    } else {
                        XCTFail(fail(tn, tc.name))
                    }
                case .info(let info):
                    if case .info(let expectedInfo) = tc.expectedOps[i] {
                        XCTAssertEqual(info, expectedInfo, fail(tn, tc.name))
                    } else {
                        XCTFail(fail(tn, tc.name))
                    }

                case .ping:
                    if case .ping = tc.expectedOps[i] {
                    } else {
                        XCTFail(fail(tn, tc.name))
                    }
                case .pong:
                    if case .pong = tc.expectedOps[i] {
                    } else {
                        XCTFail(fail(tn, tc.name))
                    }
                case .error(_):
                    if case .error(_) = tc.expectedOps[i] {
                    } else {
                        XCTFail(fail(tn, tc.name))
                    }
                case .message(let msg):
                    if case .message(let expectedMessage) = tc.expectedOps[i] {
                        XCTAssertEqual(msg, expectedMessage, fail(tn, tc.name))
                    } else {
                        XCTFail(fail(tn, tc.name))
                    }
                case .hMessage(let msg):
                    if case .hMessage(let expectedMessage) = tc.expectedOps[i] {
                        XCTAssertEqual(msg, expectedMessage, fail(tn, tc.name))
                    } else {
                        XCTFail(fail(tn, tc.name))
                    }
                }
            }
        }
    }
}
