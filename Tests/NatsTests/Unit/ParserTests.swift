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

import NIOCore
import NIOEmbedded
import NIOWebSocket
import XCTest

@testable import Nats

class ParserTests: XCTestCase {

    static var allTests = [
        ("testParseOutMessages", testParseOutMessages),
        ("testParseNonUtf8LineThrows", testParseNonUtf8LineThrows),
        ("testParseInvalidMessageSizesThrow", testParseInvalidMessageSizesThrow),
        ("testParseHugeMessageSizeWaitsForPayload", testParseHugeMessageSizeWaitsForPayload),
        ("testParseEmptyHeaderBlockThrows", testParseEmptyHeaderBlockThrows),
        (
            "testHeaderBlockThatDoesNotParseDropsOnlyThatMessage",
            testHeaderBlockThatDoesNotParseDropsOnlyThatMessage
        ),
        (
            "testHeaderWithEmptyValueOrFoldedLineIsDelivered",
            testHeaderWithEmptyValueOrFoldedLineIsDelivered
        ),
        ("testWebsocketTextFrameIsAnError", testWebsocketTextFrameIsAnError),
        ("testWebsocketContinuationFrameIsAnError", testWebsocketContinuationFrameIsAnError),
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

    func testParseNonUtf8LineThrows() {
        let cases: [(String, Data)] = [
            ("short line", Data([0xff, 0x0d, 0x0a])),
            ("unknown operation", Data([0xff, 0xfe, 0xfd, 0x0d, 0x0a])),
            ("client operation", Data("PUB ".utf8) + Data([0xff, 0x0d, 0x0a])),
        ]
        for (name, input) in cases {
            XCTAssertThrowsError(try input.parseOutMessages(), name)
        }
    }

    func testParseInvalidMessageSizesThrow() {
        let cases = [
            "MSG foo 1 -5\r\nhello\r\n",
            "MSG foo 1 abc\r\n",
            "HMSG foo 1 -1 5\r\nhello\r\n",
            "HMSG foo 1 abc 5\r\nhello\r\n",
            "HMSG foo 1 0 abc\r\n",
            "HMSG foo 1 30 5\r\nhello\r\n",
        ]
        for input in cases {
            XCTAssertThrowsError(try Data(input.utf8).parseOutMessages(), input)
        }
    }

    func testParseHugeMessageSizeWaitsForPayload() throws {
        let cases = [
            "MSG foo 1 9223372036854775807\r\nhello\r\n",
            "HMSG foo 1 12 9223372036854775807\r\nNATS/1.0\r\n\r\nhello\r\n",
        ]
        for input in cases {
            let data = Data(input.utf8)
            let result = try data.parseOutMessages()
            XCTAssertTrue(result.ops.isEmpty, input)
            XCTAssertEqual(result.remainder, data, input)
        }
    }

    func testParseEmptyHeaderBlockThrows() {
        XCTAssertThrowsError(try NatsHeaderMap(from: ""))
    }

    func testHeaderBlockThatDoesNotParseDropsOnlyThatMessage() throws {
        var headerBlocks = [
            "",
            "NATS/2.0\r\n\r\n",
            "NATS/1.0 abc\r\n\r\n",
            "NATS/1.0\r\na b:c\r\n\r\n",
            "NATS/1.0\r\n\u{e9}l:x\r\n\r\n",
            "NATS/1.0\r\nbad\r\n\r\n",
            "NATS/1.0 503\r\nbad line\r\n\r\n",
            "NATS/1.0\r\n:v\r\n\r\n",
        ].map { Data($0.utf8) }
        // a header block that is not valid UTF-8
        headerBlocks.append(Data("NATS/1.0\r\nX: ".utf8) + Data([0xff]) + Data("\r\n\r\n".utf8))
        var inputs = headerBlocks.map { headers in
            Data("HMSG foo 1 \(headers.count) \(headers.count + 5)\r\n".utf8) + headers
                + Data("hello\r\nMSG foo 1 5\r\nworld\r\nPING\r\n".utf8)
        }
        // a message without a header block
        inputs.append(Data("HMSG foo 1 0 0\r\n\r\nMSG foo 1 5\r\nworld\r\nPING\r\n".utf8))
        for input in inputs {
            let name = String(decoding: input, as: UTF8.self)
            let result = try input.parseOutMessages()
            XCTAssertNil(result.remainder, name)
            guard result.ops.count == 2, case .message(let msg) = result.ops[0],
                case .ping = result.ops[1]
            else {
                XCTFail("expected the second message and PING only: \(name)")
                continue
            }
            XCTAssertEqual(msg.payload, Data("world".utf8), name)
        }
    }

    func testHeaderWithEmptyValueOrFoldedLineIsDelivered() throws {
        let cases: [(String, [(String, String)])] = [
            ("NATS/1.0\r\nX-A:\r\nX-B: b\r\n\r\n", [("X-A", ""), ("X-B", "b")]),
            ("NATS/1.0\r\nX: a\r\n b\r\n\r\n", [("X", "a")]),
        ]
        for (headers, expected) in cases {
            let size = headers.utf8.count
            let input = "HMSG foo 1 \(size) \(size + 5)\r\n\(headers)hello\r\n"
            let result = try Data(input.utf8).parseOutMessages()
            XCTAssertNil(result.remainder, input)
            guard result.ops.count == 1, case .hMessage(let msg) = result.ops[0] else {
                XCTFail("expected the message: \(input)")
                continue
            }
            XCTAssertEqual(msg.payload, Data("hello".utf8), input)
            var expectedHeaders = NatsHeaderMap()
            for (name, value) in expected {
                expectedHeaders.append(try NatsHeaderName(name), NatsHeaderValue(value))
            }
            XCTAssertEqual(msg.headers, expectedHeaders, input)
        }
    }

    func testWebsocketTextFrameIsAnError() throws {
        let channel = EmbeddedChannel(handler: WebSocketByteBufferCodec())
        XCTAssertThrowsError(
            try channel.writeInbound(
                WebSocketFrame(fin: true, opcode: .text, data: ByteBuffer(string: "PING\r\n")))
        ) { error in
            XCTAssertEqual(
                error as? NatsError.ProtocolError,
                .parserFailure("unexpected websocket text frame"))
        }
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))
    }

    func testWebsocketContinuationFrameIsAnError() throws {
        let channel = EmbeddedChannel(handler: WebSocketByteBufferCodec())
        XCTAssertThrowsError(
            try channel.writeInbound(
                WebSocketFrame(fin: true, opcode: .continuation, data: ByteBuffer(string: "x")))
        ) { error in
            XCTAssertEqual(
                error as? NatsError.ProtocolError,
                .parserFailure("unexpected websocket continuation frame"))
        }
        XCTAssertNil(try channel.readInbound(as: ByteBuffer.self))
    }
}
