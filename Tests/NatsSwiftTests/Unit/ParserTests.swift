//
//  ParserTests.swift
//  NatsSwiftTests
//

import XCTest
@testable import NatsSwift

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

        let testCases = [
            TestCase(
                name: "Single chunk, different operations",
                givenChunks: ["MSG foo 1 5\r\nhello\r\n+OK\r\nPONG\r\n"],
                expectedOps: [
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)),
                    .Ok,
                    .Pong
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
                    "hello\r\n"
                ],
                expectedOps: [
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)),
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "world".data(using: .utf8)!, length: 5)),
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)),
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "world".data(using: .utf8)!, length: 5)),
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)),
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "world".data(using: .utf8)!, length: 5)),
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)),
                ]
            ),
            TestCase(
                name: "With empty lines",
                givenChunks: [
                    "MSG foo 1 5\r\nhello\r\nMSG foo 1 5\r\nwo",
                    "",
                    "",
                    "rld\r\nMSG f",
                    "oo 1 5\r\nhello\r\n"
                ],
                expectedOps: [
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)),
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "world".data(using: .utf8)!, length: 5)),
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: "hello".data(using: .utf8)!, length: 5)),
                ]
            ),
            TestCase(
                name: "With crlf in payload",
                givenChunks: [
                    "MSG foo 1 7\r\nhe\r\nllo\r\n",
                ],
                expectedOps: [
                    .Message(MessageInbound(subject: "foo", sid: 1, payload: Data("he\r\nllo".utf8), length: 7))
                ]
            )
        ]

        for (tn, tc) in testCases.enumerated() {
            logger.logLevel = .debug
            var ops = [ServerOp]()
            var prevRemainder: Data?
            for chunk in tc.givenChunks {
                var chunkData = Data(chunk.utf8)
                if let prevRemainder {
                    chunkData.prepend(prevRemainder)
                }
                let res = chunkData.parseOutMessages()
                prevRemainder = res.remainder
                ops.append(contentsOf: res.ops)
            }
            XCTAssertEqual(ops.count, tc.expectedOps.count)
            for (i, op) in ops.enumerated() {
                switch op {
                case .Ok:
                    if case .Ok = tc.expectedOps[i] {} else {
                        XCTFail(fail(tn, tc.name))
                    }
                case .Info(let info):
                    if case .Info(let expectedInfo) = tc.expectedOps[i] {
                        XCTAssertEqual(info, expectedInfo, fail(tn, tc.name))
                    } else {
                        XCTFail(fail(tn, tc.name))
                    }

                case .Ping:
                    if case .Ping = tc.expectedOps[i] {} else {
                        XCTFail(fail(tn, tc.name))
                    }
                case .Pong:
                    if case .Pong = tc.expectedOps[i] {} else {
                        XCTFail(fail(tn, tc.name))
                    }
                case .Error(_):
                    if case .Error(_) = tc.expectedOps[i] {} else {
                        XCTFail(fail(tn, tc.name))
                    }
                case .Message(let msg):
                    if case .Message(let expectedMessage) = tc.expectedOps[i] {
                        XCTAssertEqual(msg, expectedMessage, fail(tn, tc.name))
                    } else {
                        XCTFail(fail(tn, tc.name))
                    }
                case .HMessage(let msg):
                    if case .HMessage(let expectedMessage) = tc.expectedOps[i] {
                        XCTAssertEqual(msg, expectedMessage, fail(tn, tc.name))
                    } else {
                        XCTFail(fail(tn, tc.name))
                    }
                }
            }
        }
    }
}
