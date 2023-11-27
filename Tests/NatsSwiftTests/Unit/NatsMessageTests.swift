//
//  NatsMessageTests.swift
//  NatsSwiftTests
//

import XCTest
@testable import NatsSwift

class NatsMessageTests: XCTestCase {

    static var allTests = [
        ("testMessageParser", testMessageParser)
    ]

    func testMessageParser() {

        let str = "MSG swift.test 643CE192 6\r\ntest-1\r\n"
        let message = NatsMessage.parse(str)

        XCTAssert(message?.payload == "test-1")
        XCTAssert(message?.subject.id == "643CE192")

    }

}
