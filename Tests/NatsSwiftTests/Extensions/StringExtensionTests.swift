//
//  StringExtensionTests.swift
//  SwiftyNatsTests
//

import XCTest
@testable import SwiftyNats

class StringExtensionTests: XCTestCase {

    static var allTests = [
        ("testRemovePrefix", testRemovePrefix),
        ("testToJsonDicitonary", testToJsonDicitonary),
        ("testRemoveNewlines", testRemoveNewlines),
        ("testGetMessageType", testGetMessageType),
        ("testParseOutMessages", testParseOutMessages)
    ]

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
    }

    func testRemovePrefix() {

        let testGroups = [
            "PUB": [ "PUB swift.test 5 \r\n hello": " swift.test 5 \r\n hello" ],
            "INFO": [ "INFO {json}": " {json}" ],
            "SUB": [ "SUB swift.test 1": " swift.test 1" ]
        ]

        for (prefix, testCases) in testGroups {
            for (input, expected) in testCases {
                let result = input.removePrefix(prefix)
                XCTAssertTrue(result == expected, "String removed prefix correctly")
            }
        }

    }

    func testToJsonDicitonary() {

        let errMsg = "String was not converted to dict correctly"

        let dictString = "{\"sslRequired\": true, \"serverName\": \"nats\", \"rate\": 22, \"tag\": null}"
        guard let dict = dictString.toJsonDicitonary() else { XCTAssertTrue(false, errMsg); return }

        let serverName = dict["serverName"] as? String
        let rate = dict["rate"] as? Int
        let sslRequired = dict["sslRequired"] as? Bool
        let tag = dict["tag"] as? String?

        XCTAssertTrue(serverName == "nats", errMsg)
        XCTAssertTrue(rate == 22, errMsg)
        XCTAssertTrue(sslRequired == true, errMsg)
        XCTAssertTrue(tag == nil, errMsg)

    }

    func testRemoveNewlines() {

        let testCases = [
            "first line\n next line\n last line\n": "first line next line last line",
            "INFO\n{json from the server}": "INFO{json from the server}"
        ]

        for (input, expected) in testCases {
            let result = input.removeNewlines()
            XCTAssertTrue(result == expected, "String did not remove newlines correctly")
        }
    }

    func testGetMessageType() {

        let testCases = [
            "MSG subject.topic sid 1\r\na message\r\n": NatsOperation.message,
            "+OK": NatsOperation.ok,
            "-ERR detailed error message": NatsOperation.error,
            "PING": NatsOperation.ping,
            "PONG": NatsOperation.pong,
            "PIGLET": nil
        ]

        for (input, expected) in testCases {
            let result = input.getMessageType()
            XCTAssertTrue(result == expected, "String did not get correct message type")
        }

    }

    func testParseOutMessages() {

        let testCases = [
            "MSG subject.topic sid 1\r\na message\r\n+OK\r\nPONG\r\n": [ "MSG subject.topic sid 1\ra message\r", "+OK\r", "PONG\r" ],
            "+OK\r\nPONG\r\n-ERR an error message\r\n": [ "+OK\r", "PONG\r", "-ERR an error message\r" ]
        ]

        for (input, expected) in testCases {
            let result = input.parseOutMessages()
            XCTAssertTrue(result == expected, "String did not parse messages")
        }

    }

}
