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

class HeadersTests: XCTestCase {

    static var allTests = [
        ("testAppend", testAppend),
        ("testSubscript", testSubscript),
        ("testInsert", testInsert),
        ("testSerialize", testSerialize),
        ("testValidNatsHeaderName", testValidNatsHeaderName),
        ("testDollarNatsHeaderName", testDollarNatsHeaderName),
        ("testInvalidNatsHeaderName", testInvalidNatsHeaderName),
        ("testInvalidNatsHeaderNameWithSpecialCharacters", testInvalidNatsHeaderNameWithSpecialCharacters),

    ]

    func testAppend() {
        var hm = NatsHeaderMap()
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("bar"))
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("baz"))
        XCTAssertEqual(hm.getAll(try! NatsHeaderName("foo")), [NatsHeaderValue("bar"), NatsHeaderValue("baz")])
    }

    func testInsert() {
        var hm = NatsHeaderMap()
        hm.insert(try! NatsHeaderName("foo"), NatsHeaderValue("bar"))
        XCTAssertEqual(hm.getAll(try! NatsHeaderName("foo")), [NatsHeaderValue("bar")])
    }

    func testSerialize() {
        var hm = NatsHeaderMap()
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("bar"))
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("baz"))
        hm.insert(try! NatsHeaderName("bar"), NatsHeaderValue("foo"))

        let expected = ["NATS/1.0\r\nfoo:bar\r\nfoo:baz\r\nbar:foo\r\n\r\n",
                        "NATS/1.0\r\nbar:foo\r\nfoo:bar\r\nfoo:baz\r\n\r\n"]

        XCTAssertTrue(expected.contains(String(bytes: hm.toBytes(), encoding: .utf8)!))
    }

    func testValidNatsHeaderName() {
        XCTAssertNoThrow(try NatsHeaderName("X-Custom-Header"))
    }

    func testDollarNatsHeaderName() {
        XCTAssertNoThrow(try NatsHeaderName("$Dollar"))
    }

    func testInvalidNatsHeaderName() {
        XCTAssertThrowsError(try NatsHeaderName("Invalid Header Name"))
    }

    func testInvalidNatsHeaderNameWithSpecialCharacters() {
        XCTAssertThrowsError(try NatsHeaderName("Invalid:Header:Name"))
    }

    func testSubscript() {
        var hm = NatsHeaderMap()

        // Test setting a value
        hm[try! NatsHeaderName("foo")] = NatsHeaderValue("bar")
        XCTAssertEqual(hm[try! NatsHeaderName("foo")], NatsHeaderValue("bar"))

        // Test updating existing value
        hm[try! NatsHeaderName("foo")] = NatsHeaderValue("baz")
        XCTAssertEqual(hm[try! NatsHeaderName("foo")], NatsHeaderValue("baz"))

        // Test retrieving non-existing value (should be nil or default)
        XCTAssertNil(hm[try! NatsHeaderName("non-existing")])

        // Test removal of a value
        hm[try! NatsHeaderName("foo")] = nil
        XCTAssertNil(hm[try! NatsHeaderName("foo")])
    }
}
