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
        ("testValidHeaderName", testValidHeaderName),
        ("testDollarHeaderName", testDollarHeaderName),
        ("testInvalidHeaderName", testInvalidHeaderName),
        ("testInvalidHeaderNameWithSpecialCharacters", testInvalidHeaderNameWithSpecialCharacters),

    ]

    func testAppend() {
        var hm = NatsHeaderMap()
        hm.append(try! HeaderName("foo"), HeaderValue("bar"))
        hm.append(try! HeaderName("foo"), HeaderValue("baz"))
        XCTAssertEqual(hm.getAll(try! HeaderName("foo")), [HeaderValue("bar"), HeaderValue("baz")])
    }

    func testInsert() {
        var hm = NatsHeaderMap()
        hm.insert(try! HeaderName("foo"), HeaderValue("bar"))
        XCTAssertEqual(hm.getAll(try! HeaderName("foo")), [HeaderValue("bar")])
    }

    func testSerialize() {
        var hm = NatsHeaderMap()
        hm.append(try! HeaderName("foo"), HeaderValue("bar"))
        hm.append(try! HeaderName("foo"), HeaderValue("baz"))
        hm.insert(try! HeaderName("bar"), HeaderValue("foo"))

        let expected = "NATS/1.0\r\nfoo:bar\r\nfoo:baz\r\nbar:foo\r\n\r\n"
        let byteArray: [UInt8] = Array(expected.utf8)

        XCTAssertEqual(hm.toBytes(), byteArray)
    }

    func testValidHeaderName() {
        XCTAssertNoThrow(try HeaderName("X-Custom-Header"))
    }

    func testDollarHeaderName() {
        XCTAssertNoThrow(try HeaderName("$Dollar"))
    }

    func testInvalidHeaderName() {
        XCTAssertThrowsError(try HeaderName("Invalid Header Name"))
    }

    func testInvalidHeaderNameWithSpecialCharacters() {
        XCTAssertThrowsError(try HeaderName("Invalid:Header:Name"))
    }

    func testSubscript() {
        var hm = NatsHeaderMap()

        // Test setting a value
        hm[try! HeaderName("foo")] = HeaderValue("bar")
        XCTAssertEqual(hm[try! HeaderName("foo")], HeaderValue("bar"))

        // Test updating existing value
        hm[try! HeaderName("foo")] = HeaderValue("baz")
        XCTAssertEqual(hm[try! HeaderName("foo")], HeaderValue("baz"))

        // Test retrieving non-existing value (should be nil or default)
        XCTAssertNil(hm[try! HeaderName("non-existing")])

        // Test removal of a value
        hm[try! HeaderName("foo")] = nil
        XCTAssertNil(hm[try! HeaderName("foo")])
    }
}
