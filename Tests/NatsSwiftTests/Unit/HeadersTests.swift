
import XCTest
@testable import NatsSwift

class HeadersTests: XCTestCase {

    static var allTests = [
        ("testAppend", testAppend),
        ("testSubscript", testSubscript),
        ("testInsert", testInsert),
        ("testSerialize", testSerialize)

    ]

    func testAppend() {
        var hm = HeaderMap()
        hm.append(try! HeaderName("foo"), HeaderValue("bar"))
        hm.append(try! HeaderName("foo"), HeaderValue("baz"))
        XCTAssertEqual(hm.getAll(try! HeaderName("foo")), [HeaderValue("bar"), HeaderValue("baz")])
    }

    func testInsert() {
        var hm = HeaderMap()
        hm.insert(try! HeaderName("foo"), HeaderValue("bar"))
        XCTAssertEqual(hm.getAll(try! HeaderName("foo")), [HeaderValue("bar")])
    }

    func testSerialize() {
        var hm = HeaderMap()
        hm.append(try! HeaderName("foo"), HeaderValue("bar"))
        hm.append(try! HeaderName("foo"), HeaderValue("baz"))
        hm.insert(try! HeaderName("bar"), HeaderValue("foo"))

        let expected = "NATS/1.0\r\nfoo: bar\r\nfoo: baz\r\nbar: foo\r\n\r\n"
        let byteArray: [UInt8] = Array(expected.utf8)

        XCTAssertEqual(hm.toBytes(), byteArray)
    }

func testSubscript() {
    var hm = HeaderMap()

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
