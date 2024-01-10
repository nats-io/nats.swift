import Foundation
import Logging
import XCTest
@testable import NatsSwift

class TestMessageWithHeadersTests: XCTestCase {

    static var allTests = [
        ("testMessageWithHeaders", testMessageWithHeaders)
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testMessageWithHeaders() async throws  {
        natsServer.start()
        logger.logLevel = .debug

        let client = Client(url : URL(string: "nats://localhost:4222")!)
        try await client.connect()

        let sub = try await client.subscribe(to: "foo")

        var hm = HeaderMap()
        hm.append(try! HeaderName("foo"), HeaderValue("bar"))
        hm.append(try! HeaderName("foo"), HeaderValue("baz"))
        hm.insert(try! HeaderName("another"), HeaderValue("one"))

        try client.publish("hello".data(using: .utf8)!, subject: "foo", reply: nil, headers: hm)

        let msg =  await sub.next()
        XCTAssertEqual(msg!.headers, hm)

    }
}
