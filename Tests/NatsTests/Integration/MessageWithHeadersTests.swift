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

import Foundation
import Logging
import XCTest

@testable import Nats

class TestMessageWithHeadersTests: XCTestCase {

    static var allTests = [
        ("testMessageWithHeaders", testMessageWithHeaders)
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testMessageWithHeaders() async throws {
        natsServer.start()
        logger.logLevel = .debug

        let client = ClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let sub = try await client.subscribe(to: "foo")

        var hm = HeaderMap()
        hm.append(try! HeaderName("foo"), HeaderValue("bar"))
        hm.append(try! HeaderName("foo"), HeaderValue("baz"))
        hm.insert(try! HeaderName("another"), HeaderValue("one"))

        try await client.publish("hello".data(using: .utf8)!, subject: "foo", reply: nil, headers: hm)

        let iter = sub.makeAsyncIterator()
        let msg = await iter.next()
        XCTAssertEqual(msg!.headers, hm)

    }
}
