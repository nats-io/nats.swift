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
import NIO
import Nats
import NatsServer
import XCTest

class TestMessageWithHeadersTests: XCTestCase {

    static var allTests = [
        ("testMessageWithHeaders", testMessageWithHeaders),
        (
            "testSubscriberKeepsReceivingAfterMessageWithNonAsciiHeaderName",
            testSubscriberKeepsReceivingAfterMessageWithNonAsciiHeaderName
        ),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testMessageWithHeaders() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let sub = try await client.subscribe(subject: "foo")

        var hm = NatsHeaderMap()
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("bar"))
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("baz"))
        hm.insert(try! NatsHeaderName("another"), NatsHeaderValue("one"))

        try await client.publish(
            "hello".data(using: .utf8)!, subject: "foo", reply: nil, headers: hm)

        let iter = sub.makeAsyncIterator()
        let msg = try await iter.next()
        XCTAssertEqual(msg!.headers, hm)

    }

    func testSubscriberKeepsReceivingAfterMessageWithNonAsciiHeaderName() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        let disconnected = XCTestExpectation(description: "subscriber was disconnected")
        disconnected.isInverted = true
        client.on(.disconnected) { _ in disconnected.fulfill() }
        try await client.connect()

        let sub = try await client.subscribe(subject: "foo")
        // the server has the subscription once it answered a PING sent after it
        _ = try await client.rtt()

        let received = XCTestExpectation(description: "message after the header was not received")
        let reader = Task {
            for try await msg in sub {
                if msg.payload == Data("world".utf8) {
                    received.fulfill()
                    return
                }
                XCTFail("unexpected message: \(msg)")
            }
        }

        // a publisher that writes the protocol itself sends a header name the server
        // passes on unchanged
        let headers = "NATS/1.0\r\n\u{e9}l: x\r\n\r\n"
        let size = headers.utf8.count
        let publisher = try await ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .connect(host: "127.0.0.1", port: natsServer.port!).get()
        try await publisher.writeAndFlush(
            ByteBuffer(
                string: "CONNECT {\"verbose\":false,\"headers\":true}\r\n"
                    + "HPUB foo \(size) \(size + 5)\r\n\(headers)hello\r\n"
                    + "PUB foo 5\r\nworld\r\nPING\r\n"))

        await fulfillment(of: [received], timeout: 5.0)
        await fulfillment(of: [disconnected], timeout: 0.5)
        reader.cancel()
        try await publisher.close()
        try await client.close()
    }
}
