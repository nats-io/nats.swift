// Copyright 2026 The NATS Authors
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
import NatsServer
import XCTest

@testable import Nats

class ServerInputTests: XCTestCase {

    static var allTests = [
        (
            "testInfoThatDoesNotParseIsSkippedOnceConnected",
            testInfoThatDoesNotParseIsSkippedOnceConnected
        )
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testInfoThatDoesNotParseIsSkippedOnceConnected() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        let reported = XCTestExpectation(description: "INFO that does not parse was not reported")
        let disconnected = XCTestExpectation(description: "client was disconnected")
        disconnected.isInverted = true
        client.on(.error) { event in
            if case .error(let error) = event, error is NatsError.ProtocolError {
                reported.fulfill()
            }
        }
        client.on(.disconnected) { _ in disconnected.fulfill() }
        try await client.connect()

        let sub = try await client.subscribe(subject: "foo")
        let received = XCTestExpectation(description: "message after the INFO was not delivered")
        let reader = Task {
            for try await msg in sub where msg.payload == Data("hello".utf8) {
                received.fulfill()
                return
            }
        }

        try await receive("INFO {\"server_id\":\r\nMSG foo \(sub.sid) 5\r\nhello\r\n", on: client)

        await fulfillment(of: [reported, received], timeout: 5.0)
        await fulfillment(of: [disconnected], timeout: 0.5)
        reader.cancel()
        try await client.close()
    }

    /// Hands `bytes` to the client as if the server had sent them on the current connection.
    private func receive(_ bytes: String, on client: NatsClient) async throws {
        let channel = try XCTUnwrap(client.connectionHandler?.channel)
        try await channel.eventLoop.submit {
            channel.pipeline.syncOperations.fireChannelRead(NIOAny(ByteBuffer(string: bytes)))
            channel.pipeline.syncOperations.fireChannelReadComplete()
        }.get()
    }
}
