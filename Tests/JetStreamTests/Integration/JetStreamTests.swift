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

import Logging
import NIO
import Nats
import NatsServer
import XCTest

@testable import JetStream
@testable import Nats

class JetStreamTests: XCTestCase {

    static var allTests = [
        ("testJetStreamContext", testJetStreamContext)
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testJetStreamContext() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        _ = JetStreamContext(client: client)
        _ = JetStreamContext(client: client, prefix: "$JS.API")
        _ = JetStreamContext(client: client, domain: "STREAMS")
        _ = JetStreamContext(client: client, timeout: 10)
        var ctx = JetStreamContext(client: client)

        let stream = """
            {
                "name": "FOO",
                "subjects": ["foo"]
            }
            """
        let data = stream.data(using: .utf8)!

        var resp = try await client.request(data, subject: "$JS.API.STREAM.CREATE.FOO")
        var ack = try await ctx.publish("foo", message: "Hello, World!".data(using: .utf8)!)
        _ = try await ack.wait()

        try await client.close()
    }

    func testRequest() async throws {

        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        var ctx = JetStreamContext(client: client)

        let stream = """
            {
                "name": "FOO",
                "subjects": ["foo"]
            }
            """
        let data = stream.data(using: .utf8)!

        var resp = try await client.request(data, subject: "$JS.API.STREAM.CREATE.FOO")

        let info: Response<AccountInfo> = try await ctx.request("INFO", message: Data())

        guard case .success(let info) = info else {
            XCTFail("request should be successful")
            return
        }

        XCTAssertEqual(info.streams, 1)
        let badInfo: Response<AccountInfo> = try await ctx.request("STREAM.INFO.BAD", message: Data())
        guard case .error(let jetStreamAPIResponse) = badInfo else {
            XCTFail("should get error")
            return
        }

        XCTAssertEqual(ErrorCode.streamNotFound, jetStreamAPIResponse.error.errorCode)


    }
}
