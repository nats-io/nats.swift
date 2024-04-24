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
        natsServer.start()
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        _ = Context(client: client)
        _ = Context(client: client, prefix: "$JS.API")
        _ = Context(client: client, domain: "STREAMS")
        _ = Context(client: client, timeout: 10)
        _ = Context(client: client, domain: "STREAMS", timeout: 10)

        try await client.close()
    }
}
