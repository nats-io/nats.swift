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
import NatsServer
import XCTest

@testable import Nats

class SlowConsumerTests: XCTestCase {

    static var allTests = [
        ("testSlowConsumerEventFiresOnOverflow", testSlowConsumerEventFiresOnOverflow),
        ("testSlowConsumerReArmsAfterDrain", testSlowConsumerReArmsAfterDrain),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testSlowConsumerEventFiresOnOverflow() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()

        let expectation = XCTestExpectation(description: "slow consumer event was not fired")
        expectation.assertForOverFulfill = true  // exactly one event per overflow episode
        client.on(.error) { event in
            if case .error(let err) = event,
                let subErr = err as? NatsError.SubscriptionError,
                case .slowConsumer = subErr
            {
                expectation.fulfill()
            }
        }
        try await client.connect()

        // Capacity 2 and we never read from the subscription, so once more than two
        // messages are delivered the buffer overflows and must fire a slow consumer
        // event exactly once.
        _ = try await client.subscribe(subject: "foo", capacity: 2)
        _ = try await client.rtt()  // ensure the SUB reached the server before publishing

        let payload = "x".data(using: .utf8)!
        for _ in 0..<10 {
            try await client.publish(payload, subject: "foo")
        }
        try await client.flush()

        await fulfillment(of: [expectation], timeout: 5.0)
        try await client.close()
    }

    func testSlowConsumerReArmsAfterDrain() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()

        let episodes = XCTestExpectation(description: "two slow consumer episodes")
        episodes.expectedFulfillmentCount = 2
        episodes.assertForOverFulfill = true
        client.on(.error) { event in
            if case .error(let err) = event,
                let subErr = err as? NatsError.SubscriptionError,
                case .slowConsumer = subErr
            {
                episodes.fulfill()
            }
        }
        try await client.connect()

        let sub = try await client.subscribe(subject: "foo", capacity: 2)
        _ = try await client.rtt()

        let payload = "x".data(using: .utf8)!
        // Episode 1: overflow the buffer.
        for _ in 0..<6 {
            try await client.publish(payload, subject: "foo")
        }
        try await client.flush()
        _ = try await client.rtt()  // let the inbound messages be processed

        // Drain below capacity/2 so the slow-consumer signal re-arms.
        var iter = sub.makeAsyncIterator()
        _ = try await iter.next()
        _ = try await iter.next()

        // Episode 2: overflow again -> a second event must fire.
        for _ in 0..<6 {
            try await client.publish(payload, subject: "foo")
        }
        try await client.flush()

        await fulfillment(of: [episodes], timeout: 5.0)
        try await client.close()
    }
}
