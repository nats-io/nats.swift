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

import NIOCore
import NIOEmbedded
import XCTest

@testable import Nats

/// An RTT command is enqueued per outgoing `PING` and completed by the matching `PONG`.
/// A connection that dies with pings in flight never sends that `PONG`, so these tests pin
/// down what happens to the promise when the command is dropped instead of completed.
class RttCommandTests: XCTestCase {

    static var allTests = [
        ("testDroppedCommandFailsItsPromise", testDroppedCommandFailsItsPromise),
        ("testCompletedCommandKeepsItsValue", testCompletedCommandKeepsItsValue),
        ("testDrainEmptiesTheQueue", testDrainEmptiesTheQueue),
    ]

    /// Without this, the promise deinits unfulfilled, which is what NIO traps on in
    /// `EventLoopFuture.deinit`: `fatalError("leaking promise created at …")`, taking down
    /// every debug build of the host application. The trap itself needs a real event loop —
    /// `EmbeddedEventLoop` does not track promise creation — so here the same regression
    /// shows up as a plain failure instead.
    func testDroppedCommandFailsItsPromise() throws {
        let channel = EmbeddedChannel()
        defer { _ = try? channel.finish() }

        var command: RttCommand? = RttCommand.makeFrom(channel: channel)
        let future = command!.promise!.futureResult

        var result: Result<TimeInterval, Error>?
        future.whenComplete { result = $0 }

        XCTAssertNil(result, "the promise must not complete while the command is alive")
        command = nil

        switch result {
        case .failure(let error):
            // `ClientError` is not `Equatable`, so match the case rather than the value.
            guard let clientError = error as? NatsError.ClientError,
                case .connectionClosed = clientError
            else {
                XCTFail("dropped command failed with an unexpected error: \(error)")
                return
            }
        case .success(let rtt):
            XCTFail("dropped command reported a round trip time of \(rtt)")
        case nil:
            XCTFail("dropped command left its promise unfulfilled")
        }
    }

    /// The failure above is a backstop, not an override: a command that was answered keeps
    /// the round trip time it measured.
    func testCompletedCommandKeepsItsValue() throws {
        let channel = EmbeddedChannel()
        defer { _ = try? channel.finish() }

        var command: RttCommand? = RttCommand.makeFrom(channel: channel)
        let future = command!.promise!.futureResult

        command!.setRoundTripTime()
        command = nil

        XCTAssertGreaterThanOrEqual(try future.wait(), 0)
    }

    func testDrainEmptiesTheQueue() {
        let queue = ConcurrentQueue<Int>()
        queue.enqueue(1)
        queue.enqueue(2)
        queue.enqueue(3)

        XCTAssertEqual(queue.drain(), [1, 2, 3])
        XCTAssertNil(queue.dequeue())
        XCTAssertEqual(queue.drain(), [])
    }
}
