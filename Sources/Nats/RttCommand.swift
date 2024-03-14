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

internal class RttCommand {
    let startTime = ContinuousClock().now
    let promise: EventLoopPromise<Duration>?

    static func makeFrom(channel: Channel?) -> RttCommand {
        RttCommand(promise: channel?.eventLoop.makePromise(of: Duration.self))
    }

    private init(promise: EventLoopPromise<Duration>?) {
        self.promise = promise
    }

    func setRoundTripTime() {
        let now: ContinuousClock.Instant = ContinuousClock().now
        let rtt: Duration = now - startTime
        promise?.succeed(rtt)
    }

    func getRoundTripTime() async throws -> Duration {
        try await promise?.futureResult.get() ?? Duration.zero
    }
}
