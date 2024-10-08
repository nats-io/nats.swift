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
import Nats
import Nuid

/// Extension to ``Consumer`` adding pull consumer capabilities.
extension Consumer {

    /// Retrieves up to a provided number of messages from a stream.
    /// This method will send a single request and deliver either requested messages unless time out is met earlier.
    ///
    ///  - Parameters:
    ///   - batch: maximum number of messages to be retrieved
    ///   - expires: timeout of a pull request
    ///   - idleHeartbeat: interval in which server should send heartbeat messages (if no user messages are available.
    ///
    ///  - Returns: ``FetchResult`` which implements ``AsyncSequence`` allowing iteration over messages.
    ///
    ///  - Throws:
    ///   - ``JetStreamError/FetchError`` if there was an error fetching messages
    public func fetch(
        batch: Int, expires: TimeInterval = 30, idleHeartbeat: TimeInterval? = nil
    ) async throws -> FetchResult {
        var request: PullRequest
        if let idleHeartbeat {
            request = PullRequest(
                batch: batch, expires: NanoTimeInterval(expires),
                heartbeat: NanoTimeInterval(idleHeartbeat))
        } else {
            request = PullRequest(batch: batch, expires: NanoTimeInterval(expires))
        }

        let subject = ctx.apiSubject("CONSUMER.MSG.NEXT.\(info.stream).\(info.name)")
        let inbox = "_INBOX.\(nextNuid())"
        let sub = try await ctx.client.subscribe(subject: inbox)
        try await self.ctx.client.publish(
            JSONEncoder().encode(request), subject: subject, reply: inbox)
        return FetchResult(ctx: ctx, sub: sub, idleHeartbeat: idleHeartbeat, batch: batch)
    }
}

///
public class FetchResult: AsyncSequence {
    public typealias Element = JetStreamMessage
    public typealias AsyncIterator = FetchIterator

    private let ctx: JetStreamContext
    private let sub: NatsSubscription
    private let idleHeartbeat: TimeInterval?
    private let batch: Int

    init(ctx: JetStreamContext, sub: NatsSubscription, idleHeartbeat: TimeInterval?, batch: Int) {
        self.ctx = ctx
        self.sub = sub
        self.idleHeartbeat = idleHeartbeat
        self.batch = batch
    }

    public func makeAsyncIterator() -> FetchIterator {
        return FetchIterator(
            ctx: ctx,
            sub: self.sub, idleHeartbeat: self.idleHeartbeat, remainingMessages: self.batch)
    }

    public struct FetchIterator: AsyncIteratorProtocol {
        private let ctx: JetStreamContext
        private let sub: NatsSubscription
        private let idleHeartbeat: TimeInterval?
        private var remainingMessages: Int
        private var subIterator: NatsSubscription.AsyncIterator

        init(
            ctx: JetStreamContext, sub: NatsSubscription, idleHeartbeat: TimeInterval?,
            remainingMessages: Int
        ) {
            self.ctx = ctx
            self.sub = sub
            self.idleHeartbeat = idleHeartbeat
            self.remainingMessages = remainingMessages
            self.subIterator = sub.makeAsyncIterator()
        }

        public mutating func next() async throws -> JetStreamMessage? {
            if remainingMessages <= 0 {
                try await sub.unsubscribe()
                return nil
            }

            while true {
                let message: NatsMessage?

                if let idleHeartbeat = idleHeartbeat {
                    let timeout = idleHeartbeat * 2
                    message = try await fetchWithTimeout(timeout, subIterator)
                } else {
                    message = try await subIterator.next()
                }

                guard let message else {
                    // the subscription has ended
                    try await sub.unsubscribe()
                    return nil
                }

                let status = message.status ?? .ok

                switch status {
                case .timeout:
                    try await sub.unsubscribe()
                    return nil
                case .idleHeartbeat:
                    // in case of idle heartbeat error, we want to
                    // wait for next message on subscription
                    continue
                case .notFound:
                    try await sub.unsubscribe()
                    return nil
                case .ok:
                    remainingMessages -= 1
                    return JetStreamMessage(message: message, client: ctx.client)
                case .badRequest:
                    try await sub.unsubscribe()
                    throw JetStreamError.FetchError.badRequest
                case .noResponders:
                    try await sub.unsubscribe()
                    throw JetStreamError.FetchError.noResponders
                case .requestTerminated:
                    try await sub.unsubscribe()
                    guard let description = message.description else {
                        throw JetStreamError.FetchError.invalidResponse
                    }

                    let descLower = description.lowercased()
                    if descLower.contains("message size exceeds maxbytes")
                        || descLower.contains("leadership changed")
                    {
                        return nil
                    } else if descLower.contains("consumer deleted") {
                        throw JetStreamError.FetchError.consumerDeleted
                    } else if descLower.contains("consumer is push based") {
                        throw JetStreamError.FetchError.consumerIsPush
                    }
                default:
                    throw JetStreamError.FetchError.unknownStatus(status, message.description)
                }

                if remainingMessages == 0 {
                    try await sub.unsubscribe()
                }

            }
        }

        func fetchWithTimeout(
            _ timeout: TimeInterval, _ subIterator: NatsSubscription.AsyncIterator
        ) async throws -> NatsMessage? {
            try await withThrowingTaskGroup(of: NatsMessage?.self) { group in
                group.addTask {
                    return try await subIterator.next()
                }
                group.addTask {
                    try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                    try await sub.unsubscribe()
                    return nil
                }
                defer {
                    group.cancelAll()
                }
                for try await result in group {
                    if let msg = result {
                        return msg
                    } else {
                        throw JetStreamError.FetchError.noHeartbeatReceived
                    }
                }
                // this should not be reachable
                throw JetStreamError.FetchError.noHeartbeatReceived
            }
        }
    }
}

internal struct PullRequest: Codable {
    let batch: Int
    let expires: NanoTimeInterval
    let maxBytes: Int?
    let noWait: Bool?
    let heartbeat: NanoTimeInterval?

    internal init(
        batch: Int, expires: NanoTimeInterval, maxBytes: Int? = nil, noWait: Bool? = nil,
        heartbeat: NanoTimeInterval? = nil
    ) {
        self.batch = batch
        self.expires = expires
        self.maxBytes = maxBytes
        self.noWait = noWait
        self.heartbeat = heartbeat
    }

    enum CodingKeys: String, CodingKey {
        case batch
        case expires
        case maxBytes = "max_bytes"
        case noWait = "no_wait"
        case heartbeat = "idle_heartbeat"
    }
}
