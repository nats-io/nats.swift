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
    /// This method will send a single request and deliver requested messages unless time out is met earlier.
    ///
    ///  - Parameters:
    ///   - batch: maximum number of messages to be retrieved
    ///   - expires: timeout of a pull request
    ///   - idleHeartbeat: interval in which server should send heartbeat messages (if no user messages are available).
    ///
    ///  - Returns: ``FetchResult`` which implements ``AsyncSequence`` allowing iteration over messages.
    ///
    ///  - Throws:
    ///   - ``JetStreamError/FetchError`` if there was an error while fetching messages
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
        let inbox = ctx.client.newInbox()
        let sub = try await ctx.client.subscribe(subject: inbox)
        do {
            try await self.ctx.client.publish(
                JSONEncoder().encode(request), subject: subject, reply: inbox)
        } catch {
            try? await sub.unsubscribe()
            throw error
        }
        return FetchResult(
            ctx: ctx, sub: sub, idleHeartbeat: idleHeartbeat, batch: batch, expires: expires)
    }
}

/// Used to iterate over results of ``Consumer/fetch(batch:expires:idleHeartbeat:)``.
public final class FetchResult: AsyncSequence, Sendable {
    public typealias Element = JetStreamMessage
    public typealias AsyncIterator = AsyncThrowingStream<JetStreamMessage, Error>.Iterator

    private let stream: AsyncThrowingStream<JetStreamMessage, Error>

    init(
        ctx: JetStreamContext, sub: NatsSubscription, idleHeartbeat: TimeInterval?, batch: Int,
        expires: TimeInterval
    ) {
        self.stream = AsyncThrowingStream { continuation in
            let producerTask = Task {
                let producer = FetchProducer(
                    ctx: ctx, sub: sub, idleHeartbeat: idleHeartbeat, remaining: batch,
                    expires: expires)
                await producer.run(into: continuation)
            }
            continuation.onTermination = { _ in
                producerTask.cancel()
                Task { await FetchResult.tearDownIgnoringClosed(sub) }
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        stream.makeAsyncIterator()
    }

    private static func tearDownIgnoringClosed(_ sub: NatsSubscription) async {
        do {
            try await sub.unsubscribe()
        } catch NatsError.SubscriptionError.subscriptionClosed,
            NatsError.ClientError.connectionClosed
        {
        } catch {
            logger.error("error tearing down fetch subscription: \(error)")
        }
    }
}

private struct FetchProducer {
    private let ctx: JetStreamContext
    private let idleHeartbeat: TimeInterval?
    private let remaining: Int
    private let deadlineUptime: TimeInterval
    private let subIterator: NatsSubscription.AsyncIterator

    private static let deadlineGrace: TimeInterval = 1

    init(
        ctx: JetStreamContext, sub: NatsSubscription, idleHeartbeat: TimeInterval?, remaining: Int,
        expires: TimeInterval
    ) {
        self.ctx = ctx
        self.idleHeartbeat = idleHeartbeat
        self.remaining = remaining
        self.subIterator = sub.makeAsyncIterator()
        self.deadlineUptime =
            ProcessInfo.processInfo.systemUptime + expires + Self.deadlineGrace
    }

    private enum ReadOutcome {
        case message(NatsMessage)
        case subscriptionEnded
        case missedHeartbeat
    }

    private enum MessageOutcome {
        case deliver(JetStreamMessage)
        case skip
        case end
    }

    fileprivate typealias Continuation = AsyncThrowingStream<JetStreamMessage, Error>.Continuation

    func run(into continuation: Continuation) async {
        await withTaskGroup(of: Void.self) { group in
            group.addTask { await self.finishAtDeadline(continuation) }
            group.addTask { [subIterator, ctx, idleHeartbeat, remaining] in
                await Self.readLoop(
                    subIterator: subIterator, ctx: ctx, idleHeartbeat: idleHeartbeat,
                    remaining: remaining, into: continuation)
            }
            await group.next()
            group.cancelAll()
        }
    }

    private func finishAtDeadline(_ continuation: Continuation) async {
        let nanos = Self.nanoseconds(
            max(0, deadlineUptime - ProcessInfo.processInfo.systemUptime))
        try? await Task.sleep(nanoseconds: nanos)
        continuation.finish()
    }

    private static func readLoop(
        subIterator: NatsSubscription.AsyncIterator, ctx: JetStreamContext,
        idleHeartbeat: TimeInterval?, remaining: Int, into continuation: Continuation
    ) async {
        var remaining = remaining
        do {
            while remaining > 0 {
                let message: NatsMessage
                switch try await readNextMessage(
                    subIterator: subIterator, idleHeartbeat: idleHeartbeat)
                {
                case .message(let received):
                    message = received
                case .subscriptionEnded:
                    continuation.finish()
                    return
                case .missedHeartbeat:
                    continuation.finish(throwing: JetStreamError.FetchError.noHeartbeatReceived)
                    return
                }
                switch try handle(message, ctx: ctx) {
                case .deliver(let jsMessage):
                    remaining -= 1
                    continuation.yield(jsMessage)
                case .skip:
                    continue
                case .end:
                    continuation.finish()
                    return
                }
            }
            continuation.finish()
        } catch {
            continuation.finish(throwing: error)
        }
    }

    private static func readNextMessage(
        subIterator: NatsSubscription.AsyncIterator, idleHeartbeat: TimeInterval?
    ) async throws -> ReadOutcome {
        guard let heartbeatNanos = idleHeartbeat.map({ nanoseconds($0 * 2) }) else {
            if let message = try await subIterator.next() {
                return .message(message)
            }
            return .subscriptionEnded
        }
        return try await withThrowingTaskGroup(of: ReadOutcome.self) { group in
            group.addTask {
                if let message = try await subIterator.next() {
                    return .message(message)
                }
                return .subscriptionEnded
            }
            group.addTask {
                try await Task.sleep(nanoseconds: heartbeatNanos)
                return .missedHeartbeat
            }
            defer { group.cancelAll() }
            return try await group.next() ?? .subscriptionEnded
        }
    }

    private static func handle(
        _ message: NatsMessage, ctx: JetStreamContext
    ) throws
        -> MessageOutcome
    {
        switch message.status ?? .ok {
        case .ok:
            return .deliver(JetStreamMessage(message: message, client: ctx.client))
        case .idleHeartbeat:
            return .skip
        case .timeout, .notFound:
            return .end
        case .badRequest:
            throw JetStreamError.FetchError.badRequest
        case .noResponders:
            throw JetStreamError.FetchError.noResponders
        case .requestTerminated:
            guard let description = message.description else {
                throw JetStreamError.FetchError.invalidResponse
            }
            let descLower = description.lowercased()
            if descLower.contains("leadership changed") {
                throw JetStreamError.FetchError.leadershipChanged
            } else if descLower.contains("consumer deleted") {
                throw JetStreamError.FetchError.consumerDeleted
            } else if descLower.contains("consumer is push based") {
                throw JetStreamError.FetchError.consumerIsPush
            }
            return .end
        default:
            throw JetStreamError.FetchError.unknownStatus(
                message.status ?? .ok, message.description)
        }
    }

    private static func nanoseconds(_ seconds: TimeInterval) -> UInt64 {
        let ns = (seconds * 1_000_000_000).rounded()
        guard ns > 0 else { return 0 }
        return ns >= Double(UInt64.max) ? .max : UInt64(ns)
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
