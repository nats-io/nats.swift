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
    ///   - maxBytes: maximum total size of messages to be retrieved
    ///   - expires: timeout of a pull request
    ///   - idleHeartbeat: interval in which server should send heartbeat messages (if no user messages are available).
    ///   - priorityGroup: priority group the request belongs to, for consumers with a ``PriorityPolicy``.
    ///
    ///  - Returns: ``FetchResult`` which implements `AsyncSequence` allowing iteration over messages.
    ///
    ///  - Throws:
    ///   - ``JetStreamError/FetchError`` if there was an error while fetching messages
    public func fetch(
        batch: Int, maxBytes: Int? = nil, expires: TimeInterval = 30,
        idleHeartbeat: TimeInterval? = nil, priorityGroup: PriorityGroupOptions? = nil
    ) async throws -> FetchResult {
        let request = PullRequest(
            batch: batch, expires: NanoTimeInterval(expires), maxBytes: maxBytes,
            heartbeat: idleHeartbeat.map { NanoTimeInterval($0) },
            priorityGroup: priorityGroup, pinId: pinId.value)
        return try await fetch(request: request, expires: expires, idleHeartbeat: idleHeartbeat)
    }

    /// Retrieves messages from a stream up to a provided total size.
    /// See ``fetch(batch:maxBytes:expires:idleHeartbeat:priorityGroup:)``.
    public func fetchBytes(
        maxBytes: Int, expires: TimeInterval = 30, idleHeartbeat: TimeInterval? = nil,
        priorityGroup: PriorityGroupOptions? = nil
    ) async throws -> FetchResult {
        try await fetch(
            batch: Self.batchMaxBytesOnly, maxBytes: maxBytes, expires: expires,
            idleHeartbeat: idleHeartbeat, priorityGroup: priorityGroup)
    }

    /// Retrieves up to a provided number of messages which are available immediately,
    /// without waiting for new messages.
    /// See ``fetch(batch:maxBytes:expires:idleHeartbeat:priorityGroup:)``.
    public func fetchNoWait(
        batch: Int, maxBytes: Int? = nil, priorityGroup: PriorityGroupOptions? = nil
    ) async throws -> FetchResult {
        let request = PullRequest(
            batch: batch, maxBytes: maxBytes, noWait: true, priorityGroup: priorityGroup,
            pinId: pinId.value)
        return try await fetch(request: request, expires: ctx.timeout, idleHeartbeat: nil)
    }

    /// Retrieves a single message from a stream.
    ///
    ///  - Parameters:
    ///   - expires: timeout of a pull request
    ///   - idleHeartbeat: interval in which server should send heartbeat messages (if no user messages are available).
    ///   - priorityGroup: priority group the request belongs to, for consumers with a ``PriorityPolicy``.
    ///
    ///  - Returns: the message or nil if no message was available before the request expired.
    ///
    ///  - Throws:
    ///   - ``JetStreamError/FetchError`` if there was an error while fetching the message
    public func next(
        expires: TimeInterval = 30, idleHeartbeat: TimeInterval? = nil,
        priorityGroup: PriorityGroupOptions? = nil
    ) async throws -> JetStreamMessage? {
        let result = try await fetch(
            batch: 1, expires: expires, idleHeartbeat: idleHeartbeat, priorityGroup: priorityGroup)
        for try await message in result {
            return message
        }
        return nil
    }

    private static let batchMaxBytesOnly = 1_000_000

    internal func fetch(
        request: PullRequest, expires: TimeInterval, idleHeartbeat: TimeInterval?
    ) async throws -> FetchResult {
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
            ctx: ctx, sub: sub, idleHeartbeat: idleHeartbeat, batch: request.batch,
            expires: expires, pinId: pinId)
    }
}

/// Options of pull requests on consumers with a ``PriorityPolicy``.
public struct PriorityGroupOptions: Sendable {
    /// Name of the priority group, one of ``ConsumerConfig/priorityGroups``.
    public var group: String
    /// Messages are delivered only if the consumer has at least this many pending messages
    /// (``PriorityPolicy/overflow``).
    public var minPending: Int64?
    /// Messages are delivered only if the consumer has at least this many unacknowledged messages
    /// (``PriorityPolicy/overflow``).
    public var minAckPending: Int64?
    /// Priority of the request, lower values are served first (``PriorityPolicy/prioritized``).
    public var priority: UInt8?

    /// Creates a value with the given properties, see the properties of the same name.
    public init(
        group: String, minPending: Int64? = nil, minAckPending: Int64? = nil,
        priority: UInt8? = nil
    ) {
        self.group = group
        self.minPending = minPending
        self.minAckPending = minAckPending
        self.priority = priority
    }
}

/// ID of the client pinned to a priority group, sent with subsequent pull requests.
internal final class PinId: @unchecked Sendable {
    private let lock = NSLock()
    private var id: String?

    var value: String? {
        get { lock.locked { id } }
        set { lock.locked { id = newValue } }
    }
}

/// Used to iterate over results of ``Consumer/fetch(batch:maxBytes:expires:idleHeartbeat:priorityGroup:)``.
public final class FetchResult: AsyncSequence, Sendable {
    public typealias Element = JetStreamMessage
    public typealias AsyncIterator = AsyncThrowingStream<JetStreamMessage, Error>.Iterator

    private let stream: AsyncThrowingStream<JetStreamMessage, Error>

    init(
        ctx: JetStreamContext, sub: NatsSubscription, idleHeartbeat: TimeInterval?, batch: Int,
        expires: TimeInterval, pinId: PinId
    ) {
        self.stream = AsyncThrowingStream { continuation in
            let producerTask = Task {
                let producer = FetchProducer(
                    ctx: ctx, sub: sub, idleHeartbeat: idleHeartbeat, remaining: batch,
                    expires: expires, pinId: pinId)
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
    private let pinId: PinId

    private static let deadlineGrace: TimeInterval = 1

    init(
        ctx: JetStreamContext, sub: NatsSubscription, idleHeartbeat: TimeInterval?, remaining: Int,
        expires: TimeInterval, pinId: PinId
    ) {
        self.ctx = ctx
        self.pinId = pinId
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
            group.addTask { [subIterator, ctx, idleHeartbeat, remaining, pinId] in
                await Self.readLoop(
                    subIterator: subIterator, ctx: ctx, idleHeartbeat: idleHeartbeat,
                    remaining: remaining, pinId: pinId, into: continuation)
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
        idleHeartbeat: TimeInterval?, remaining: Int, pinId: PinId,
        into continuation: Continuation
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
                switch try handle(message, ctx: ctx, pinId: pinId) {
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
        _ message: NatsMessage, ctx: JetStreamContext, pinId: PinId
    ) throws
        -> MessageOutcome
    {
        switch message.status ?? .ok {
        case .ok:
            if let id = message.headers?[.natsPinId] {
                pinId.value = id.description
            }
            return .deliver(JetStreamMessage(message: message, client: ctx.client))
        case .idleHeartbeat:
            return .skip
        case .timeout, .notFound:
            return .end
        case .badRequest:
            throw JetStreamError.FetchError.badRequest
        case .noResponders:
            throw JetStreamError.FetchError.noResponders
        case .pinIdMismatch:
            // another client got pinned, next request will compete for the pin again
            pinId.value = nil
            throw JetStreamError.FetchError.pinIdMismatch
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
    let expires: NanoTimeInterval?
    let maxBytes: Int?
    let noWait: Bool?
    let heartbeat: NanoTimeInterval?
    let minPending: Int64?
    let minAckPending: Int64?
    let pinId: String?
    let group: String?
    let priority: UInt8?

    internal init(
        batch: Int, expires: NanoTimeInterval? = nil, maxBytes: Int? = nil, noWait: Bool? = nil,
        heartbeat: NanoTimeInterval? = nil, priorityGroup: PriorityGroupOptions? = nil,
        pinId: String? = nil
    ) {
        self.batch = batch
        self.expires = expires
        self.maxBytes = maxBytes
        self.noWait = noWait
        self.heartbeat = heartbeat
        self.minPending = priorityGroup?.minPending
        self.minAckPending = priorityGroup?.minAckPending
        self.group = priorityGroup?.group
        self.priority = priorityGroup?.priority
        self.pinId = priorityGroup == nil ? nil : pinId
    }

    enum CodingKeys: String, CodingKey {
        case batch
        case expires
        case maxBytes = "max_bytes"
        case noWait = "no_wait"
        case heartbeat = "idle_heartbeat"
        case minPending = "min_pending"
        case minAckPending = "min_ack_pending"
        case pinId = "id"
        case group
        case priority
    }
}
