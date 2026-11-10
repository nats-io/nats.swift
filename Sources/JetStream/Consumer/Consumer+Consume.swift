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

extension Consumer {

    /// Continuously retrieves messages from a stream.
    /// Pull requests are pipelined: a new request is sent as soon as the number of pending messages
    /// drops below a threshold, so messages are buffered ahead of the iteration.
    /// Missed heartbeats, reconnects and leadership changes are recovered from by sending new pull requests.
    ///
    ///  - Parameters:
    ///   - options: ``ConsumeOptions`` controlling the size of pull requests and heartbeats.
    ///   - errorHandler: called with recoverable errors (e.g. ``JetStreamError/FetchError/noHeartbeatReceived``),
    ///   after which consuming continues.
    ///
    ///  - Returns: ``ConsumeResult`` which implements `AsyncSequence` allowing iteration over messages.
    ///
    ///  - Throws (while iterating):
    ///   - ``JetStreamError/FetchError`` if the consumer was deleted, is push based or the request is invalid.
    public func consume(
        options: ConsumeOptions = ConsumeOptions(),
        errorHandler: (@Sendable (Error) -> Void)? = nil
    ) async throws -> ConsumeResult {
        let inbox = ctx.client.newInbox()
        let sub = try await ctx.client.subscribe(subject: inbox)
        let puller = ConsumePuller(
            consumer: self, options: options, inbox: inbox, errorHandler: errorHandler)
        do {
            try await puller.checkPending()
        } catch {
            try? await sub.unsubscribe()
            throw error
        }
        return ConsumeResult(puller: puller, sub: sub, client: ctx.client)
    }
}

/// Options of ``Consumer/consume(options:errorHandler:)``.
public struct ConsumeOptions: Sendable {
    /// Maximum number of messages buffered (requested but not yet iterated over).
    /// Ignored if ``maxBytes`` is set.
    public var maxMessages: Int
    /// Maximum total size of messages buffered.
    public var maxBytes: Int?
    /// Timeout of each pull request.
    public var expires: TimeInterval
    /// Interval in which the server sends heartbeats, defaults to half of ``expires`` (at most 30 seconds).
    public var idleHeartbeat: TimeInterval?
    /// A new pull request is sent when fewer messages are pending, defaults to half of ``maxMessages``.
    public var thresholdMessages: Int?
    /// A new pull request is sent when fewer bytes are pending, defaults to half of ``maxBytes``.
    public var thresholdBytes: Int?
    /// Priority group of the pull requests, for consumers with a ``PriorityPolicy``.
    public var priorityGroup: PriorityGroupOptions?

    /// Creates a value with the given properties, see the properties of the same name.
    public init(
        maxMessages: Int = 500, maxBytes: Int? = nil, expires: TimeInterval = 30,
        idleHeartbeat: TimeInterval? = nil, thresholdMessages: Int? = nil,
        thresholdBytes: Int? = nil, priorityGroup: PriorityGroupOptions? = nil
    ) {
        self.maxMessages = maxMessages
        self.maxBytes = maxBytes
        self.expires = expires
        self.idleHeartbeat = idleHeartbeat
        self.thresholdMessages = thresholdMessages
        self.thresholdBytes = thresholdBytes
        self.priorityGroup = priorityGroup
    }
}

/// Used to iterate over results of ``Consumer/consume(options:errorHandler:)``.
/// Iteration ends when the iterating task is cancelled, ``stop()`` or ``drain()`` is called
/// or an unrecoverable error occurs.
public final class ConsumeResult: AsyncSequence, Sendable {
    public typealias Element = JetStreamMessage

    private let puller: ConsumePuller
    private let sub: NatsSubscription
    private let stream: AsyncThrowingStream<JetStreamMessage, Error>

    init(puller: ConsumePuller, sub: NatsSubscription, client: NatsClient) {
        self.puller = puller
        self.sub = sub
        self.stream = AsyncThrowingStream { continuation in
            let reader = Task { await puller.read(sub, into: continuation) }
            let monitor = Task { await puller.monitorHeartbeats() }
            let listener = client.on([.connected, .disconnected]) { event in
                Task { await puller.connectionChanged(event) }
            }
            continuation.onTermination = { _ in
                reader.cancel()
                monitor.cancel()
                client.off(listener)
                Task { try? await sub.unsubscribe() }
            }
        }
    }

    /// Stops consuming immediately, discarding buffered messages.
    public func stop() async {
        await puller.stop()
        try? await sub.unsubscribe()
    }

    /// Stops sending pull requests. Messages which were already received are still delivered,
    /// after which the iteration ends.
    public func drain() async {
        await puller.drain()
        try? await sub.unsubscribe()
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: stream.makeAsyncIterator(), puller: puller)
    }

    /// Iterator of the consumed messages.
    public struct AsyncIterator: AsyncIteratorProtocol {
        var base: AsyncThrowingStream<JetStreamMessage, Error>.Iterator
        let puller: ConsumePuller

        public mutating func next() async throws -> JetStreamMessage? {
            guard let message = try await base.next(), await !puller.isStopped else {
                return nil
            }
            await puller.delivered(message)
            return message
        }
    }
}

internal actor ConsumePuller {
    private let consumer: Consumer
    private let inbox: String
    private let errorHandler: (@Sendable (Error) -> Void)?
    private let batch: Int
    private let maxBytes: Int
    private let expires: TimeInterval
    private let heartbeat: TimeInterval
    private let thresholdMessages: Int
    private let thresholdBytes: Int
    private let priorityGroup: PriorityGroupOptions?

    private var pendingMessages = 0
    private var pendingBytes = 0
    private var lastActivity = ProcessInfo.processInfo.systemUptime
    private var connected = true
    private var draining = false
    private(set) var isStopped = false

    private static let batchMaxBytesOnly = 1_000_000

    init(
        consumer: Consumer, options: ConsumeOptions, inbox: String,
        errorHandler: (@Sendable (Error) -> Void)?
    ) {
        self.consumer = consumer
        self.inbox = inbox
        self.errorHandler = errorHandler
        self.batch = options.maxBytes == nil ? options.maxMessages : Self.batchMaxBytesOnly
        self.maxBytes = options.maxBytes ?? 0
        self.expires = options.expires
        self.heartbeat = options.idleHeartbeat ?? min(options.expires / 2, 30)
        self.thresholdMessages =
            options.thresholdMessages ?? Int((Double(options.maxMessages) / 2).rounded(.up))
        self.thresholdBytes =
            options.thresholdBytes ?? Int((Double(options.maxBytes ?? 0) / 2).rounded(.up))
        self.priorityGroup = options.priorityGroup
    }

    /// Sends a new pull request if fewer messages (or bytes) than the threshold are pending.
    func checkPending() async throws {
        guard !draining, !isStopped, connected else {
            return
        }
        // with maxBytes the batch size is only an upper bound, so only bytes are tracked
        let needsPull =
            maxBytes > 0 ? pendingBytes < thresholdBytes : pendingMessages < thresholdMessages
        guard needsPull else {
            return
        }
        let requestBatch = maxBytes > 0 ? batch : batch - pendingMessages
        let requestBytes = maxBytes > 0 ? maxBytes - pendingBytes : nil
        let request = PullRequest(
            batch: requestBatch, expires: NanoTimeInterval(expires), maxBytes: requestBytes,
            heartbeat: NanoTimeInterval(heartbeat), priorityGroup: priorityGroup,
            pinId: consumer.pinId.value)
        let subject = consumer.ctx.apiSubject(
            "CONSUMER.MSG.NEXT.\(consumer.info.stream).\(consumer.info.name)")
        try await consumer.ctx.client.publish(
            JSONEncoder().encode(request), subject: subject, reply: inbox)
        pendingMessages += requestBatch
        pendingBytes += requestBytes ?? 0
    }

    func delivered(_ message: JetStreamMessage) async {
        pendingMessages = max(0, pendingMessages - 1)
        pendingBytes = max(0, pendingBytes - message.size)
        await pullIfNeeded()
    }

    func read(_ sub: NatsSubscription, into continuation: ConsumeContinuation) async {
        do {
            for try await message in sub {
                lastActivity = ProcessInfo.processInfo.systemUptime
                if let jsMessage = try await handle(message) {
                    continuation.yield(jsMessage)
                }
            }
            continuation.finish()
        } catch {
            continuation.finish(throwing: error)
        }
    }

    func monitorHeartbeats() async {
        let interval = heartbeat * 2
        while !Task.isCancelled {
            try? await Task.sleep(nanoseconds: UInt64(heartbeat * 1_000_000_000))
            let idle = ProcessInfo.processInfo.systemUptime - lastActivity
            guard !Task.isCancelled, connected, !draining, idle >= interval else {
                continue
            }
            report(JetStreamError.FetchError.noHeartbeatReceived)
            lastActivity = ProcessInfo.processInfo.systemUptime
            resetPending()
            await pullIfNeeded()
        }
    }

    func connectionChanged(_ event: NatsEvent) async {
        switch event {
        case .disconnected:
            connected = false
        case .connected where !connected:
            // pull requests do not survive a reconnect
            connected = true
            lastActivity = ProcessInfo.processInfo.systemUptime
            resetPending()
            await pullIfNeeded()
        default:
            break
        }
    }

    func drain() {
        draining = true
    }

    func stop() {
        isStopped = true
    }

    private func handle(_ message: NatsMessage) async throws -> JetStreamMessage? {
        switch message.status ?? .ok {
        case .ok:
            if let id = message.headers?[.natsPinId] {
                consumer.pinId.value = id.description
            }
            return JetStreamMessage(message: message, client: consumer.ctx.client)
        case .idleHeartbeat:
            return nil
        case .timeout, .notFound:
            releasePending(message)
        case .badRequest:
            throw JetStreamError.FetchError.badRequest
        case .noResponders:
            // retried after the next missed heartbeat
            report(JetStreamError.FetchError.noResponders)
            resetPending()
            return nil
        case .pinIdMismatch:
            consumer.pinId.value = nil
            report(JetStreamError.FetchError.pinIdMismatch)
            resetPending()
        case .requestTerminated:
            let description = message.description?.lowercased() ?? ""
            if description.contains("consumer deleted") {
                throw JetStreamError.FetchError.consumerDeleted
            } else if description.contains("consumer is push based") {
                throw JetStreamError.FetchError.consumerIsPush
            } else if description.contains("leadership change") {
                report(JetStreamError.FetchError.leadershipChanged)
                resetPending()
            } else if description.contains("maxbytes") || description.contains("batch completed") {
                releasePending(message)
            } else {
                report(
                    JetStreamError.FetchError.unknownStatus(.requestTerminated, message.description)
                )
                resetPending()
                return nil
            }
        default:
            throw JetStreamError.FetchError.unknownStatus(
                message.status ?? .ok, message.description)
        }
        await pullIfNeeded()
        return nil
    }

    /// Removes messages and bytes which will not be delivered by a finished pull request.
    private func releasePending(_ message: NatsMessage) {
        let messagesLeft = message.headers?[.natsPendingMessages].flatMap { Int($0.description) }
        let bytesLeft = message.headers?[.natsPendingBytes].flatMap { Int($0.description) }
        pendingMessages = max(0, pendingMessages - (messagesLeft ?? 0))
        pendingBytes = max(0, pendingBytes - (bytesLeft ?? 0))
    }

    private func resetPending() {
        pendingMessages = 0
        pendingBytes = 0
    }

    private func pullIfNeeded() async {
        do {
            try await checkPending()
        } catch {
            report(error)
        }
    }

    private func report(_ error: Error) {
        errorHandler?(error)
    }
}

internal typealias ConsumeContinuation = AsyncThrowingStream<JetStreamMessage, Error>.Continuation
