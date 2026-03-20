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
import NIOConcurrencyHelpers
import Nats

/// Configuration for ``Consumer/consume(config:errorHandler:)``.
public struct ConsumeConfig: Sendable {
    /// Max messages buffered in client. Default: 500.
    /// Mutually exclusive with ``maxBytes``.
    public var maxMessages: Int?

    /// Max bytes buffered in client. Default: not set (use ``maxMessages``).
    /// Mutually exclusive with ``maxMessages``. When set, ``maxMessages`` is
    /// internally overridden to 1,000,000 to bypass server batch_size == 0
    /// behavior.
    public var maxBytes: Int?

    /// Timeout per pull request. Default: 30s. Minimum: 1s.
    public var expires: TimeInterval

    /// Idle heartbeat interval.
    /// Default: ``expires`` / 2, capped at 30s, minimum 500ms.
    public var idleHeartbeat: TimeInterval?

    /// Message count threshold to trigger re-pull. Default: 50% of
    /// ``maxMessages``.
    public var thresholdMessages: Int?

    /// Byte count threshold to trigger re-pull. Default: 50% of ``maxBytes``.
    public var thresholdBytes: Int?

    public init(
        maxMessages: Int? = nil,
        maxBytes: Int? = nil,
        expires: TimeInterval = 30,
        idleHeartbeat: TimeInterval? = nil,
        thresholdMessages: Int? = nil,
        thresholdBytes: Int? = nil
    ) {
        self.maxMessages = maxMessages
        self.maxBytes = maxBytes
        self.expires = expires
        self.idleHeartbeat = idleHeartbeat
        self.thresholdMessages = thresholdMessages
        self.thresholdBytes = thresholdBytes
    }
}

/// Extension to ``Consumer`` adding continuous pull (consume) capability.
extension Consumer {

    /// Continuously retrieves messages from the server, maintaining a buffer
    /// that refills automatically when a threshold is reached.
    ///
    /// Returns a ``ConsumerMessages`` implementing ``AsyncSequence`` allowing
    /// iteration over messages using `for try await msg in messages`.
    ///
    /// - Parameters:
    ///   - config: configuration controlling buffer sizes, expiry, heartbeat
    ///     and thresholds. Defaults to ``ConsumeConfig()``.
    ///   - errorHandler: optional callback invoked for non-terminal warnings
    ///     (missed heartbeats, exceeded server limits). Terminal errors are
    ///     thrown from the iterator.
    ///
    /// - Returns: ``ConsumerMessages`` which implements ``AsyncSequence``.
    ///
    /// - Throws:
    ///   - ``JetStreamError/ConsumeError`` for terminal errors during setup.
    public func consume(
        config: ConsumeConfig = ConsumeConfig(),
        errorHandler: (@Sendable (Error) -> Void)? = nil
    ) async throws -> ConsumerMessages {
        // Validate config
        if config.maxMessages != nil && config.maxBytes != nil {
            throw JetStreamError.ConsumeError.invalidConfig(
                "only one of maxMessages and maxBytes can be specified")
        }
        if let maxMessages = config.maxMessages, maxMessages < 1 {
            throw JetStreamError.ConsumeError.invalidConfig(
                "maxMessages must be at least 1")
        }
        if let maxBytes = config.maxBytes, maxBytes < 1 {
            throw JetStreamError.ConsumeError.invalidConfig(
                "maxBytes must be greater than 0")
        }
        if config.expires < 1.0 {
            throw JetStreamError.ConsumeError.invalidConfig(
                "expires value must be at least 1s")
        }
        if let hb = config.idleHeartbeat {
            if hb < 0.5 || hb > 30.0 {
                throw JetStreamError.ConsumeError.invalidConfig(
                    "idleHeartbeat value must be within 500ms-30s range")
            }
        }

        // Compute effective values
        let expires = config.expires

        let useBytes = config.maxBytes != nil
        let effectiveMaxMessages: Int
        let effectiveMaxBytes: Int?
        if let maxBytes = config.maxBytes {
            effectiveMaxMessages = 1_000_000
            effectiveMaxBytes = maxBytes
        } else {
            effectiveMaxMessages = config.maxMessages ?? 500
            effectiveMaxBytes = nil
        }

        let effectiveThresholdMessages =
            config.thresholdMessages ?? (effectiveMaxMessages / 2)
        let effectiveThresholdBytes: Int?
        if let maxBytes = effectiveMaxBytes {
            effectiveThresholdBytes = config.thresholdBytes ?? (maxBytes / 2)
        } else {
            effectiveThresholdBytes = nil
        }

        // Compute idle heartbeat: default expires/2, capped at 30s
        let effectiveHeartbeat: TimeInterval
        if let hb = config.idleHeartbeat {
            effectiveHeartbeat = hb
        } else {
            effectiveHeartbeat = min(expires / 2.0, 30.0)
        }
        if effectiveHeartbeat > expires / 2.0 {
            throw JetStreamError.ConsumeError.invalidConfig(
                "idleHeartbeat must be less than 50% of expires")
        }

        let pullSubject = ctx.apiSubject(
            "CONSUMER.MSG.NEXT.\(info.stream).\(info.name)")
        let inbox = ctx.client.newInbox()
        let sub = try await ctx.client.subscribe(subject: inbox)

        let messages = ConsumerMessages(
            ctx: ctx,
            sub: sub,
            pullSubject: pullSubject,
            inbox: inbox,
            maxMessages: effectiveMaxMessages,
            maxBytes: effectiveMaxBytes,
            expires: expires,
            idleHeartbeat: effectiveHeartbeat,
            thresholdMessages: effectiveThresholdMessages,
            thresholdBytes: effectiveThresholdBytes,
            useBytes: useBytes,
            errorHandler: errorHandler
        )

        // Send initial pull request
        try await messages.sendPullRequest()

        // Start heartbeat monitoring
        messages.startHeartbeatTimer()

        // Register for connection events
        messages.registerEventListeners()

        return messages
    }
}

/// Returned by ``Consumer/consume(config:errorHandler:)``.
///
/// Implements ``AsyncSequence`` to allow iteration over messages using
/// `for try await msg in messages`.
///
/// Use ``stop()`` to discard buffered messages and end iteration, or
/// ``drain()`` to deliver remaining buffered messages before ending.
public final class ConsumerMessages: AsyncSequence, Sendable {
    public typealias Element = JetStreamMessage
    public typealias AsyncIterator = ConsumerIterator

    internal let ctx: JetStreamContext
    internal let sub: NatsSubscription
    internal let pullSubject: String
    internal let inbox: String
    internal let maxMessages: Int
    internal let maxBytes: Int?
    internal let expires: TimeInterval
    internal let idleHeartbeat: TimeInterval
    internal let thresholdMessages: Int
    internal let thresholdBytes: Int?
    internal let useBytes: Bool
    internal let errorHandler: (@Sendable (Error) -> Void)?

    internal struct State: Sendable {
        var pendingMessages: Int = 0
        var pendingBytes: Int = 0
        var stopped: Bool = false
        var draining: Bool = false
        var disconnected: Bool = false
        var lastMessageNanos: UInt64 = DispatchTime.now().uptimeNanoseconds
    }

    internal let state: NIOLockedValueBox<State>
    private let _heartbeatTask = NIOLockedValueBox<Task<Void, Never>?>(nil)
    private let _eventListenerId = NIOLockedValueBox<String?>(nil)

    internal init(
        ctx: JetStreamContext,
        sub: NatsSubscription,
        pullSubject: String,
        inbox: String,
        maxMessages: Int,
        maxBytes: Int?,
        expires: TimeInterval,
        idleHeartbeat: TimeInterval,
        thresholdMessages: Int,
        thresholdBytes: Int?,
        useBytes: Bool,
        errorHandler: (@Sendable (Error) -> Void)?
    ) {
        self.ctx = ctx
        self.sub = sub
        self.pullSubject = pullSubject
        self.inbox = inbox
        self.maxMessages = maxMessages
        self.maxBytes = maxBytes
        self.expires = expires
        self.idleHeartbeat = idleHeartbeat
        self.thresholdMessages = thresholdMessages
        self.thresholdBytes = thresholdBytes
        self.useBytes = useBytes
        self.errorHandler = errorHandler
        self.state = NIOLockedValueBox(State())
    }

    public func makeAsyncIterator() -> ConsumerIterator {
        return ConsumerIterator(messages: self)
    }

    /// Stop consuming. Discards buffered messages. The iterator will return
    /// `nil` on the next call to `next()`.
    public func stop() {
        state.withLockedValue { $0.stopped = true }
        cleanup()
    }

    /// Stop fetching new messages but deliver all remaining buffered messages
    /// before ending iteration.
    public func drain() {
        state.withLockedValue { $0.draining = true }
        cancelHeartbeatTimer()
    }

    // MARK: - Internal helpers

    private let jsonEncoder = JSONEncoder()

    internal func sendPullRequest() async throws {
        guard let data = try preparePullRequest() else { return }
        try await ctx.client.publish(data, subject: pullSubject, reply: inbox)
    }

    private func preparePullRequest() throws -> Data? {
        let (batch, maxBytesReq) = state.withLockedValue { state -> (Int, Int?) in
            let batch: Int
            let maxBytesReq: Int?
            if useBytes, let maxBytes = self.maxBytes {
                batch = self.maxMessages - state.pendingMessages
                let remainingBytes = maxBytes - state.pendingBytes
                maxBytesReq = Swift.max(remainingBytes, 0)
            } else {
                batch = self.maxMessages - state.pendingMessages
                maxBytesReq = nil
            }
            // Update pending counts
            state.pendingMessages += batch
            if let maxBytesReq {
                state.pendingBytes += maxBytesReq
            }
            return (batch, maxBytesReq)
        }

        guard batch > 0 else { return nil }

        let request = PullRequest(
            batch: batch,
            expires: NanoTimeInterval(expires),
            maxBytes: maxBytesReq,
            heartbeat: NanoTimeInterval(idleHeartbeat)
        )

        return try jsonEncoder.encode(request)
    }

    internal func startHeartbeatTimer() {
        let task = Task { [weak self] in
            guard let self else { return }
            let heartbeatTimeoutNanos = UInt64(self.idleHeartbeat * 2 * 1_000_000_000)
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: heartbeatTimeoutNanos)
                if Task.isCancelled { break }

                let elapsedNanos = self.state.withLockedValue { state -> UInt64 in
                    DispatchTime.now().uptimeNanoseconds - state.lastMessageNanos
                }

                if elapsedNanos >= heartbeatTimeoutNanos {
                    self.errorHandler?(
                        JetStreamError.ConsumeWarning.missedHeartbeat)
                    // Reset pending counts and re-pull
                    self.state.withLockedValue { state in
                        state.pendingMessages = 0
                        state.pendingBytes = 0
                    }
                    try? await self.sendPullRequest()
                }
            }
        }
        _heartbeatTask.withLockedValue { $0 = task }
    }

    internal func cancelHeartbeatTimer() {
        _heartbeatTask.withLockedValue { task in
            task?.cancel()
            task = nil
        }
    }

    internal func resetHeartbeatTime() {
        state.withLockedValue { $0.lastMessageNanos = DispatchTime.now().uptimeNanoseconds }
    }

    internal func registerEventListeners() {
        let listenerId = ctx.client.on([.connected, .disconnected]) {
            [weak self] event in
            guard let self else { return }
            switch event {
            case .disconnected:
                self.state.withLockedValue { $0.disconnected = true }
                self.cancelHeartbeatTimer()
            case .connected:
                let wasDraining = self.state.withLockedValue { state -> Bool in
                    state.disconnected = false
                    if !state.draining && !state.stopped {
                        state.pendingMessages = 0
                        state.pendingBytes = 0
                    }
                    return state.draining || state.stopped
                }
                if !wasDraining {
                    self.resetHeartbeatTime()
                    self.startHeartbeatTimer()
                    Task {
                        try? await self.sendPullRequest()
                    }
                }
            default:
                break
            }
        }
        _eventListenerId.withLockedValue { $0 = listenerId }
    }

    private func cleanup() {
        cancelHeartbeatTimer()
        if let listenerId = _eventListenerId.withLockedValue({
            let id = $0
            $0 = nil
            return id
        }) {
            ctx.client.off(listenerId)
        }
        Task {
            try? await sub.unsubscribe()
        }
    }

    /// Returns true if consuming is active (not stopped, draining, or
    /// disconnected) and a new pull request should be sent.
    internal func shouldRePull() -> Bool {
        return state.withLockedValue { state in
            !state.stopped && !state.draining && !state.disconnected
        }
    }

    /// Calculate message size: subject + reply + headers + payload.
    internal static func messageSize(_ message: NatsMessage) -> Int {
        return message.subject.utf8.count
            + (message.replySubject?.utf8.count ?? 0)
            + message.length
    }
}

/// Iterator for ``ConsumerMessages``.
///
/// Implements the ADR-37 message processing algorithm for continuous pull.
public struct ConsumerIterator: AsyncIteratorProtocol, Sendable {
    private let messages: ConsumerMessages
    private var subIterator: NatsSubscription.AsyncIterator

    internal init(messages: ConsumerMessages) {
        self.messages = messages
        self.subIterator = messages.sub.makeAsyncIterator()
    }

    /// Deferred pull flag — set in the post-message lock, checked at the
    /// top of the next iteration before awaiting.
    private var needsPull: Bool = false

    public mutating func next() async throws -> JetStreamMessage? {
        while true {
            // Send deferred pull request *before* awaiting the next
            // message so the server can start filling while we block.
            if needsPull {
                needsPull = false
                try await messages.sendPullRequest()
            }

            // Await next message from subscription.
            // If stop() was called, the subscription is closed and
            // this returns nil immediately.
            guard let message = try await subIterator.next() else {
                return nil
            }

            let status = message.status ?? .ok

            switch status {
            case .idleHeartbeat:
                messages.resetHeartbeatTime()
                continue

            case .ok:
                // One lock per message: update pending, heartbeat, check
                // threshold and drain. Decide on pull/drain/stopped atomically
                // and set needsPull for the next iteration to avoid doing async
                // work inside the lock.
                enum Action {
                    case deliver
                    case deliverAndPull
                    case drainComplete
                    case stopped
                }
                let msgSize =
                    messages.useBytes
                    ? ConsumerMessages.messageSize(message) : 0
                let action: Action = messages.state.withLockedValue {
                    state -> Action in
                    state.pendingMessages = max(state.pendingMessages - 1, 0)
                    state.pendingBytes = max(state.pendingBytes - msgSize, 0)
                    state.lastMessageNanos =
                        DispatchTime.now().uptimeNanoseconds

                    if state.stopped {
                        return .stopped
                    }
                    if state.draining && state.pendingMessages <= 0 {
                        return .drainComplete
                    }
                    if state.draining || state.disconnected {
                        return .deliver
                    }
                    if messages.useBytes,
                        let tb = messages.thresholdBytes
                    {
                        return state.pendingBytes <= tb
                            ? .deliverAndPull : .deliver
                    }
                    return state.pendingMessages
                        <= messages.thresholdMessages
                        ? .deliverAndPull : .deliver
                }

                switch action {
                case .stopped:
                    return nil
                case .deliver:
                    return JetStreamMessage(
                        message: message, client: messages.ctx.client)
                case .deliverAndPull:
                    needsPull = true
                    return JetStreamMessage(
                        message: message, client: messages.ctx.client)
                case .drainComplete:
                    let jsMsg = JetStreamMessage(
                        message: message, client: messages.ctx.client)
                    messages.stop()
                    return jsMsg
                }

            case .timeout, .notFound, .wrongPinId:
                // Pull request is done — adjust pending and re-pull.
                adjustPendingAndResetHeartbeat(message)
                if checkDrainComplete() {
                    return nil
                }
                if messages.shouldRePull() {
                    needsPull = true
                }
                continue

            case .badRequest:
                messages.stop()
                throw JetStreamError.ConsumeError.badRequest

            case .noResponders:
                messages.stop()
                throw JetStreamError.ConsumeError.noResponders

            case .requestTerminated:
                adjustPendingAndResetHeartbeat(message)

                guard let description = message.description else {
                    messages.stop()
                    throw JetStreamError.ConsumeError.unknownStatus(
                        status, nil)
                }

                let descLower = description.lowercased()

                if descLower.contains("consumer deleted") {
                    messages.stop()
                    throw JetStreamError.ConsumeError.consumerDeleted
                } else if descLower.contains("consumer is push based") {
                    messages.stop()
                    throw JetStreamError.ConsumeError.consumerIsPush
                } else if descLower.contains(
                    "message size exceeds maxbytes")
                {
                    if checkDrainComplete() { return nil }
                    if messages.shouldRePull() {
                        needsPull = true
                    }
                    continue
                } else if descLower.contains("interest expired") {
                    if checkDrainComplete() { return nil }
                    if messages.shouldRePull() {
                        needsPull = true
                    }
                    continue
                } else if descLower.contains("exceeded maxrequestbatch") {
                    messages.errorHandler?(
                        JetStreamError.ConsumeWarning
                            .exceededMaxRequestBatch(description))
                    continue
                } else if descLower.contains("exceeded maxrequestexpires") {
                    messages.errorHandler?(
                        JetStreamError.ConsumeWarning
                            .exceededMaxRequestExpires(description))
                    continue
                } else if descLower.contains("exceeded maxrequestmaxbytes") {
                    messages.errorHandler?(
                        JetStreamError.ConsumeWarning
                            .exceededMaxRequestMaxBytes(description))
                    continue
                } else if descLower.contains("exceeded maxwaiting") {
                    messages.errorHandler?(
                        JetStreamError.ConsumeWarning.exceededMaxWaiting(
                            description))
                    continue
                }

                // Unknown 409 - not terminal, just warn
                messages.errorHandler?(
                    JetStreamError.ConsumeError.unknownStatus(
                        status, description))
                continue

            default:
                adjustPendingAndResetHeartbeat(message)
                messages.stop()
                throw JetStreamError.ConsumeError.unknownStatus(
                    status, message.description)
            }
        }
    }

    /// Check if drain is complete: draining is set and no more messages
    /// are expected from the server.
    private func checkDrainComplete() -> Bool {
        let done = messages.state.withLockedValue { state -> Bool in
            guard state.draining else { return false }
            return state.pendingMessages <= 0
        }
        if done {
            messages.stop()
        }
        return done
    }

    private func adjustPendingAndResetHeartbeat(_ message: NatsMessage) {
        let headers = message.headers
        let pendingMsgs = headers?.get(.natsPendingMessages)
        let pendingBytes = headers?.get(.natsPendingBytes)
        messages.state.withLockedValue { state in
            state.lastMessageNanos = DispatchTime.now().uptimeNanoseconds
            if let pendingMsgs,
                let count = Int(pendingMsgs.description)
            {
                state.pendingMessages = max(state.pendingMessages - count, 0)
            }
            if let pendingBytes,
                let bytes = Int(pendingBytes.description)
            {
                state.pendingBytes = max(state.pendingBytes - bytes, 0)
            }
        }
    }
}
