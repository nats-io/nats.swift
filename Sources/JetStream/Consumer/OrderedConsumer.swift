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

extension JetStreamContext {

    /// Creates an ordered consumer on a stream. No request is sent to the server until messages are requested.
    ///
    /// - Parameters:
    ///  - stream: name of the stream to consume from
    ///  - config: ``OrderedConsumerConfig``
    public func orderedConsumer(
        stream: String, config: OrderedConsumerConfig = OrderedConsumerConfig()
    ) -> OrderedConsumer {
        OrderedConsumer(ctx: self, stream: stream, config: config)
    }
}

extension Stream {

    /// Creates an ordered consumer on the stream.
    /// See ``JetStreamContext/orderedConsumer(stream:config:)``.
    public func orderedConsumer(
        config: OrderedConsumerConfig = OrderedConsumerConfig()
    ) -> OrderedConsumer {
        ctx.orderedConsumer(stream: info.config.name, config: config)
    }
}

/// Configuration of an ``OrderedConsumer``.
public struct OrderedConsumerConfig: Sendable {
    /// Subjects to filter messages delivered from the stream.
    public var filterSubjects: [String]?
    /// Defines from which point to start delivering messages from the stream.
    public var deliverPolicy: DeliverPolicy
    /// Sequence number from which to start message delivery, used with ``DeliverPolicy/byStartSequence``.
    public var optStartSeq: UInt64?
    /// Time (RFC 3339) from which to start message delivery, used with ``DeliverPolicy/byStartTime``.
    public var optStartTime: String?
    /// Defines the rate at which messages are sent to the consumer.
    public var replayPolicy: ReplayPolicy
    /// Duration after which the server deletes the underlying consumer if it is inactive (5 minutes by default).
    public var inactiveThreshold: NanoTimeInterval?
    /// Indicates whether only headers of messages should be sent (and no payload).
    public var headersOnly: Bool?
    /// Metadata of the underlying consumers.
    public var metadata: [String: String]?
    /// Maximum number of attempts to recreate the underlying consumer, unlimited by default.
    public var maxResetAttempts: Int?

    /// Creates a value with the given properties, see the properties of the same name.
    public init(
        filterSubjects: [String]? = nil, deliverPolicy: DeliverPolicy = .all,
        optStartSeq: UInt64? = nil, optStartTime: String? = nil,
        replayPolicy: ReplayPolicy = .instant, inactiveThreshold: NanoTimeInterval? = nil,
        headersOnly: Bool? = nil, metadata: [String: String]? = nil, maxResetAttempts: Int? = nil
    ) {
        self.filterSubjects = filterSubjects
        self.deliverPolicy = deliverPolicy
        self.optStartSeq = optStartSeq
        self.optStartTime = optStartTime
        self.replayPolicy = replayPolicy
        self.inactiveThreshold = inactiveThreshold
        self.headersOnly = headersOnly
        self.metadata = metadata
        self.maxResetAttempts = maxResetAttempts
    }
}

/// A consumer which delivers messages of a stream in order, without gaps and without acknowledgements.
/// It is backed by ephemeral consumers which are recreated from the last delivered stream sequence
/// whenever a gap is detected, heartbeats are missed or the underlying consumer is lost.
public final class OrderedConsumer: @unchecked Sendable {
    internal let ctx: JetStreamContext
    /// Name of the stream the consumer reads from.
    public let stream: String
    private let config: OrderedConsumerConfig
    private let namePrefix = nextNuid()
    private let lock = NSLock()
    private var serial = 0
    private var streamSeq: UInt64 = 0
    private var deliverSeq: UInt64 = 0
    private var current: Consumer?

    init(ctx: JetStreamContext, stream: String, config: OrderedConsumerConfig) {
        self.ctx = ctx
        self.stream = stream
        self.config = config
    }

    /// Info of the current underlying consumer, nil if no messages were requested yet.
    public var cachedInfo: ConsumerInfo? {
        lock.locked { current?.info }
    }

    /// Retrieves info of the current underlying consumer, nil if no messages were requested yet.
    public func info() async throws -> ConsumerInfo? {
        try await lock.locked { current }?.info()
    }

    /// Continuously retrieves messages in order. See ``Consumer/consume(options:errorHandler:)``.
    public func consume(
        options: ConsumeOptions = ConsumeOptions(),
        errorHandler: (@Sendable (Error) -> Void)? = nil
    ) async throws -> OrderedConsumeResult {
        let result = OrderedConsumeResult(
            ordered: self, options: options, errorHandler: errorHandler)
        try await result.start(retry: false)
        return result
    }

    /// Retrieves up to a provided number of messages in order, continuing after the last delivered message.
    /// See ``Consumer/fetch(batch:maxBytes:expires:idleHeartbeat:priorityGroup:)``.
    public func fetch(
        batch: Int, maxBytes: Int? = nil, expires: TimeInterval = 30,
        idleHeartbeat: TimeInterval? = nil
    ) async throws -> OrderedFetchResult {
        let consumer = try await reset(retry: false)
        let result = try await consumer.fetch(
            batch: batch, maxBytes: maxBytes, expires: expires, idleHeartbeat: idleHeartbeat)
        return OrderedFetchResult(ordered: self, base: result)
    }

    /// Retrieves the next message in order, or nil if none arrived before the request expired.
    public func next(
        expires: TimeInterval = 30, idleHeartbeat: TimeInterval? = nil
    ) async throws -> JetStreamMessage? {
        for try await message in try await fetch(
            batch: 1, expires: expires, idleHeartbeat: idleHeartbeat)
        {
            return message
        }
        return nil
    }

    /// Records a delivered message, returns false if it does not directly follow the previous one.
    internal func advance(_ message: JetStreamMessage) throws -> Bool {
        let meta = try message.metadata()
        return lock.locked {
            guard meta.consumer == current?.info.name, meta.consumerSequence == deliverSeq + 1
            else {
                return false
            }
            deliverSeq = meta.consumerSequence
            streamSeq = meta.streamSequence
            return true
        }
    }

    /// Replaces the underlying consumer with a new one starting after the last delivered message.
    internal func reset(retry: Bool) async throws -> Consumer {
        let (old, cfg) = lock.locked { () -> (Consumer?, ConsumerConfig) in
            let old = current
            current = nil
            serial += 1
            deliverSeq = 0
            return (old, consumerConfig())
        }
        if let old {
            let name = old.info.name
            Task { try? await ctx.deleteConsumer(stream: stream, name: name) }
        }

        var attempt = 0
        var delay: TimeInterval = 1
        while true {
            do {
                let consumer = try await ctx.createOrUpdateConsumer(stream: stream, cfg: cfg)
                lock.locked { current = consumer }
                return consumer
            } catch {
                attempt += 1
                if !retry || Task.isCancelled
                    || config.maxResetAttempts.map({ attempt >= $0 }) ?? false
                {
                    throw error
                }
                try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                delay = min(delay * 2, 10)
            }
        }
    }

    private func consumerConfig() -> ConsumerConfig {
        var cfg = ConsumerConfig(
            name: "\(namePrefix)_\(serial)", deliverPolicy: .byStartSequence,
            optStartSeq: streamSeq + 1, ackPolicy: .none,
            replayPolicy: config.replayPolicy, headersOnly: config.headersOnly,
            inactiveThreshold: config.inactiveThreshold ?? NanoTimeInterval(300),
            replicas: 1, memoryStorage: true, metadata: config.metadata)
        if let filters = config.filterSubjects, filters.count == 1 {
            cfg.filterSubject = filters[0]
        } else {
            cfg.filterSubjects = config.filterSubjects
        }
        // the initial consumer uses the configured deliver policy
        if streamSeq == 0 {
            cfg.deliverPolicy = config.deliverPolicy
            cfg.optStartSeq = config.deliverPolicy == .byStartSequence ? config.optStartSeq : nil
            cfg.optStartTime = config.deliverPolicy == .byStartTime ? config.optStartTime : nil
        }
        return cfg
    }
}

/// Used to iterate over results of ``OrderedConsumer/consume(options:errorHandler:)``.
public final class OrderedConsumeResult: AsyncSequence, @unchecked Sendable {
    public typealias Element = JetStreamMessage

    private let ordered: OrderedConsumer
    private let options: ConsumeOptions
    private let errorHandler: (@Sendable (Error) -> Void)?
    private let lock = NSLock()
    private var inner: ConsumeResult?
    private var stopped = false
    // only accessed by the iterating task; dropping a stream iterator ends the stream
    private var iterator: ConsumeResult.AsyncIterator?

    init(
        ordered: OrderedConsumer, options: ConsumeOptions,
        errorHandler: (@Sendable (Error) -> Void)?
    ) {
        self.ordered = ordered
        self.options = options
        self.errorHandler = errorHandler
    }

    /// Stops consuming and deletes the underlying consumer.
    public func stop() async {
        let inner = lock.locked {
            stopped = true
            return self.inner
        }
        await inner?.stop()
        if let name = ordered.cachedInfo?.name {
            try? await ordered.ctx.deleteConsumer(stream: ordered.stream, name: name)
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(result: self)
    }

    /// Creates a new underlying consumer and starts consuming from it.
    fileprivate func start(retry: Bool) async throws {
        let consumer = try await ordered.reset(retry: retry)
        let handler = errorHandler
        let result = try await consumer.consume(options: options) { [weak self] error in
            handler?(error)
            // the underlying consumer may be gone, recreate it
            if case JetStreamError.FetchError.noHeartbeatReceived = error {
                Task { await self?.restart() }
            } else if case JetStreamError.FetchError.noResponders = error {
                Task { await self?.restart() }
            }
        }
        lock.locked { inner = result }
    }

    /// Ends the current inner iteration, causing the iterator to recreate the consumer.
    private func restart() async {
        await lock.locked { inner }?.stop()
    }

    fileprivate func next() async throws -> JetStreamMessage? {
        while true {
            guard let inner = lock.locked({ stopped ? nil : inner }) else {
                return nil
            }
            if iterator == nil {
                iterator = inner.makeAsyncIterator()
            }
            do {
                if let message = try await iterator?.next() {
                    if try ordered.advance(message) {
                        return message
                    }
                    if try message.metadata().consumer != ordered.cachedInfo?.name {
                        continue  // left over from a previous consumer
                    }
                } else if lock.locked({ stopped }) || Task.isCancelled {
                    return nil
                }
            } catch is CancellationError {
                return nil
            } catch let error as JetStreamError.FetchError {
                errorHandler?(error)
            }
            // gap detected, inner iteration ended or consumer lost
            iterator = nil
            await inner.stop()
            try await start(retry: true)
        }
    }

    /// Iterator of the delivered messages.
    public struct AsyncIterator: AsyncIteratorProtocol {
        let result: OrderedConsumeResult

        public mutating func next() async throws -> JetStreamMessage? {
            try await result.next()
        }
    }
}

/// Used to iterate over results of ``OrderedConsumer/fetch(batch:maxBytes:expires:idleHeartbeat:)``.
public struct OrderedFetchResult: AsyncSequence, Sendable {
    public typealias Element = JetStreamMessage

    let ordered: OrderedConsumer
    let base: FetchResult

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(ordered: ordered, base: base.makeAsyncIterator())
    }

    /// Iterator of the delivered messages.
    public struct AsyncIterator: AsyncIteratorProtocol {
        let ordered: OrderedConsumer
        var base: FetchResult.AsyncIterator

        public mutating func next() async throws -> JetStreamMessage? {
            guard let message = try await base.next(), try ordered.advance(message) else {
                return nil
            }
            return message
        }
    }
}
