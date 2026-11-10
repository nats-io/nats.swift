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

extension JetStreamContext {

    /// Creates a push consumer, which delivers messages to ``ConsumerConfig/deliverSubject``.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer will be created on
    ///  - cfg: consumer config, ``ConsumerConfig/deliverSubject`` is required
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError/notPushConsumer``: if the deliver subject is not set.
    /// > - ``JetStreamError/ConsumerError``: if there was an error creating the consumer.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    public func createPushConsumer(stream: String, cfg: ConsumerConfig) async throws -> PushConsumer
    {
        try await upsertPushConsumer(stream: stream, cfg: cfg, action: "create")
    }

    /// Updates an existing push consumer. See ``createPushConsumer(stream:cfg:)``.
    public func updatePushConsumer(stream: String, cfg: ConsumerConfig) async throws -> PushConsumer
    {
        try await upsertPushConsumer(stream: stream, cfg: cfg, action: "update")
    }

    /// Creates a push consumer or updates it if it exists. See ``createPushConsumer(stream:cfg:)``.
    public func createOrUpdatePushConsumer(
        stream: String, cfg: ConsumerConfig
    ) async throws -> PushConsumer {
        try await upsertPushConsumer(stream: stream, cfg: cfg, action: nil)
    }

    /// Retrieves a push consumer by its name, or nil if it does not exist.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError/notPushConsumer``: if the consumer is a pull consumer.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    public func pushConsumer(stream: String, name: String) async throws -> PushConsumer? {
        guard let consumer = try await getConsumer(stream: stream, name: name) else {
            return nil
        }
        guard consumer.info.config.deliverSubject != nil else {
            throw JetStreamError.ConsumerError.notPushConsumer
        }
        return PushConsumer(ctx: self, info: consumer.info)
    }

    private func upsertPushConsumer(
        stream: String, cfg: ConsumerConfig, action: String?
    ) async throws -> PushConsumer {
        try Stream.validate(name: stream)
        guard cfg.deliverSubject != nil else {
            throw JetStreamError.ConsumerError.notPushConsumer
        }
        let consumer = try await upsertConsumer(stream: stream, cfg: cfg, action: action)
        return PushConsumer(ctx: self, info: consumer.info)
    }
}

extension Stream {

    /// Creates a push consumer on the stream. See ``JetStreamContext/createPushConsumer(stream:cfg:)``.
    public func createPushConsumer(cfg: ConsumerConfig) async throws -> PushConsumer {
        try await ctx.createPushConsumer(stream: info.config.name, cfg: cfg)
    }

    /// Creates or updates a push consumer on the stream.
    /// See ``JetStreamContext/createOrUpdatePushConsumer(stream:cfg:)``.
    public func createOrUpdatePushConsumer(cfg: ConsumerConfig) async throws -> PushConsumer {
        try await ctx.createOrUpdatePushConsumer(stream: info.config.name, cfg: cfg)
    }

    /// Retrieves a push consumer by its name. See ``JetStreamContext/pushConsumer(stream:name:)``.
    public func pushConsumer(name: String) async throws -> PushConsumer? {
        try await ctx.pushConsumer(stream: info.config.name, name: name)
    }
}

/// A consumer which delivers messages to a subject (``ConsumerConfig/deliverSubject``).
public final class PushConsumer: @unchecked Sendable {
    /// Contains information about the consumer.
    /// Note that this may be out of date and reading it does not query the server.
    public internal(set) var info: ConsumerInfo {
        get { cachedInfo.value }
        set { cachedInfo.value = newValue }
    }
    private let cachedInfo: Locked<ConsumerInfo>
    internal let ctx: JetStreamContext
    // guards `consuming`
    private let lock = NSLock()
    private var consuming = false

    init(ctx: JetStreamContext, info: ConsumerInfo) {
        self.ctx = ctx
        self.cachedInfo = Locked(info)
    }

    /// Retrieves information about the consumer, refreshing ``PushConsumer/info``.
    public func info() async throws -> ConsumerInfo {
        let subj = "CONSUMER.INFO.\(info.stream).\(info.name)"
        let info: Response<ConsumerInfo> = try await ctx.request(subj)
        switch info {
        case .success(let info):
            self.info = info
            return info
        case .error(let apiResponse):
            throw apiResponse.error
        }
    }

    /// Subscribes to the deliver subject (as a queue subscription if ``ConsumerConfig/deliverGroup`` is set),
    /// answering flow control requests and monitoring idle heartbeats.
    ///
    ///  - Parameter errorHandler: called with recoverable errors, after which consuming continues.
    ///  The server stops sending messages and heartbeats to deleted push consumers, so if
    ///  ``ConsumerConfig/idleHeartbeat`` is set this reports ``JetStreamError/FetchError/noHeartbeatReceived``.
    ///
    ///  - Returns: ``PushConsumeResult`` which implements `AsyncSequence` allowing iteration over messages.
    ///
    ///  - Throws:
    ///   - ``JetStreamError/ConsumerError/alreadyConsuming`` if the consumer is already being consumed.
    ///   - ``JetStreamError/FetchError/consumerDeleted`` (while iterating) if the consumer was deleted.
    public func consume(
        errorHandler: (@Sendable (Error) -> Void)? = nil
    ) async throws -> PushConsumeResult {
        try lock.locked {
            guard !consuming else {
                throw JetStreamError.ConsumerError.alreadyConsuming
            }
            consuming = true
        }
        do {
            let sub = try await ctx.client.subscribe(
                subject: info.config.deliverSubject!, queue: info.config.deliverGroup)
            return PushConsumeResult(consumer: self, sub: sub, errorHandler: errorHandler)
        } catch {
            release()
            throw error
        }
    }

    fileprivate func release() {
        lock.locked { consuming = false }
    }
}

/// Used to iterate over results of ``PushConsumer/consume(errorHandler:)``.
/// Iteration ends when the iterating task is cancelled or ``stop()`` is called.
public final class PushConsumeResult: AsyncSequence, Sendable {
    public typealias Element = JetStreamMessage

    private let sub: NatsSubscription
    private let stream: AsyncThrowingStream<JetStreamMessage, Error>

    init(
        consumer: PushConsumer, sub: NatsSubscription, errorHandler: (@Sendable (Error) -> Void)?
    ) {
        self.sub = sub
        let client = consumer.ctx.client
        let monitor = PushMonitor(heartbeat: consumer.info.config.idleHeartbeat?.value)
        self.stream = AsyncThrowingStream { continuation in
            let reader = Task {
                do {
                    for try await message in sub {
                        await monitor.activity()
                        if let jsMessage = try await Self.handle(
                            message, client: client, errorHandler: errorHandler)
                        {
                            continuation.yield(jsMessage)
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            let watchdog = Task { await monitor.run(errorHandler: errorHandler) }
            let listener = client.on([.connected, .disconnected]) { event in
                Task { await monitor.connectionChanged(event) }
            }
            continuation.onTermination = { _ in
                reader.cancel()
                watchdog.cancel()
                client.off(listener)
                consumer.release()
                Task { try? await sub.unsubscribe() }
            }
        }
    }

    /// Stops consuming. Messages which were already received are still delivered.
    public func stop() async {
        try? await sub.unsubscribe()
    }

    public func makeAsyncIterator() -> AsyncThrowingStream<JetStreamMessage, Error>.Iterator {
        stream.makeAsyncIterator()
    }

    private static func handle(
        _ message: NatsMessage, client: NatsClient, errorHandler: (@Sendable (Error) -> Void)?
    ) async throws -> JetStreamMessage? {
        switch message.status ?? .ok {
        case .ok:
            return JetStreamMessage(message: message, client: client)
        case .idleHeartbeat:
            // flow control requests and stalled consumers expect an empty reply
            let reply =
                message.description?.lowercased() == "flowcontrol request"
                ? message.replySubject : message.headers?[.natsConsumerStalled]?.description
            if let reply {
                try await client.publish(Data(), subject: reply)
            }
            return nil
        case .requestTerminated:
            let description = message.description?.lowercased() ?? ""
            if description.contains("consumer deleted") {
                throw JetStreamError.FetchError.consumerDeleted
            }
            if description.contains("leadership change") {
                errorHandler?(JetStreamError.FetchError.leadershipChanged)
            }
            return nil
        default:
            errorHandler?(
                JetStreamError.FetchError.unknownStatus(message.status ?? .ok, message.description))
            return nil
        }
    }
}

private actor PushMonitor {
    private let heartbeat: TimeInterval?
    private var lastActivity = ProcessInfo.processInfo.systemUptime
    private var connected = true

    init(heartbeat: TimeInterval?) {
        self.heartbeat = heartbeat
    }

    func activity() {
        lastActivity = ProcessInfo.processInfo.systemUptime
    }

    func connectionChanged(_ event: NatsEvent) {
        switch event {
        case .disconnected:
            connected = false
        case .connected:
            connected = true
            lastActivity = ProcessInfo.processInfo.systemUptime
        default:
            break
        }
    }

    func run(errorHandler: (@Sendable (Error) -> Void)?) async {
        guard let heartbeat else {
            return
        }
        while !Task.isCancelled {
            try? await Task.sleep(nanoseconds: UInt64(heartbeat * 1_000_000_000))
            let idle = ProcessInfo.processInfo.systemUptime - lastActivity
            if !Task.isCancelled, connected, idle >= heartbeat * 2 {
                errorHandler?(JetStreamError.FetchError.noHeartbeatReceived)
                lastActivity = ProcessInfo.processInfo.systemUptime
            }
        }
    }
}
