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

/// Tracks publishes awaiting acknowledgements from JetStream.
/// All acknowledgements are received on a single wildcard subscription, `<inbox>.*`,
/// each publish using its own token as the last subject token.
internal actor AsyncPublisher {
    private let client: NatsClient
    private let prefix: String
    private var sub: NatsSubscription?
    private var reader: Task<Void, Never>?
    private var pending: [String: PendingAck] = [:]
    private var completionWaiters: [CheckedContinuation<Void, Never>] = []

    /// Maximum number of publishes awaiting acknowledgements.
    var maxPending = 4000
    /// How long a publish waits for room when ``maxPending`` is reached.
    var stallWait: TimeInterval = 0.2

    init(client: NatsClient) {
        self.client = client
        self.prefix = client.newInbox() + "."
    }

    var pendingCount: Int { pending.count }

    func configure(maxPending: Int, stallWait: TimeInterval) {
        self.maxPending = maxPending
        self.stallWait = stallWait
    }

    /// Registers a new publish, returning its reply subject and pending acknowledgement.
    func register() async throws -> (reply: String, ack: PendingAck) {
        try await subscribeIfNeeded()
        if pending.count >= maxPending {
            let deadline = ProcessInfo.processInfo.systemUptime + stallWait
            while pending.count >= maxPending {
                guard ProcessInfo.processInfo.systemUptime < deadline else {
                    throw JetStreamError.PublishError.tooManyStalledMessages
                }
                try await Task.sleep(nanoseconds: 5_000_000)
            }
        }
        let token = nextNuid()
        let ack = PendingAck()
        pending[token] = ack
        return (prefix + token, ack)
    }

    /// Re-registers a publish being retried, under the same reply subject.
    func reregister(_ reply: String, _ ack: PendingAck) {
        ack.reset()
        pending[String(reply.dropFirst(prefix.count))] = ack
    }

    func remove(_ reply: String) {
        pending[String(reply.dropFirst(prefix.count))] = nil
        notifyIfComplete()
    }

    /// Waits until all publishes were acknowledged (or failed).
    func waitForCompletion() async {
        guard !pending.isEmpty else {
            return
        }
        await withCheckedContinuation { completionWaiters.append($0) }
    }

    private func resolve(_ message: NatsMessage) {
        let token = String(message.subject.dropFirst(prefix.count))
        guard let ack = pending.removeValue(forKey: token) else {
            return
        }
        ack.resolve(.success(message))
        notifyIfComplete()
    }

    private func notifyIfComplete() {
        guard pending.isEmpty else {
            return
        }
        let waiters = completionWaiters
        completionWaiters = []
        for waiter in waiters {
            waiter.resume()
        }
    }

    private func subscribeIfNeeded() async throws {
        guard sub == nil else {
            return
        }
        let sub = try await client.subscribe(subject: prefix + "*")
        self.sub = sub
        reader = Task { [weak self] in
            do {
                for try await message in sub {
                    await self?.resolve(message)
                }
            } catch {
                logger.debug("publish acknowledgement subscription ended: \(error)")
            }
        }
    }

    deinit {
        reader?.cancel()
        if let sub {
            Task { try? await sub.unsubscribe() }
        }
    }
}

/// Acknowledgement of a single publish, resolved by ``AsyncPublisher``.
internal final class PendingAck: @unchecked Sendable {
    private let lock = NSLock()
    private var result: Result<NatsMessage, Error>?
    private var waiters: [CheckedContinuation<NatsMessage, Error>] = []

    func resolve(_ result: Result<NatsMessage, Error>) {
        let waiters: [CheckedContinuation<NatsMessage, Error>] = lock.locked {
            guard self.result == nil else {
                return []
            }
            self.result = result
            defer { self.waiters = [] }
            return self.waiters
        }
        for waiter in waiters {
            waiter.resume(with: result)
        }
    }

    func reset() {
        lock.locked { result = nil }
    }

    func value() async throws -> NatsMessage {
        try await withCheckedThrowingContinuation { continuation in
            let result: Result<NatsMessage, Error>? = lock.locked {
                if let result = self.result {
                    return result
                }
                waiters.append(continuation)
                return nil
            }
            if let result {
                continuation.resume(with: result)
            }
        }
    }
}
