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

// TODO(pp): Implement slow consumer
// TODO(pp): Add queue subscribe
public class NatsSubscription: AsyncSequence {
    public typealias Element = NatsMessage
    public typealias AsyncIterator = SubscriptionIterator

    public let subject: String
    internal var max: UInt64?
    internal var delivered: UInt64 = 0
    internal let sid: UInt64

    private var buffer: [Result<Element, NatsError.SubscriptionError>]
    private let capacity: UInt64
    private var closed = false
    private var continuation:
        CheckedContinuation<Result<Element, NatsError.SubscriptionError>?, Never>?
    private let lock = NSLock()
    private let conn: ConnectionHandler

    private static let defaultSubCapacity: UInt64 = 512 * 1024

    convenience init(sid: UInt64, subject: String, conn: ConnectionHandler) {
        self.init(
            sid: sid, subject: subject, capacity: NatsSubscription.defaultSubCapacity, conn: conn)
    }

    init(sid: UInt64, subject: String, capacity: UInt64, conn: ConnectionHandler) {
        self.sid = sid
        self.subject = subject
        self.capacity = capacity
        self.buffer = []
        self.conn = conn
    }

    public func makeAsyncIterator() -> SubscriptionIterator {
        return SubscriptionIterator(subscription: self)
    }

    func receiveMessage(_ message: NatsMessage) {
        lock.withLock {
            if let continuation = self.continuation {
                // Immediately use the continuation if it exists
                self.continuation = nil
                continuation.resume(returning: .success(message))
            } else if buffer.count < capacity {
                // Only append to buffer if no continuation is available
                // TODO(pp): Hadndle SlowConsumer as subscription event
                buffer.append(.success(message))
            }
        }
    }

    func receiveError(_ error: NatsError.SubscriptionError) {
        lock.withLock {
            if let continuation = self.continuation {
                // Immediately use the continuation if it exists
                self.continuation = nil
                continuation.resume(returning: .failure(error))
            } else {
                buffer.append(.failure(error))
            }
        }
    }

    internal func complete() {
        lock.withLock {
            closed = true
            if let continuation {
                self.continuation = nil
                continuation.resume(returning: nil)
            }

        }
    }

    // AsyncIterator implementation
    public class SubscriptionIterator: AsyncIteratorProtocol {
        private var subscription: NatsSubscription

        init(subscription: NatsSubscription) {
            self.subscription = subscription
        }

        public func next() async throws -> Element? {
            try await subscription.nextMessage()
        }
    }

    private func nextMessage() async throws -> Element? {
        let result: Result<Element, NatsError.SubscriptionError>? = await withCheckedContinuation {
            continuation in
            lock.withLock {
                if closed {
                    continuation.resume(returning: nil)
                    return
                }

                delivered += 1
                if let message = buffer.first {
                    buffer.removeFirst()
                    continuation.resume(returning: message)
                } else {
                    self.continuation = continuation
                }
            }
        }
        if let max, delivered >= max {
            conn.removeSub(sub: self)
        }
        switch result {
        case .success(let msg):
            return msg
        case .failure(let error):
            throw error
        default:
            return nil
        }
    }

    /// Unsubscribes from subscription.
    ///
    /// - Parameter after: If set, unsubscribe will be performed after reaching given number of messages.
    ///   If it already reached or surpassed the passed value, it will immediately stop.
    ///
    /// > **Throws:**
    /// > - ``NatsError/ClientError/connectionClosed`` if the conneciton is closed.
    /// > - ``NatsError/SubscriptionError/subscriptionClosed`` if the subscription is already closed
    public func unsubscribe(after: UInt64? = nil) async throws {
        logger.info("unsubscribe from subject \(subject)")
        if case .closed = self.conn.state {
            throw NatsError.ClientError.connectionClosed
        }
        if self.closed {
            throw NatsError.SubscriptionError.subscriptionClosed
        }
        return try await self.conn.unsubscribe(sub: self, max: after)
    }
}
