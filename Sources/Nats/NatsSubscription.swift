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
import NIOCore
import NIOConcurrencyHelpers

// TODO(pp): Implement slow consumer
public final class NatsSubscription: AsyncSequence, Sendable {
    public typealias Element = NatsMessage
    public typealias AsyncIterator = SubscriptionIterator

    public let subject: String
    public let queue: String?
    private let _max = NIOLockedValueBox<UInt64?>(nil)
    internal var max: UInt64? {
        get { _max.withLockedValue { $0 } }
        set { _max.withLockedValue { $0 = newValue } }
    }
    
    private let _delivered = NIOLockedValueBox<UInt64>(0)
    internal var delivered: UInt64 {
        get { _delivered.withLockedValue { $0 } }
        set { _delivered.withLockedValue { $0 = newValue } }
    }
    internal let sid: UInt64

    private struct State: Sendable {
        var buffer: [Result<NatsMessage, NatsError.SubscriptionError>] = []
        var closed = false
        var continuation: CheckedContinuation<Result<NatsMessage, NatsError.SubscriptionError>?, Never>? = nil
    }
    
    private let state = NIOLockedValueBox(State())
    private let capacity: UInt64
    private let conn: ConnectionHandler

    private static let defaultSubCapacity: UInt64 = 512 * 1024

    convenience init(sid: UInt64, subject: String, queue: String?, conn: ConnectionHandler) throws {
        try self.init(
            sid: sid, subject: subject, queue: queue, capacity: NatsSubscription.defaultSubCapacity,
            conn: conn)
    }

    init(
        sid: UInt64, subject: String, queue: String?, capacity: UInt64, conn: ConnectionHandler
    ) throws {
        if !NatsSubscription.validSubject(subject) {
            throw NatsError.SubscriptionError.invalidSubject
        }
        if let queue, !NatsSubscription.validQueue(queue) {
            throw NatsError.SubscriptionError.invalidQueue
        }
        self.sid = sid
        self.subject = subject
        self.queue = queue
        self.capacity = capacity
        self.conn = conn
    }

    public func makeAsyncIterator() -> SubscriptionIterator {
        return SubscriptionIterator(subscription: self)
    }

    func receiveMessage(_ message: NatsMessage) {
        state.withLockedValue { state in
            if let continuation = state.continuation {
                // Immediately use the continuation if it exists
                state.continuation = nil
                continuation.resume(returning: .success(message))
            } else if state.buffer.count < capacity {
                // Only append to buffer if no continuation is available
                // TODO(pp): Hadndle SlowConsumer as subscription event
                state.buffer.append(.success(message))
            }
        }
    }

    func receiveError(_ error: NatsError.SubscriptionError) {
        state.withLockedValue { state in
            if let continuation = state.continuation {
                // Immediately use the continuation if it exists
                state.continuation = nil
                continuation.resume(returning: .failure(error))
            } else {
                state.buffer.append(.failure(error))
            }
        }
    }

    internal func complete() {
        state.withLockedValue { state in
            state.closed = true
            if let continuation = state.continuation {
                state.continuation = nil
                continuation.resume(returning: nil)
            }
        }
    }

    // AsyncIterator implementation
    public final class SubscriptionIterator: AsyncIteratorProtocol, Sendable {
        private let subscription: NatsSubscription

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
            state.withLockedValue { state in
                if state.closed {
                    continuation.resume(returning: nil)
                    return
                }

                _delivered.withLockedValue { $0 += 1 }
                if let message = state.buffer.first {
                    state.buffer.removeFirst()
                    continuation.resume(returning: message)
                } else {
                    state.continuation = continuation
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
        if case .closed = self.conn.currentState {
            throw NatsError.ClientError.connectionClosed
        }
        let isClosed = state.withLockedValue { $0.closed }
        if isClosed {
            throw NatsError.SubscriptionError.subscriptionClosed
        }
        return try await self.conn.unsubscribe(sub: self, max: after)
    }

    // validateSubject will do a basic subject validation.
    // Spaces are not allowed and all tokens should be > 0 in length.
    private static func validSubject(_ subj: String) -> Bool {
        let whitespaceCharacterSet = CharacterSet.whitespacesAndNewlines
        if subj.rangeOfCharacter(from: whitespaceCharacterSet) != nil {
            return false
        }
        let tokens = subj.split(separator: ".")
        for token in tokens {
            if token.isEmpty {
                return false
            }
        }
        return true
    }

    // validQueue will check a queue name for whitespaces.
    private static func validQueue(_ queue: String) -> Bool {
        let whitespaceCharacterSet = CharacterSet.whitespacesAndNewlines
        return queue.rangeOfCharacter(from: whitespaceCharacterSet) == nil
    }
}
