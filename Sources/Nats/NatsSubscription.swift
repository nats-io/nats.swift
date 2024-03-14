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
public class Subscription: AsyncSequence {
    public typealias Element = NatsMessage
    public typealias AsyncIterator = SubscriptionIterator

    private var buffer: [Element]
    private let maxPending: UInt64
    private var closed = false
    private var continuation: CheckedContinuation<Element?, Never>?
    private let lock = NSLock()
    public let subject: String

    private static let defaultMaxPending: UInt64 = 512 * 1024

    convenience init(subject: String) {
        self.init(subject: subject, maxPending: Subscription.defaultMaxPending)
    }

    init(subject: String, maxPending: UInt64) {
        self.subject = subject
        self.maxPending = maxPending
        self.buffer = []
    }

    public func makeAsyncIterator() -> SubscriptionIterator {
        return SubscriptionIterator(subscription: self)
    }

    func receiveMessage(_ message: Element) {
        lock.withLock {
            if let continuation = self.continuation {
                // Immediately use the continuation if it exists
                self.continuation = nil
                continuation.resume(returning: message)
            } else if buffer.count < maxPending {
                // Only append to buffer if no continuation is available
                buffer.append(message)
            }
        }
    }

    func complete() async {
        lock.withLock {
            closed = true
            continuation?.resume(returning: nil)
        }
    }

    // AsyncIterator implementation
    public class SubscriptionIterator: AsyncIteratorProtocol {
        private var subscription: Subscription

        init(subscription: Subscription) {
            self.subscription = subscription
        }

        public func next() async -> Element? {
            await subscription.nextMessage()
        }
    }

    private func nextMessage() async -> Element? {
        await withCheckedContinuation { continuation in
            lock.withLock {
                if closed {
                    continuation.resume(returning: nil)
                    return
                }

                if let message = buffer.first {
                    buffer.removeFirst()
                    continuation.resume(returning: message)
                } else {
                    self.continuation = continuation
                }
            }
        }
    }
}
