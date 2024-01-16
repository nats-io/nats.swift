//
//  NatsSubscription.swift
//  NatsSwift
//

import Foundation

// TODO(pp): Implement slow consumer
public class Subscription: AsyncIteratorProtocol {
    public typealias Element = NatsMessage

    private var buffer: [Element]
    private let maxPending: UInt64
    private var closed = false
    private var continuation: CheckedContinuation<Element?, Never>?
    private let lock = NSLock()
    public let subject: String

    private static let defaultMaxPending: UInt64 = 512*1024

    convenience init(subject: String) {
        self.init(subject: subject, maxPending: Subscription.defaultMaxPending)
    }

    init(subject: String, maxPending: uint64) {
        self.subject = subject
        self.maxPending = maxPending
        self.buffer = []
    }

    public func next() async -> Element? {
        let msg: NatsMessage? = lock.withLock {
            if closed {
                return nil
            }

            if let message = buffer.first {
                buffer.removeFirst()
                return message
            }
            return nil
        }
        if let msg {
            return msg
        }
        return await withCheckedContinuation { continuation in
            lock.withLock{
                self.continuation = continuation
            }
        }
    }

    func receiveMessage(_ message: Element) {
        lock.withLock {
            if let continuation = self.continuation {
                continuation.resume(returning: message)
                self.continuation = nil
            } else if buffer.count < maxPending {
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

}

internal class SubscriptionCounter {
    private var counter: UInt64 = 0
    private let queue = DispatchQueue(label: "io.nats.swift.subscriptionCounter")

    func next() -> UInt64 {
        queue.sync {
            counter+=1
            return counter
        }
    }
}
