//
//  NatsClient+Queue.swift
//  NatsSwift
//

import Foundation
import Dispatch

extension NatsClient: NatsQueue {

    public var queueCount: Int {
        return self.messageQueue.operationCount
    }

    private var waitTimeBetweenQueueCheck: UInt32 {
        return 500 // milliseconds
    }

    open func flushQueue(maxWait: TimeInterval? = nil) throws {

        let startTimestamp = Date().timeIntervalSinceReferenceDate

        self.disconnect()

        DispatchQueue.global(qos: .default).async { [weak self] in
            self?.messageQueue.waitUntilAllOperationsAreFinished()
        }

        while true {
            if self.queueCount == 0 { break }
            if let maxSeconds = maxWait {
                if Date().timeIntervalSinceReferenceDate - startTimestamp > maxSeconds {
                    throw NatsTimeoutError("Could not handle all messages in queue before max time limit reached")
                }
            }
            usleep(self.waitTimeBetweenQueueCheck)
        }

    }

}
