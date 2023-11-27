//
//  NatsSubject.swift
//  SwiftyNats
//

import Foundation

public struct NatsSubject {

    let id: String
    public let subject: String
    public let queue: String?

    // @depricated as replacement of subject, use subject instead.
    public var description: String {
        get {
            return subject
        }
    }

    init(subject: String, id: String, queue: String? = nil) {
        self.subject = subject
        self.id = id
        self.queue = queue
    }

    init(subject: String, queue: String? = nil) {
        let id = String.hash()
        self.init(subject: subject, id: id, queue: queue)
    }
}

extension NatsSubject: Hashable {
    public static func ==(lhs: NatsSubject, rhs: NatsSubject) -> Bool {
        return lhs.hashValue == rhs.hashValue
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.id.hashValue)
    }
}

