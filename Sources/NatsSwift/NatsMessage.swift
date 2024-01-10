//
//  NatsMessage.swift
//  NatsSwift
//

import Foundation

// TODO(pp) Add headers, status, description etc
public struct NatsMessage {
    public let payload: Data?
    public let subject: String
    public let replySubject: String?
    public let length: Int
    public let headers: HeaderMap?
    public let status: Int?
}
