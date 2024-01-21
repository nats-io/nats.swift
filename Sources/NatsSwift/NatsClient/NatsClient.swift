//
//  NatsClient.swift
//  NatsSwift
//

import Foundation
import NIO
import NIOFoundationCompat
import Dispatch

import Logging

var logger = Logger(label: "NatsSwift")

/// Client connection states
public enum NatsState {
    case Pending
    case Connected
    case Disconnected
}

public class Client {
    internal let allocator = ByteBufferAllocator()
    internal var buffer: ByteBuffer
    internal var connectionHandler: ConnectionHandler?

    internal init() {
        self.buffer = allocator.buffer(capacity: 1024)
    }
}

extension Client {
    public func connect() async throws  {
        //TODO(jrm): reafactor for reconnection and review error handling.
        //TODO(jrm): handle response
        logger.debug("connect")
        guard let connectionHandler = self.connectionHandler else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "empty connection handler"])
        }
        try await connectionHandler.connect()
    }

    public func publish(_ payload: Data, subject: String, reply: String? = nil, headers: HeaderMap? = nil) throws {
        logger.debug("publish")
        guard let connectionHandler = self.connectionHandler else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "empty connection handler"])
        }
        try connectionHandler.write(operation: ClientOp.Publish((subject, reply, payload, headers)))
    }

    public func flush() async throws {
        logger.debug("flush")
        guard let connectionHandler = self.connectionHandler else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "empty connection handler"])
        }
        connectionHandler.channel?.flush()
    }

    public func subscribe(to subject: String) async throws -> Subscription {
        logger.info("subscribe to subject \(subject)")
        guard let connectionHandler = self.connectionHandler else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "empty connection handler"])
        }
        return try await connectionHandler.subscribe(subject)
        
    }
}
