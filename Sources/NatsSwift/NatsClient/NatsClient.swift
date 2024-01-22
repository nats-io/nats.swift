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

public struct Auth {
    var user: String?
    var password: String?
    var token: String?

    init(user: String, password: String) {
        self.user = user
        self.password = password
    }
    init(token: String) {
        self.token = token
    }
}

public class Client {
    var urls: [URL] = []
    var pingInteval: TimeInterval = 1.0
    var reconnectWait: TimeInterval = 2.0
    var maxReconnects: Int? = nil
    var auth: Auth? = nil

    internal let allocator = ByteBufferAllocator()
    internal var buffer: ByteBuffer
    internal var connectionHandler: ConnectionHandler?

    internal init() {
        self.buffer = allocator.buffer(capacity: 1024)
        self.connectionHandler = ConnectionHandler(
            inputBuffer: buffer,
            urls: urls,
            reconnectWait: reconnectWait,
            maxReconnects: maxReconnects,
            pingInterval: pingInteval,
            auth: auth
        )
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
