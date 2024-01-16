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
    var urls: [URL] = []
    var pingInteval: TimeInterval = 1.0
    var reconnectWait: TimeInterval = 2.0
    var maxReconnects: Int? = nil

    internal let allocator = ByteBufferAllocator()
    internal var buffer: ByteBuffer
    internal var connectionHandler: ConnectionHandler

    internal init() {
        self.buffer = allocator.buffer(capacity: 1024)
        self.connectionHandler = ConnectionHandler(
            inputBuffer: buffer,
            urls: urls,
            reconnectWait: reconnectWait,
            maxReconnects: maxReconnects
        )
    }
}

extension Client {
    public func connect() async throws  {
        //TODO(jrm): reafactor for reconnection and review error handling.
        //TODO(jrm): handle response
        logger.debug("connect")
        try await self.connectionHandler.connect()
    }

    public func publish(_ payload: Data, subject: String, reply: String? = nil, headers: HeaderMap? = nil) throws {
        logger.debug("publish")
        try self.connectionHandler.write(operation: ClientOp.Publish((subject, reply, payload, headers)))
    }

    public func flush() async throws {
        logger.debug("flush")
        self.connectionHandler.channel?.flush()
    }

    public func subscribe(to subject: String) async throws -> Subscription {
        logger.info("subscribe to subject \(subject)")
        return try await self.connectionHandler.subscribe(subject)
    }
}
