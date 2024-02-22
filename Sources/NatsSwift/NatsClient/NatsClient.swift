//
//  NatsClient.swift
//  NatsSwift
//

import Dispatch
import Foundation
import Logging
import NIO
import NIOFoundationCompat

var logger = Logger(label: "NatsSwift")

/// Client connection states
public enum NatsState {
    case pending
    case connected
    case disconnected
    case closed
}

public struct Auth {
    var user: String?
    var password: String?
    var token: String?
    var credentialsPath: URL?

    init() {

    }

    init(user: String, password: String) {
        self.user = user
        self.password = password
    }
    init(token: String) {
        self.token = token
    }
    static func fromCredentials(_ credentials: URL) -> Auth {
        var auth = Auth()
        auth.credentialsPath = credentials
        return auth
    }
}

public class Client {
    public var connectedUrl: URL? {
        connectionHandler?.connectedUrl
    }
    internal let allocator = ByteBufferAllocator()
    internal var buffer: ByteBuffer
    internal var connectionHandler: ConnectionHandler?

    internal init() {
        self.buffer = allocator.buffer(capacity: 1024)
    }
}

extension Client {
    public func connect() async throws {
        logger.debug("connect")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        if !connectionHandler.retryOnFailedConnect {
            try await connectionHandler.connect()
        } else {
            connectionHandler.handleReconnect()
        }
    }

    public func close() async throws {
        logger.debug("close")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        try await connectionHandler.close()
    }

    public func publish(
        _ payload: Data, subject: String, reply: String? = nil, headers: HeaderMap? = nil
    ) throws {
        logger.debug("publish")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        try connectionHandler.write(operation: ClientOp.publish((subject, reply, payload, headers)))
    }

    public func flush() async throws {
        logger.debug("flush")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        connectionHandler.channel?.flush()
    }

    public func subscribe(to subject: String) async throws -> Subscription {
        logger.info("subscribe to subject \(subject)")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        return try await connectionHandler.subscribe(subject)

    }
    
    public func rtt() async throws -> Duration {
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        let ping = RttCommand.makeFrom(channel: connectionHandler.channel)
        connectionHandler.sendPing(ping)
        return try await ping.getRoundTripTime	()
    }
}
