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

import Dispatch
import Foundation
import Logging
import NIO
import NIOFoundationCompat
import Nuid

public var logger = Logger(label: "Nats")

/// NatsClient connection states
public enum NatsState {
    case pending
    case connected
    case disconnected
    case closed
    case suspended
}

public struct Auth {
    var user: String?
    var password: String?
    var token: String?
    var credentialsPath: URL?
    var nkeyPath: URL?
    var nkey: String?

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
    static func fromNkey(_ nkey: URL) -> Auth {
        var auth = Auth()
        auth.nkeyPath = nkey
        return auth
    }
    static func fromNkey(_ nkey: String) -> Auth {
        var auth = Auth()
        auth.nkey = nkey
        return auth
    }
}

public class NatsClient {
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

extension NatsClient {

    /// Connects to a NATS server using configuration provided via ``NatsClientOptions``.
    /// If ``NatsClientOptions/retryOnfailedConnect()`` is used, `connect()`
    /// will not wait until the connection is established but rather return immediatelly.
    ///
    /// > **Throws:**
    /// > - ``NatsError/ConnectError/invalidConfig(_:)`` if the provided configuration is invalid
    /// > - ``NatsError/ConnectError/tlsFailure(_:)`` if upgrading to TLS connection fails
    /// > - ``NatsError/ConnectError/timeout`` if there was a timeout waiting to establish TCP connection
    /// > - ``NatsError/ConnectError/dns(_:)`` if there was an error during dns lookup
    /// > - ``NatsError/ConnectError/io`` if there was other error establishing connection
    /// > - ``NatsError/ServerError/autorization(_:)`` if connection could not be established due to invalid/missing/expired auth
    /// > - ``NatsError/ServerError/other(_:)`` if the server responds to client connection with a different error (e.g. max connections exceeded)
    public func connect() async throws {
        logger.debug("connect")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        if !connectionHandler.retryOnFailedConnect {
            try await connectionHandler.connect()
            connectionHandler.state = .connected
            connectionHandler.fire(.connected)
        } else {
            connectionHandler.handleReconnect()
        }
    }

    /// Closes a connection to NATS server.
    ///
    /// - Throws ``NatsError/ClientError/connectionClosed`` if the conneciton is already closed.
    public func close() async throws {
        logger.debug("close")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        if case .closed = connectionHandler.state {
            throw NatsError.ClientError.connectionClosed
        }
        try await connectionHandler.close()
    }

    /// Suspends a connection to NATS server.
    /// A suspended connection does not receive messages on subscriptions.
    /// It can be resumed using ``resume()`` which restores subscriptions on successful reconnect.
    ///
    /// - Throws ``NatsError/ClientError/connectionClosed`` if the conneciton is closed.
    public func suspend() async throws {
        logger.debug("suspend")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        if case .closed = connectionHandler.state {
            throw NatsError.ClientError.connectionClosed
        }
        try await connectionHandler.suspend()
    }

    /// Resumes a suspended connection.
    /// ``resume()`` will not wait for successful reconnection but rather trigger a reconnect process and return.
    /// Register ``NatsEvent`` using ``NatsClient/on()`` to wait for successful reconnection.
    ///
    /// - Throws ``NatsError/ClientError`` if the conneciton is not in suspended state.
    public func resume() async throws {
        logger.debug("resume")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        if case .closed = connectionHandler.state {
            throw NatsError.ClientError.connectionClosed
        }
        try await connectionHandler.resume()
    }

    /// Forces a reconnect attempt to the server.
    /// This is a non-blocking operation and will start the  process without waiting for it to complete.
    ///
    /// - Throws ``NatsError/ClientError/connectionClosed`` if the conneciton is closed.
    public func reconnect() async throws {
        logger.debug("resume")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        if case .closed = connectionHandler.state {
            throw NatsError.ClientError.connectionClosed
        }
        try await connectionHandler.reconnect()
    }

    /// Publishes a message on a given subject.
    ///
    /// - Parameters:
    ///   - payload: data to be published.
    ///   - subject: a NATS subject on which the message will be published.
    ///   - reply: optional reply subject when publishing a request.
    ///   - headers: optional message headers.
    ///
    /// > **Throws:**
    /// >  - ``NatsError/ClientError/connectionClosed`` if the conneciton is closed.
    /// >  - ``NatsError/ClientError/io(_:)`` if there is an error writing message to a TCP socket (e.g. bloken pipe).
    public func publish(
        _ payload: Data, subject: String, reply: String? = nil, headers: NatsHeaderMap? = nil
    ) async throws {
        logger.debug("publish")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        if case .closed = connectionHandler.state {
            throw NatsError.ClientError.connectionClosed
        }
        try await connectionHandler.write(
            operation: ClientOp.publish((subject, reply, payload, headers)))
    }

    /// Sends a blocking request on a given subject.
    ///
    /// - Parameters:
    ///   - payload: data to be published in the request.
    ///   - subject: a NATS subject on which the request will be published.
    ///   - headers: optional request headers.
    ///   - timeout: request timeout - defaults to 5 seconds.
    ///
    /// - Returns a ``NatsMessage`` containing the response.
    ///
    /// > **Throws:**
    /// >  - ``NatsError/ClientError/connectionClosed`` if the conneciton is closed.
    /// >  - ``NatsError/ClientError/io(_:)`` if there is an error writing message to a TCP socket (e.g. bloken pipe).
    /// >  - ``NatsError/RequestError/noResponders`` if there are no responders available for the request.
    /// >  - ``NatsError/RequestError/timeout`` if there was a timeout waiting for the response.
    public func request(
        _ payload: Data, subject: String, headers: NatsHeaderMap? = nil, timeout: TimeInterval = 5
    ) async throws -> NatsMessage {
        logger.debug("request")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        if case .closed = connectionHandler.state {
            throw NatsError.ClientError.connectionClosed
        }
        let inbox = "_INBOX.\(nextNuid())"

        let sub = try await connectionHandler.subscribe(inbox)
        try await sub.unsubscribe(after: 1)
        try await connectionHandler.write(
            operation: ClientOp.publish((subject, inbox, payload, headers)))

        return try await withThrowingTaskGroup(
            of: NatsMessage?.self
        ) { group in
            group.addTask {
                do {
                    return try await sub.makeAsyncIterator().next()
                } catch NatsError.SubscriptionError.permissionDenied {
                    throw NatsError.RequestError.permissionDenied
                }
            }

            // task for the timeout
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                return nil
            }

            for try await result in group {
                // if the result is not empty, return it (or throw status error)
                if let msg = result {
                    group.cancelAll()
                    if let status = msg.status, status == StatusCode.noResponders {
                        throw NatsError.RequestError.noResponders
                    }
                    return msg
                } else {
                    try await sub.unsubscribe()
                    group.cancelAll()
                    throw NatsError.RequestError.timeout
                }
            }

            // this should not be reachable
            throw NatsError.ClientError.internalError("error waiting for response")
        }
    }

    /// Flushes the internal buffer ensuring that all messages are sent.
    ///
    /// - Throws ``NatsError/ClientError/connectionClosed`` if the conneciton is closed.
    public func flush() async throws {
        logger.debug("flush")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        if case .closed = connectionHandler.state {
            throw NatsError.ClientError.connectionClosed
        }
        connectionHandler.channel?.flush()
    }

    /// Subscribes to a subject to receive messages.
    ///
    /// - Parameters:
    ///  - subject:a subject the client want's to subscribe to.
    ///  - queue: optional queue group name.
    ///
    /// - Returns a ``NatsSubscription`` allowing iteration over incoming messages.
    ///
    /// > **Throws:**
    /// >  - ``NatsError/ClientError/connectionClosed`` if the conneciton is closed.
    /// >  - ``NatsError/ClientError/io(_:)`` if there is an error sending the SUB request to the server.
    /// > - ``NatsError/SubscriptionError/invalidSubject`` if the provided subject is invalid.
    /// > - ``NatsError/SubscriptionError/invalidQueue`` if the provided queue group is invalid.
    public func subscribe(subject: String, queue: String? = nil) async throws -> NatsSubscription {
        logger.info("subscribe to subject \(subject)")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        if case .closed = connectionHandler.state {
            throw NatsError.ClientError.connectionClosed
        }
        return try await connectionHandler.subscribe(subject, queue: queue)
    }

    /// Sends a PING to the server, returning the time it took for the server to respond.
    ///
    /// - Returns rtt of the request.
    ///
    /// > **Throws:**
    /// >  - ``NatsError/ClientError/connectionClosed`` if the conneciton is closed.
    /// >  - ``NatsError/ClientError/io(_:)`` if there is an error sending the SUB request to the server.
    public func rtt() async throws -> TimeInterval {
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        if case .closed = connectionHandler.state {
            throw NatsError.ClientError.connectionClosed
        }
        let ping = RttCommand.makeFrom(channel: connectionHandler.channel)
        await connectionHandler.sendPing(ping)
        return try await ping.getRoundTripTime()
    }
}
