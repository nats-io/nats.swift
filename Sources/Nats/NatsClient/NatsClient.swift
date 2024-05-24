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
    /// will not wait until the connection is but rather return immediatelly.
    ///
    /// - Throws:
    ///  - ``NatsError/ConnectError/invalidConfig(_:)`` if the provided configuration is invalid
    ///  - ``NatsError/ConnectError/tlsFailure(_:)`` if upgrading to TLS connection fails
    ///  - ``NatsError/ConnectError/timeout`` if there was a timeout waiting to establish TCP connection
    ///  - ``NatsError/ConnectError/dns(_:)`` if there was an error during dns lookup
    ///  - ``NatsError/ConnectError/io`` if there was other error establishing connection
    ///  - ``NatsError/ServerError/autorization(_:)`` if connection could not be established due to invalid/missing/expired auth
    ///  - ``NatsError/ServerError/other(_:)`` if the server responds to client connection with a different error (e.g. max connections exceeded)
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

    public func close() async throws {
        logger.debug("close")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        try await connectionHandler.close()
    }

    public func suspend() async throws {
        logger.debug("suspend")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        try await connectionHandler.suspend()
    }

    public func resume() async throws {
        logger.debug("resume")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        try await connectionHandler.resume()
    }

    public func reconnect() async throws {
        logger.debug("resume")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        try await connectionHandler.reconnect()
    }

    public func publish(
        _ payload: Data, subject: String, reply: String? = nil, headers: NatsHeaderMap? = nil
    ) async throws {
        logger.debug("publish")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        try await connectionHandler.write(
            operation: ClientOp.publish((subject, reply, payload, headers)))
    }

    public func request(
        _ payload: Data, subject: String, headers: NatsHeaderMap? = nil, timeout: TimeInterval = 5
    ) async throws -> NatsMessage {
        logger.debug("request")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        let inbox = "_INBOX.\(nextNuid())"

        let sub = try await connectionHandler.subscribe(inbox)
        try await sub.unsubscribe(after: 1)
        try await connectionHandler.write(
            operation: ClientOp.publish((subject, inbox, payload, headers)))

        return try await withThrowingTaskGroup(
            of: NatsMessage?.self,
            body: { group in
                group.addTask {
                    return await sub.makeAsyncIterator().next()
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
            })
    }

    public func flush() async throws {
        logger.debug("flush")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        connectionHandler.channel?.flush()
    }

    public func subscribe(subject: String) async throws -> NatsSubscription {
        logger.info("subscribe to subject \(subject)")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        return try await connectionHandler.subscribe(subject)
    }

    public func rtt() async throws -> TimeInterval {
        guard let connectionHandler = self.connectionHandler else {
            throw NatsError.ClientError.internalError("empty connection handler")
        }
        let ping = RttCommand.makeFrom(channel: connectionHandler.channel)
        await connectionHandler.sendPing(ping)
        return try await ping.getRoundTripTime()
    }
}
