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

var logger = Logger(label: "Nats")

/// NatsClient connection states
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
        _ payload: Data, subject: String, reply: String? = nil, headers: NatsHeaderMap? = nil
    ) async throws {
        logger.debug("publish")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        try await connectionHandler.write(
            operation: ClientOp.publish((subject, reply, payload, headers)))
    }

    public func request(
        _ payload: Data, subject: String, headers: NatsHeaderMap? = nil, timeout: TimeInterval = 5
    ) async throws -> NatsMessage {
        logger.debug("request")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        let inbox = "_INBOX.\(nextNuid())"

        let response = try await connectionHandler.subscribe(inbox)
        try await response.unsubscribe(after: 1)
        try await connectionHandler.write(
            operation: ClientOp.publish((subject, inbox, payload, headers)))

        return try await withThrowingTaskGroup(
            of: NatsMessage?.self,
            body: { group in
                group.addTask {
                    return await response.makeAsyncIterator().next()
                }

                // task for the timeout
                group.addTask {
                    try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                    return nil
                }

                for try await result in group {
                    // if the result is not empty, return it (or throw status error)
                    if let msg = result {
                        if let status = msg.status, status == StatusCode.noResponders {
                            throw NatsRequestError.noResponders
                        }
                        return msg
                    } else {
                        // if result is empty, time out
                        throw NatsRequestError.timeout
                    }
                }

                // this should not be reachable
                throw NatsClientError("internal error; error waiting for response")
            })
    }

    public func flush() async throws {
        logger.debug("flush")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        connectionHandler.channel?.flush()
    }

    public func subscribe(subject: String) async throws -> Subscription {
        logger.info("subscribe to subject \(subject)")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        return try await connectionHandler.subscribe(subject)
    }

    public func rtt() async throws -> TimeInterval {
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        let ping = RttCommand.makeFrom(channel: connectionHandler.channel)
        await connectionHandler.sendPing(ping)
        return try await ping.getRoundTripTime()
    }
}
