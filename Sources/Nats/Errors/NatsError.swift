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

import Foundation

/// A protocol adopted by all errors thrown by the client.
public protocol NatsErrorProtocol: Error, CustomStringConvertible, Sendable {}

/// Namespace of the errors thrown by the client, grouped by their origin.
public enum NatsError {
    /// Errors reported by the server with an `-ERR` protocol message.
    public enum ServerError: NatsErrorProtocol, Equatable {
        /// The server closed the connection because it did not respond to pings.
        case staleConnection
        /// The server reached its maximum number of connections.
        case maxConnectionsExceeded
        /// The credentials were rejected by the server.
        case authorizationViolation
        /// The credentials of the connection expired.
        case authenticationExpired
        /// The credentials of the connection were revoked.
        case authenticationRevoked
        /// The client did not authenticate in time.
        case authenticationTimeout
        /// The client is not permitted to publish or subscribe to a subject (operation, subject and queue).
        case permissionsViolation(Operation, String, String?)
        /// Any other error reported by the server.
        case proto(String)

        /// A description of the error.
        public var description: String {
            switch self {
            case .staleConnection:
                return "nats: stale connection"
            case .maxConnectionsExceeded:
                return "nats: maximum connections exceeded"
            case .authorizationViolation:
                return "nats: authorization violation"
            case .authenticationExpired:
                return "nats: authentication expired"
            case .authenticationRevoked:
                return "nats: authentication revoked"
            case .authenticationTimeout:
                return "nats: authentication timeout"
            case .permissionsViolation(let operation, let subject, let queue):
                if let queue {
                    return
                        "nats: permissions violation for operation \"\(operation)\" on subject \"\(subject)\" using queue \"\(queue)\""
                } else {
                    return
                        "nats: permissions violation for operation \"\(operation)\" on subject \"\(subject)\""
                }
            case .proto(let error):
                return "nats: \(error)"
            }
        }
        var normalizedError: String {
            return description.trimWhitespacesAndApostrophes().lowercased()
        }
        init(_ error: String) {
            let normalizedError = error.trimWhitespacesAndApostrophes().lowercased()
            if normalizedError.contains("stale connection") {
                self = .staleConnection
            } else if normalizedError.contains("maximum connections exceeded") {
                self = .maxConnectionsExceeded
            } else if normalizedError.contains("authorization violation") {
                self = .authorizationViolation
            } else if normalizedError.contains("authentication expired") {
                self = .authenticationExpired
            } else if normalizedError.contains("authentication revoked") {
                self = .authenticationRevoked
            } else if normalizedError.contains("authentication timeout") {
                self = .authenticationTimeout
            } else if normalizedError.contains("permissions violation") {
                if let (operation, subject, queue) = NatsError.ServerError.parsePermissions(
                    error: error)
                {
                    self = .permissionsViolation(operation, subject, queue)
                } else {
                    self = .proto(error)
                }
            } else {
                self = .proto(error)
            }
        }

        /// Operation denied by a permissions violation.
        public enum Operation: String, Equatable, Sendable {
            /// Publishing to a subject.
            case publish = "Publish"
            /// Subscribing to a subject.
            case subscribe = "Subscription"
        }

        internal static func parsePermissions(error: String) -> (Operation, String, String?)? {
            let pattern = "(Publish|Subscription) to \"(\\S+)\""
            let regex = try! NSRegularExpression(pattern: pattern)
            let matches = regex.matches(
                in: error, options: [], range: NSRange(location: 0, length: error.utf16.count))

            guard let match = matches.first else {
                return nil
            }

            var operation: Operation?
            if let operationRange = Range(match.range(at: 1), in: error) {
                let operationString = String(error[operationRange])
                operation = Operation(rawValue: operationString)
            }

            var subject: String?
            if let subjectRange = Range(match.range(at: 2), in: error) {
                subject = String(error[subjectRange])
            }

            let queuePattern = "using queue \"(\\S+)\""
            let queueRegex = try! NSRegularExpression(pattern: queuePattern)
            let queueMatches = queueRegex.matches(
                in: error, options: [], range: NSRange(location: 0, length: error.utf16.count))

            var queue: String?
            if let match = queueMatches.first, let queueRange = Range(match.range(at: 1), in: error)
            {
                queue = String(error[queueRange])
            }

            if let operation, let subject {
                return (operation, subject, queue)
            } else {
                return nil
            }
        }
    }

    /// Errors caused by unexpected data received from the server.
    public enum ProtocolError: NatsErrorProtocol, Equatable {
        /// The server sent an unknown protocol operation.
        case invalidOperation(String)
        /// A protocol message could not be parsed.
        case parserFailure(String)

        /// A description of the error.
        public var description: String {
            switch self {
            case .invalidOperation(let op):
                return "nats: unknown server operation: \(op)"
            case .parserFailure(let cause):
                return "nats: parser failure: \(cause)"
            }
        }
    }

    /// Errors of the client or its connection.
    public enum ClientError: NatsErrorProtocol {
        /// An unexpected internal error.
        case internalError(String)
        /// The maximum number of reconnect attempts was reached.
        case maxReconnects
        /// The connection was closed.
        case connectionClosed
        /// A network error occurred.
        case io(Error)
        /// The operation is not allowed in the current state of the connection.
        case invalidConnection(String)
        /// The operation was cancelled.
        case cancelled
        /// The client is already connected.
        case alreadyConnected

        /// A description of the error.
        public var description: String {
            switch self {
            case .internalError(let error):
                return "nats: internal error: \(error)"
            case .maxReconnects:
                return "nats: max reconnects exceeded"
            case .connectionClosed:
                return "nats: connection is closed"
            case .io(let error):
                return "nats: IO error: \(error)"
            case .invalidConnection(let error):
                return "nats: \(error)"
            case .cancelled:
                return "nats: operation cancelled"
            case .alreadyConnected:
                return "nats: client is already connected or connecting"
            }
        }
    }

    /// Errors establishing a connection.
    public enum ConnectError: NatsErrorProtocol {
        /// The client options are invalid, e.g. a credentials file could not be read.
        case invalidConfig(String)
        /// The TLS handshake failed.
        case tlsFailure(Error)
        /// The connection timed out.
        case timeout
        /// The server address could not be resolved.
        case dns(Error)
        /// A network error occurred.
        case io(Error)

        /// A description of the error.
        public var description: String {
            switch self {
            case .invalidConfig(let error):
                return "nats: invalid client configuration: \(error)"
            case .tlsFailure(let error):
                return "nats: TLS error: \(error)"
            case .timeout:
                return "nats: timed out waiting for connection"
            case .dns(let error):
                return "nats: DNS lookup error: \(error)"
            case .io(let error):
                return "nats: error establishing connection: \(error)"
            }
        }
    }

    /// Errors of ``NatsClient/request(_:subject:headers:timeout:)``.
    public enum RequestError: NatsErrorProtocol, Equatable {
        /// No subscriber is listening on the request subject.
        case noResponders
        /// No reply was received before the timeout.
        case timeout
        /// The client is not permitted to use the subject.
        case permissionDenied

        /// A description of the error.
        public var description: String {
            switch self {
            case .noResponders:
                return "nats: no responders available for request"
            case .timeout:
                return "nats: request timed out"
            case .permissionDenied:
                return "nats: permission denied"
            }
        }
    }

    /// Errors of subscriptions.
    public enum SubscriptionError: NatsErrorProtocol, Equatable {
        /// The subject is not valid.
        case invalidSubject
        /// The queue group name is not valid.
        case invalidQueue
        /// The client is not permitted to use the subject.
        case permissionDenied
        /// The subscription was closed.
        case subscriptionClosed
        /// Messages were dropped because the subscription did not keep up with them.
        case slowConsumer

        /// A description of the error.
        public var description: String {
            switch self {
            case .invalidSubject:
                return "nats: invalid subject name"
            case .invalidQueue:
                return "nats: invalid queue group name"
            case .permissionDenied:
                return "nats: permission denied"
            case .subscriptionClosed:
                return "nats: subscription closed"
            case .slowConsumer:
                return "nats: slow consumer, messages dropped"
            }
        }
    }

    /// Errors creating message headers.
    public enum ParseHeaderError: NatsErrorProtocol, Equatable {
        /// The header name contains characters which are not allowed.
        case invalidCharacter

        /// A description of the error.
        public var description: String {
            switch self {
            case .invalidCharacter:
                return
                    "nats: invalid header name (name cannot contain non-ascii alphanumeric characters other than '-')"
            }
        }
    }
}
