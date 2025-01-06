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

public protocol NatsErrorProtocol: Error, CustomStringConvertible {}

public enum NatsError {
    public enum ServerError: NatsErrorProtocol, Equatable {
        case staleConnection
        case maxConnectionsExceeded
        case authorizationViolation
        case authenticationExpired
        case authenticationRevoked
        case authenticationTimeout
        case permissionsViolation(Operation, String, String?)
        case proto(String)

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

        public enum Operation: String, Equatable {
            case publish = "Publish"
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

    public enum ProtocolError: NatsErrorProtocol, Equatable {
        case invalidOperation(String)
        case parserFailure(String)

        public var description: String {
            switch self {
            case .invalidOperation(let op):
                return "nats: unknown server operation: \(op)"
            case .parserFailure(let cause):
                return "nats: parser failure: \(cause)"
            }
        }
    }

    public enum ClientError: NatsErrorProtocol {
        case internalError(String)
        case maxReconnects
        case connectionClosed
        case io(Error)
        case invalidConnection(String)
        case cancelled

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
            }
        }
    }

    public enum ConnectError: NatsErrorProtocol {
        case invalidConfig(String)
        case tlsFailure(Error)
        case timeout
        case dns(Error)
        case io(Error)

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

    public enum RequestError: NatsErrorProtocol, Equatable {
        case noResponders
        case timeout
        case permissionDenied

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

    public enum SubscriptionError: NatsErrorProtocol, Equatable {
        case invalidSubject
        case invalidQueue
        case permissionDenied
        case subscriptionClosed

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
            }
        }
    }

    public enum ParseHeaderError: NatsErrorProtocol, Equatable {
        case invalidCharacter

        public var description: String {
            switch self {
            case .invalidCharacter:
                return
                    "nats: invalid header name (name cannot contain non-ascii alphanumeric characters other than '-')"
            }
        }
    }
}
