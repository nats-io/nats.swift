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
    public enum ServerError: NatsErrorProtocol {
        case autorization(String)
        case other(String)

        public var description: String {
            switch self {
            case .autorization(let type):
                return "nats: authorization error: \(type)"
            case .other(let error):
                return "nats: \(error)"
            }
        }
        var normalizedError: String {
            return description.trimWhitespacesAndApostrophes().lowercased()
        }
        init(_ error: String) {
            let normalizedError = error.trimWhitespacesAndApostrophes().lowercased()
            if normalizedError.contains("authorization violation")
                || normalizedError.contains("user authentication expired")
                || normalizedError.contains("user authentication revoked")
                || normalizedError.contains("account authentication expired")
            {
                self = .autorization(normalizedError)
            } else {
                self = .other(normalizedError)
            }
        }
    }

    public enum ProtocolError: NatsErrorProtocol {
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

    public enum RequestError: NatsErrorProtocol {
        case noResponders
        case timeout

        public var description: String {
            switch self {
            case .noResponders:
                return "nats: no responders available for request"
            case .timeout:
                return "nats: request timed out"
            }
        }
    }

    public enum SubscriptionError: NatsErrorProtocol {
        case subscriptionClosed

        public var description: String {
            switch self {
            case .subscriptionClosed:
                return "nats: subscription closed"
            }
        }
    }

    public enum ParseHeaderError: NatsErrorProtocol {
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
