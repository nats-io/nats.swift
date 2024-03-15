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

public protocol NatsError: Error {
    var description: String { get }
}

struct NatsServerError: NatsError {
    var description: String
    var normalizedError: String {
        return description.trimWhitespacesAndApostrophes().lowercased()
    }
    init(_ description: String) {
        self.description = description
    }
}

struct NatsParserError: NatsError {
    var description: String
    init(_ description: String) {
        self.description = description
    }
}

struct NatsClientError: NatsError {
    var description: String
    init(_ description: String) {
        self.description = description
    }
}

struct NatsConfigError: NatsError {
    var description: String
    init(_ description: String) {
        self.description = description
    }
}

public enum NatsRequestError: NatsError {
    case noResponders
    case timeout

    public var description: String {
        switch self {
        case .noResponders:
            return "no responders available for request"
        case .timeout:
            return "request timed out"
        }
    }
}