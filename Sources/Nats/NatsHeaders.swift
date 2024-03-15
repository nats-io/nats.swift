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

// Represents NATS header field value in Swift.
public struct HeaderValue: Equatable, CustomStringConvertible {
    private var inner: String

    public init(_ value: String) {
        self.inner = value
    }

    public var description: String {
        return inner
    }
}

public enum ParseHeaderNameError: NatsError {
    case invalidCharacter

    public var description: String {
        switch self {
        case .invalidCharacter:
            return
                "Invalid header name (name cannot contain non-ascii alphanumeric characters other than '-')"
        }
    }
}

// Custom header representation in Swift
public struct HeaderName: Equatable, Hashable, CustomStringConvertible {
    private var inner: String

    public init(_ value: String) throws {
        if value.contains(where: { $0 == ":" || $0.asciiValue! < 33 || $0.asciiValue! > 126 }) {
            throw ParseHeaderNameError.invalidCharacter
        }
        self.inner = value
    }

    public var description: String {
        return inner
    }

    // Example of standard headers
    public static let natsStream = try! HeaderName("Nats-Stream")
    public static let natsSequence = try! HeaderName("Nats-Sequence")
    // Add other standard headers as needed...
}

// Represents a NATS header map in Swift.
public struct HeaderMap: Equatable {
    private var inner: [HeaderName: [HeaderValue]]

    public init() {
        self.inner = [:]
    }

    var isEmpty: Bool {
        return inner.isEmpty
    }

    public mutating func insert(_ name: HeaderName, _ value: HeaderValue) {
        self.inner[name] = [value]
    }

    public mutating func append(_ name: HeaderName, _ value: HeaderValue) {
        if inner[name] != nil {
            inner[name]?.append(value)
        } else {
            insert(name, value)
        }
    }

    public func get(_ name: HeaderName) -> HeaderValue? {
        return inner[name]?.first
    }

    public func getAll(_ name: HeaderName) -> [HeaderValue] {
        return inner[name] ?? []
    }

    //TODO(jrm): can we use unsafe methods here? Probably yes.
    func toBytes() -> [UInt8] {
        var bytes: [UInt8] = []
        bytes.append(contentsOf: "NATS/1.0\r\n".utf8)
        for (name, values) in inner {
            for value in values {
                bytes.append(contentsOf: name.description.utf8)
                bytes.append(contentsOf: ":".utf8)
                bytes.append(contentsOf: value.description.utf8)
                bytes.append(contentsOf: "\r\n".utf8)
            }
        }
        bytes.append(contentsOf: "\r\n".utf8)
        return bytes
    }
}

extension HeaderMap {
    subscript(name: HeaderName) -> HeaderValue? {
        get {
            return get(name)
        }
        set {
            if let value = newValue {
                insert(name, value)
            } else {
                inner[name] = nil
            }
        }
    }
}