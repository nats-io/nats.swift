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
public struct NatsHeaderValue: Equatable, CustomStringConvertible, Sendable {
    private var inner: String

    public init(_ value: String) {
        self.inner = value
    }

    public var description: String {
        return inner
    }
}

// Custom header representation in Swift
public struct NatsHeaderName: Equatable, Hashable, CustomStringConvertible, Sendable {
    private var inner: String

    public init(_ value: String) throws {
        if value.contains(where: { char in
            guard let ascii = char.asciiValue else { return true }
            return char == ":" || ascii < 33 || ascii > 126
        }) {
            throw NatsError.ParseHeaderError.invalidCharacter
        }
        self.inner = value
    }

    public var description: String {
        return inner
    }

    // Example of standard headers
    public static let natsStream = try! NatsHeaderName("Nats-Stream")
    public static let natsSequence = try! NatsHeaderName("Nats-Sequence")
    public static let natsTimestamp = try! NatsHeaderName("Nats-Time-Stamp")
    public static let natsSubject = try! NatsHeaderName("Nats-Subject")
    // Add other standard headers as needed...
}

// Represents a NATS header map in Swift.
public struct NatsHeaderMap: Equatable, Sendable {
    private var inner: [NatsHeaderName: [NatsHeaderValue]]
    internal var status: StatusCode? = nil
    internal var description: String? = nil

    public init() {
        self.inner = [:]
    }

    public init(from headersString: String) throws {
        self.inner = [:]
        let headersArray = headersString.split(separator: "\r\n")
        guard let versionLine = headersArray.first, versionLine.hasPrefix(Data.versionLinePrefix)
        else {
            throw NatsError.ProtocolError.parserFailure(
                "header version line does not begin with `NATS/1.0`")
        }
        let versionLineSuffix =
            versionLine
            .dropFirst(Data.versionLinePrefix.count)
            .trimmingCharacters(in: .whitespacesAndNewlines)

        // handle inlines status and description
        if versionLineSuffix.count > 0 {
            let statusAndDesc = versionLineSuffix.split(
                separator: " ", maxSplits: 1)
            guard let status = StatusCode(statusAndDesc[0]) else {
                throw NatsError.ProtocolError.parserFailure("could not parse status parameter")
            }
            self.status = status
            if statusAndDesc.count > 1 {
                self.description = String(statusAndDesc[1])
            }
        }

        for header in headersArray.dropFirst() {
            // a line that begins with a space or a tab continues the value above it; it is
            // not joined to that value
            if header.first == " " || header.first == "\t" {
                logger.error("Error parsing header: \(header)")
                continue
            }
            guard let colon = header.firstIndex(of: ":"), colon != header.startIndex else {
                throw NatsError.ProtocolError.parserFailure(
                    "header line does not begin with a name and a colon")
            }
            self.append(
                try NatsHeaderName(String(header[..<colon])),
                NatsHeaderValue(
                    String(header[header.index(after: colon)...])
                        .trimmingCharacters(in: .whitespaces)))
        }
    }

    var isEmpty: Bool {
        return inner.isEmpty
    }

    public mutating func insert(_ name: NatsHeaderName, _ value: NatsHeaderValue) {
        self.inner[name] = [value]
    }

    public mutating func append(_ name: NatsHeaderName, _ value: NatsHeaderValue) {
        if inner[name] != nil {
            inner[name]?.append(value)
        } else {
            insert(name, value)
        }
    }

    public func get(_ name: NatsHeaderName) -> NatsHeaderValue? {
        return inner[name]?.first
    }

    public func getAll(_ name: NatsHeaderName) -> [NatsHeaderValue] {
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

    // Implementing the == operator to exclude status and desc internal properties
    public static func == (lhs: NatsHeaderMap, rhs: NatsHeaderMap) -> Bool {
        return lhs.inner == rhs.inner
    }
}

extension NatsHeaderMap {
    public subscript(name: NatsHeaderName) -> NatsHeaderValue? {
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
