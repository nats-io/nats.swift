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

/// The value of a message header.
public struct NatsHeaderValue: Equatable, CustomStringConvertible, Sendable {
    private var inner: String

    /// Creates a header value.
    public init(_ value: String) {
        self.inner = value
    }

    /// The header name or value as a string.
    public var description: String {
        return inner
    }
}

/// The name of a message header.
///
/// Header names are case sensitive and may contain printable ASCII characters except `:`.
/// Names of headers used by NATS and JetStream are available as static properties, e.g. ``natsMsgId``.
public struct NatsHeaderName: Equatable, Hashable, CustomStringConvertible, Sendable {
    private var inner: String

    /// Creates a header name.
    ///
    /// - Throws: ``NatsError/ParseHeaderError/invalidCharacter`` if the name contains characters which are not allowed.
    public init(_ value: String) throws {
        if value.contains(where: { $0 == ":" || $0.asciiValue! < 33 || $0.asciiValue! > 126 }) {
            throw NatsError.ParseHeaderError.invalidCharacter
        }
        self.inner = value
    }

    /// The header name or value as a string.
    public var description: String {
        return inner
    }

    /// `Nats-Stream`: Stream a message was retrieved from.
    public static let natsStream = try! NatsHeaderName("Nats-Stream")
    /// `Nats-Sequence`: Stream sequence of a retrieved message.
    public static let natsSequence = try! NatsHeaderName("Nats-Sequence")
    /// `Nats-Time-Stamp`: Time a retrieved message was stored.
    public static let natsTimestamp = try! NatsHeaderName("Nats-Time-Stamp")
    /// `Nats-Subject`: Original subject of a retrieved message.
    public static let natsSubject = try! NatsHeaderName("Nats-Subject")
    /// `Nats-Msg-Id`: Unique ID of a published message, used to discard duplicates.
    public static let natsMsgId = try! NatsHeaderName("Nats-Msg-Id")
    /// `Nats-Expected-Stream`: Stream a published message is expected to be stored in.
    public static let natsExpectedStream = try! NatsHeaderName("Nats-Expected-Stream")
    /// `Nats-Expected-Last-Sequence`: Expected sequence of the last message in the stream.
    public static let natsExpectedLastSequence = try! NatsHeaderName(
        "Nats-Expected-Last-Sequence")
    /// `Nats-Expected-Last-Subject-Sequence`: Expected sequence of the last message on the subject.
    public static let natsExpectedLastSubjectSequence = try! NatsHeaderName(
        "Nats-Expected-Last-Subject-Sequence")
    /// `Nats-Expected-Last-Msg-Id`: Expected message ID of the last message in the stream.
    public static let natsExpectedLastMsgId = try! NatsHeaderName("Nats-Expected-Last-Msg-Id")
    /// `Nats-Expected-Last-Subject-Sequence-Subject`: Subject the expected last subject sequence is checked against.
    public static let natsExpectedLastSubjectSequenceSubject = try! NatsHeaderName(
        "Nats-Expected-Last-Subject-Sequence-Subject")
    /// `Nats-TTL`: Time to live of a message.
    public static let natsTTL = try! NatsHeaderName("Nats-TTL")
    /// `Nats-Rollup`: Replaces the messages of a subject (`sub`) or stream (`all`) with this message.
    public static let natsRollup = try! NatsHeaderName("Nats-Rollup")
    /// `Nats-Pin-Id`: ID of the client pinned to a consumer priority group.
    public static let natsPinId = try! NatsHeaderName("Nats-Pin-Id")
    /// `Nats-Pending-Messages`: Number of messages which were not delivered by a pull request.
    public static let natsPendingMessages = try! NatsHeaderName("Nats-Pending-Messages")
    /// `Nats-Pending-Bytes`: Number of bytes which were not delivered by a pull request.
    public static let natsPendingBytes = try! NatsHeaderName("Nats-Pending-Bytes")
    /// `Nats-Consumer-Stalled`: Subject to reply to in order to resume a stalled push consumer.
    public static let natsConsumerStalled = try! NatsHeaderName("Nats-Consumer-Stalled")
    /// `Nats-Marker-Reason`: Reason of a marker placed by the server (e.g. `MaxAge`).
    public static let natsMarkerReason = try! NatsHeaderName("Nats-Marker-Reason")
    /// `Nats-Schedule`: Schedule of a message (`@at`, `@every` or a cron expression).
    public static let natsSchedule = try! NatsHeaderName("Nats-Schedule")
    /// `Nats-Schedule-Target`: Subject scheduled messages are delivered on.
    public static let natsScheduleTarget = try! NatsHeaderName("Nats-Schedule-Target")
    /// `Nats-Schedule-Source`: Subject whose latest message is delivered on schedule.
    public static let natsScheduleSource = try! NatsHeaderName("Nats-Schedule-Source")
    /// `Nats-Schedule-TTL`: Time to live of scheduled messages.
    public static let natsScheduleTTL = try! NatsHeaderName("Nats-Schedule-TTL")
    /// `Nats-Schedule-Time-Zone`: Time zone of a cron schedule.
    public static let natsScheduleTimeZone = try! NatsHeaderName("Nats-Schedule-Time-Zone")
    /// `Nats-Schedule-Rollup`: Rollup applied to scheduled messages.
    public static let natsScheduleRollup = try! NatsHeaderName("Nats-Schedule-Rollup")
    /// `Nats-Batch-Id`: ID of the atomic batch a message belongs to.
    public static let natsBatchId = try! NatsHeaderName("Nats-Batch-Id")
    /// `Nats-Batch-Sequence`: Position of a message in its atomic batch, starting at 1.
    public static let natsBatchSequence = try! NatsHeaderName("Nats-Batch-Sequence")
    /// `Nats-Batch-Commit`: Commits an atomic batch (`1`, or `eob` for an end of batch marker).
    public static let natsBatchCommit = try! NatsHeaderName("Nats-Batch-Commit")
    // Add other standard headers as needed...
}

/// The headers of a message, mapping header names to one or more values.
///
/// ```swift
/// var headers = NatsHeaderMap()
/// headers.append(try NatsHeaderName("X-Trace-Id"), NatsHeaderValue("abc"))
/// let traceId = headers[try NatsHeaderName("X-Trace-Id")]
/// ```
public struct NatsHeaderMap: Equatable, Sendable {
    private var inner: [NatsHeaderName: [NatsHeaderValue]]
    internal var status: StatusCode? = nil
    internal var description: String? = nil

    /// Creates empty headers.
    public init() {
        self.inner = [:]
    }

    /// Parses headers in the NATS wire format.
    ///
    /// - Throws: ``NatsError/ProtocolError`` if the headers are malformed.
    public init(from headersString: String) throws {
        self.inner = [:]
        let headersArray = headersString.split(separator: "\r\n")
        let versionLine = headersArray[0]
        guard versionLine.hasPrefix(Data.versionLinePrefix) else {
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
            let headerParts = header.split(separator: ":", maxSplits: 1)
            if headerParts.count == 2 {
                self.append(
                    try NatsHeaderName(String(headerParts[0])),
                    NatsHeaderValue(String(headerParts[1]).trimmingCharacters(in: .whitespaces)))
            } else {
                logger.error("Error parsing header: \(header)")
            }
        }
    }

    var isEmpty: Bool {
        return inner.isEmpty
    }

    /// Sets the value of a header, replacing existing values.
    public mutating func insert(_ name: NatsHeaderName, _ value: NatsHeaderValue) {
        self.inner[name] = [value]
    }

    /// Adds a value to a header, keeping existing values.
    public mutating func append(_ name: NatsHeaderName, _ value: NatsHeaderValue) {
        if inner[name] != nil {
            inner[name]?.append(value)
        } else {
            insert(name, value)
        }
    }

    /// Returns the first value of a header.
    public func get(_ name: NatsHeaderName) -> NatsHeaderValue? {
        return inner[name]?.first
    }

    /// Returns all values of a header.
    public func getAll(_ name: NatsHeaderName) -> [NatsHeaderValue] {
        return inner[name] ?? []
    }

    //TODO(jrm): can we use unsafe methods here? Probably yes.
    /// Serializes the headers in the NATS wire format.
    public func toBytes() -> [UInt8] {
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
    /// Compares the headers, ignoring the status of the message.
    public static func == (lhs: NatsHeaderMap, rhs: NatsHeaderMap) -> Bool {
        return lhs.inner == rhs.inner
    }
}

extension NatsHeaderMap {
    /// The first value of a header.
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
