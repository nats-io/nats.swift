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
import JetStream
import Nats

/// A key-value store backed by a JetStream stream (`KV_<bucket>`).
public final class KeyValue: Sendable {
    /// Name of the bucket.
    public let bucket: String
    internal let ctx: JetStreamContext
    internal let stream: JetStream.Stream
    /// Subject prefix of keys when reading.
    internal let prefix: String
    /// Subject prefix of keys when writing, differs from ``prefix`` for mirrors.
    internal let putPrefix: String

    internal static let streamPrefix = "KV_"

    init(ctx: JetStreamContext, stream: JetStream.Stream) {
        self.ctx = ctx
        self.stream = stream
        self.bucket = String(stream.info.config.name.dropFirst(Self.streamPrefix.count))
        if let mirror = stream.info.config.mirror {
            // mirrors keep the subjects of the origin bucket, writes go to the origin
            let origin =
                mirror.name.hasPrefix(Self.streamPrefix)
                ? String(mirror.name.dropFirst(Self.streamPrefix.count)) : mirror.name
            self.prefix = "$KV.\(origin)."
            let apiPrefix = mirror.external.map { "\($0.apiPrefix)." } ?? ""
            self.putPrefix = apiPrefix + prefix
        } else {
            self.prefix = "$KV.\(bucket)."
            // writes through a different domain or account are routed via its API prefix
            let apiPrefix = ctx.prefix == "$JS.API" ? "" : "\(ctx.prefix)."
            self.putPrefix = apiPrefix + prefix
        }
    }

    /// Retrieves the latest value of a key.
    ///
    /// - Returns ``KeyValueEntry`` or nil if the key does not exist or was deleted.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/KeyValueError/invalidKey(_:)``: if the key is not valid.
    /// > - `JetStreamError.RequestError`: if the request fails.
    public func get(_ key: String) async throws -> KeyValueEntry? {
        try await entry(key).flatMap { $0.operation == .put ? $0 : nil }
    }

    /// Retrieves the value of a key at the given revision.
    ///
    /// - Returns ``KeyValueEntry`` or nil if the revision does not exist, belongs to a different key
    /// or is a delete marker.
    public func get(_ key: String, revision: UInt64) async throws -> KeyValueEntry? {
        try await entry(key, revision: revision).flatMap { $0.operation == .put ? $0 : nil }
    }

    /// Sets the value of a key.
    ///
    /// - Returns the revision of the new value.
    @discardableResult
    public func put(_ key: String, _ value: Data) async throws -> UInt64 {
        try Self.validate(key: key)
        return try await ctx.publish(putPrefix + key, message: value).wait().seq
    }

    /// Sets the value of a key. See ``put(_:_:)-(String,Data)``.
    @discardableResult
    public func put(_ key: String, _ value: String) async throws -> UInt64 {
        try await put(key, Data(value.utf8))
    }

    /// Sets the value of a key only if it does not exist (or was deleted).
    ///
    /// - Parameters:
    ///  - ttl: time after which the key expires, requires ``KeyValueConfig/limitMarkerTTL``.
    ///
    /// - Returns the revision of the new value.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/KeyValueError/keyExists(_:)``: if the key exists.
    @discardableResult
    public func create(
        _ key: String, _ value: Data, ttl: TimeInterval? = nil
    ) async throws
        -> UInt64
    {
        do {
            return try await update(key, value, revision: 0, ttl: ttl)
        } catch JetStreamError.KeyValueError.wrongLastRevision {
            // the key may have been deleted, in which case it can be recreated
            guard let deleted = try await entry(key), deleted.operation != .put else {
                throw JetStreamError.KeyValueError.keyExists(key)
            }
            do {
                return try await update(key, value, revision: deleted.revision, ttl: ttl)
            } catch JetStreamError.KeyValueError.wrongLastRevision {
                throw JetStreamError.KeyValueError.keyExists(key)
            }
        }
    }

    /// Sets the value of a key only if its latest revision matches the given one.
    ///
    /// - Returns the revision of the new value.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/KeyValueError/wrongLastRevision(_:)``: if the revision does not match.
    @discardableResult
    public func update(_ key: String, _ value: Data, revision: UInt64) async throws -> UInt64 {
        try await update(key, value, revision: revision, ttl: nil)
    }

    /// Deletes a key, keeping its history. A delete marker is stored as the latest value.
    ///
    /// - Parameters:
    ///  - key: the key to delete.
    ///  - lastRevision: if set, the key is only deleted if its latest revision matches.
    public func delete(_ key: String, lastRevision: UInt64? = nil) async throws {
        try await remove(key, operation: .delete, lastRevision: lastRevision, ttl: nil)
    }

    /// Deletes a key and all of its history. A purge marker is stored as the latest value.
    ///
    /// - Parameters:
    ///  - lastRevision: if set, the key is only purged if its latest revision matches.
    ///  - ttl: time after which the purge marker is removed, requires ``KeyValueConfig/limitMarkerTTL``.
    public func purge(
        _ key: String, lastRevision: UInt64? = nil, ttl: TimeInterval? = nil
    ) async throws {
        try await remove(key, operation: .purge, lastRevision: lastRevision, ttl: ttl)
    }

    /// Retrieves the status of the bucket.
    public func status() async throws -> KeyValueStatus {
        KeyValueStatus(bucket: bucket, info: try await stream.info())
    }

    /// Retrieves the latest entry of a key or the entry at the given revision, including delete markers.
    internal func entry(_ key: String, revision: UInt64? = nil) async throws -> KeyValueEntry? {
        try Self.validate(key: key)
        let subject = prefix + key
        let direct = stream.info.config.allowDirect || stream.info.config.mirrorDirect
        let message: StreamMessage?
        switch (revision, direct) {
        case (let revision?, true):
            message = try await stream.getMessageDirect(sequence: revision)
        case (let revision?, false):
            message = try await stream.getMessage(sequence: revision)
        case (nil, true):
            message = try await stream.getMessageDirect(lastForSubject: subject)
        case (nil, false):
            message = try await stream.getMessage(lastForSubject: subject)
        }
        guard let message, message.subject == subject else {
            return nil
        }
        return KeyValueEntry(bucket: bucket, key: key, message: message)
    }

    private func update(
        _ key: String, _ value: Data, revision: UInt64, ttl: TimeInterval?
    ) async throws -> UInt64 {
        try Self.validate(key: key)
        let options = PublishOptions(
            expectedLastSubjectSequence: revision, ttl: ttl.map { .after($0) })
        do {
            return try await ctx.publish(putPrefix + key, message: value, options: options)
                .wait().seq
        } catch JetStreamError.PublishError.streamWrongLastSequence {
            throw JetStreamError.KeyValueError.wrongLastRevision(key)
        }
    }

    private func remove(
        _ key: String, operation: KeyValueOperation, lastRevision: UInt64?, ttl: TimeInterval?
    ) async throws {
        try Self.validate(key: key)
        var headers = NatsHeaderMap()
        headers.insert(.kvOperation, NatsHeaderValue(operation.rawValue))
        if operation == .purge {
            headers.insert(.natsRollup, NatsHeaderValue("sub"))
        }
        let options = PublishOptions(
            expectedLastSubjectSequence: lastRevision, ttl: ttl.map { .after($0) })
        do {
            _ = try await ctx.publish(
                putPrefix + key, message: Data(), headers: headers, options: options
            ).wait()
        } catch JetStreamError.PublishError.streamWrongLastSequence {
            throw JetStreamError.KeyValueError.wrongLastRevision(key)
        }
    }

    internal static func streamName(_ bucket: String) -> String {
        streamPrefix + bucket
    }

    internal static func validate(bucket: String) throws {
        guard !bucket.isEmpty,
            bucket.allSatisfy({
                $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "_" || $0 == "-")
            })
        else {
            throw JetStreamError.KeyValueError.invalidBucketName(bucket)
        }
    }

    internal static func validate(key: String, wildcards: Bool = false) throws {
        let allowed = { (c: Character) in
            c.isASCII
                && (c.isLetter || c.isNumber || "-/_=.".contains(c) || (wildcards && c == "*"))
        }
        var body = Substring(key)
        if wildcards, body.hasSuffix(">") {
            body = body.dropLast()
        }
        guard !key.isEmpty, !key.hasPrefix("."), !key.hasSuffix("."), !key.contains(".."),
            body.allSatisfy(allowed)
        else {
            throw JetStreamError.KeyValueError.invalidKey(key)
        }
    }
}

/// Operation which produced a ``KeyValueEntry``.
public enum KeyValueOperation: String, Sendable {
    /// The value was set.
    case put = "PUT"
    /// The key was deleted.
    case delete = "DEL"
    /// The key and its history were purged.
    case purge = "PURGE"
}

/// A value of a key at a given revision.
public struct KeyValueEntry: Sendable {
    /// Name of the bucket.
    public let bucket: String
    /// The key.
    public let key: String
    /// The value, empty for delete and purge markers.
    public let value: Data
    /// Revision of the value (stream sequence).
    public let revision: UInt64
    /// Time the value was stored.
    public let created: String
    /// Number of values stored after this one, set for entries delivered by watchers.
    public let delta: UInt64
    /// Operation which produced the entry.
    public let operation: KeyValueOperation

    internal init(bucket: String, key: String, message: StreamMessage, delta: UInt64 = 0) {
        self.init(
            bucket: bucket, key: key, value: message.payload, revision: message.sequence,
            created: message.time, delta: delta, headers: message.headers)
    }

    internal init(
        bucket: String, key: String, value: Data, revision: UInt64, created: String,
        delta: UInt64, headers: NatsHeaderMap?
    ) {
        self.bucket = bucket
        self.key = key
        self.value = value
        self.revision = revision
        self.created = created
        self.delta = delta
        self.operation = Self.operation(headers)
    }

    private static func operation(_ headers: NatsHeaderMap?) -> KeyValueOperation {
        if let operation = headers?[.kvOperation].flatMap({
            KeyValueOperation(rawValue: $0.description)
        }) {
            return operation
        }
        // markers placed by the server when keys are removed by limits
        switch headers?[.natsMarkerReason]?.description {
        case "MaxAge", "Purge":
            return .purge
        case "Remove":
            return .delete
        default:
            return .put
        }
    }
}

/// Status of a key-value bucket.
public struct KeyValueStatus: Sendable {
    /// Name of the bucket.
    public let bucket: String
    /// Info of the underlying stream.
    public let streamInfo: StreamInfo

    /// Number of values (including history and delete markers) stored in the bucket.
    public var values: UInt64 { streamInfo.state.messages }
    /// Number of historical values kept per key.
    public var history: Int64 { streamInfo.config.maxMsgsPerSubject }
    /// Time after which values expire.
    public var ttl: NanoTimeInterval { streamInfo.config.maxAge }
    /// Size of the bucket in bytes.
    public var bytes: UInt64 { streamInfo.state.bytes }
    /// Indicates whether the bucket is compressed.
    public var isCompressed: Bool { streamInfo.config.compression != .none }
    /// Time limit markers are kept for.
    public var limitMarkerTTL: NanoTimeInterval? { streamInfo.config.subjectDeleteMarkerTTL }
    /// Metadata of the bucket.
    public var metadata: [String: String]? { streamInfo.config.metadata }

    /// Configuration of the bucket.
    public var config: KeyValueConfig {
        let cfg = streamInfo.config
        return KeyValueConfig(
            bucket: bucket, description: cfg.description,
            maxValueSize: cfg.maxMsgSize > 0 ? cfg.maxMsgSize : nil,
            history: UInt8(clamping: cfg.maxMsgsPerSubject),
            ttl: cfg.maxAge.value > 0 ? cfg.maxAge.value : nil,
            maxBytes: cfg.maxBytes > 0 ? cfg.maxBytes : nil, storage: cfg.storage,
            replicas: cfg.replicas, placement: cfg.placement, rePublish: cfg.rePublish,
            mirror: cfg.mirror, sources: cfg.sources, compression: cfg.compression != .none,
            limitMarkerTTL: cfg.subjectDeleteMarkerTTL?.value, metadata: cfg.metadata)
    }

    init(bucket: String, info: StreamInfo) {
        self.bucket = bucket
        self.streamInfo = info
    }
}

extension NatsHeaderName {
    static let kvOperation = try! NatsHeaderName("KV-Operation")
}
