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

extension JetStreamContext {

    /// Retrieves a key-value bucket.
    ///
    /// - Parameter bucket: name of the bucket
    ///
    /// - Returns ``KeyValue`` or nil if the bucket does not exist.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/KeyValueError/invalidBucketName(_:)``: if the bucket name is not valid.
    /// > - ``JetStream/JetStreamError/KeyValueError/badBucket(_:)``: if the underlying stream is not a key-value store.
    /// > - `JetStreamError.RequestError`: if the request fails if e.g. JetStream is not enabled.
    public func keyValue(bucket: String) async throws -> KeyValue? {
        try KeyValue.validate(bucket: bucket)
        guard let stream = try await getStream(name: KeyValue.streamName(bucket)) else {
            return nil
        }
        guard stream.info.config.maxMsgsPerSubject >= 1 else {
            throw JetStreamError.KeyValueError.badBucket(bucket)
        }
        return KeyValue(ctx: self, stream: stream)
    }

    /// Creates a key-value bucket.
    ///
    /// - Parameter cfg: ``KeyValueConfig``
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/KeyValueError/bucketExists(_:)``: if a bucket with different configuration exists.
    /// > - ``JetStream/JetStreamError/KeyValueError``: if the configuration is not valid.
    /// > - `JetStreamError.StreamError`: if the underlying stream could not be created.
    public func createKeyValue(cfg: KeyValueConfig) async throws -> KeyValue {
        let streamConfig = try await streamConfig(for: cfg)
        do {
            return KeyValue(ctx: self, stream: try await createStream(cfg: streamConfig))
        } catch JetStreamError.StreamError.streamNameExist {
            throw JetStreamError.KeyValueError.bucketExists(cfg.bucket)
        }
    }

    /// Updates a key-value bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/KeyValueError/bucketNotFound(_:)``: if the bucket does not exist.
    /// > - `JetStreamError.StreamError`: if the underlying stream could not be updated.
    public func updateKeyValue(cfg: KeyValueConfig) async throws -> KeyValue {
        let streamConfig = try await streamConfig(for: cfg)
        do {
            return KeyValue(ctx: self, stream: try await updateStream(cfg: streamConfig))
        } catch JetStreamError.StreamError.streamNotFound {
            throw JetStreamError.KeyValueError.bucketNotFound(cfg.bucket)
        }
    }

    /// Creates a key-value bucket or updates it if it exists.
    public func createOrUpdateKeyValue(cfg: KeyValueConfig) async throws -> KeyValue {
        let streamConfig = try await streamConfig(for: cfg)
        return KeyValue(ctx: self, stream: try await createOrUpdateStream(cfg: streamConfig))
    }

    /// Lists the names of key-value buckets.
    public func keyValueStoreNames() async -> AsyncThrowingStream<String, Error> {
        let names = await streamNames(subject: "$KV.*.>")
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for try await name in names where name.hasPrefix(KeyValue.streamPrefix) {
                        continuation.yield(String(name.dropFirst(KeyValue.streamPrefix.count)))
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    /// Lists the statuses of key-value buckets.
    public func keyValueStores() async -> AsyncThrowingStream<KeyValueStatus, Error> {
        let infos = await streams(subject: "$KV.*.>")
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for try await info in infos
                    where info.config.name.hasPrefix(KeyValue.streamPrefix) {
                        let bucket = String(info.config.name.dropFirst(KeyValue.streamPrefix.count))
                        continuation.yield(KeyValueStatus(bucket: bucket, info: info))
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    /// Deletes a key-value bucket and all of its data.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/KeyValueError/bucketNotFound(_:)``: if the bucket does not exist.
    public func deleteKeyValue(bucket: String) async throws {
        try KeyValue.validate(bucket: bucket)
        do {
            try await deleteStream(name: KeyValue.streamName(bucket))
        } catch JetStreamError.StreamError.streamNotFound {
            throw JetStreamError.KeyValueError.bucketNotFound(bucket)
        }
    }

    private func streamConfig(for cfg: KeyValueConfig) async throws -> StreamConfig {
        try KeyValue.validate(bucket: cfg.bucket)
        guard cfg.history <= KeyValueConfig.maxHistory else {
            throw JetStreamError.KeyValueError.historyTooLarge
        }
        if cfg.limitMarkerTTL != nil {
            guard try await accountInfo().api?.level ?? 0 >= 1 else {
                throw JetStreamError.KeyValueError.limitMarkerTTLNotSupported
            }
        }
        let duplicateWindow = min(cfg.ttl.flatMap { $0 > 0 ? $0 : nil } ?? 120, 120)
        var streamConfig = StreamConfig(
            name: KeyValue.streamName(cfg.bucket), description: cfg.description,
            maxConsumers: -1, maxMsgs: -1, maxBytes: cfg.maxBytes ?? -1, discard: .new,
            maxAge: NanoTimeInterval(cfg.ttl ?? 0), maxMsgsPerSubject: Int64(max(cfg.history, 1)),
            maxMsgSize: cfg.maxValueSize ?? -1, storage: cfg.storage, replicas: cfg.replicas,
            duplicates: NanoTimeInterval(duplicateWindow), placement: cfg.placement,
            denyDelete: true, allowRollup: true, compression: cfg.compression ? .s2 : .none,
            rePublish: cfg.rePublish, allowDirect: true, metadata: cfg.metadata,
            allowMsgTTL: cfg.limitMarkerTTL != nil ? true : nil,
            subjectDeleteMarkerTTL: cfg.limitMarkerTTL.map { NanoTimeInterval($0) })

        if let mirror = cfg.mirror {
            streamConfig.mirror = Self.prefixedSource(mirror)
            streamConfig.mirrorDirect = true
        } else {
            streamConfig.subjects = ["$KV.\(cfg.bucket).>"]
            streamConfig.sources = cfg.sources?.map { source in
                if source.subjectTransforms?.isEmpty == false {
                    return source
                }
                let sourceBucket =
                    source.name.hasPrefix(KeyValue.streamPrefix)
                    ? String(source.name.dropFirst(KeyValue.streamPrefix.count)) : source.name
                // sources are written to the subjects of this bucket
                let transforms =
                    source.external == nil || sourceBucket != cfg.bucket
                    ? [
                        SubjectTransformConfig(
                            source: "$KV.\(sourceBucket).>", destination: "$KV.\(cfg.bucket).>")
                    ] : nil
                return StreamSource(
                    name: KeyValue.streamName(sourceBucket), optStartSeq: source.optStartSeq,
                    optStartTime: source.optStartTime, filterSubject: source.filterSubject,
                    subjectTransforms: transforms, external: source.external,
                    consumer: source.consumer, domain: source.domain)
            }
        }
        return streamConfig
    }

    private static func prefixedSource(_ source: StreamSource) -> StreamSource {
        guard !source.name.hasPrefix(KeyValue.streamPrefix) else {
            return source
        }
        return StreamSource(
            name: KeyValue.streamName(source.name), optStartSeq: source.optStartSeq,
            optStartTime: source.optStartTime, filterSubject: source.filterSubject,
            subjectTransforms: source.subjectTransforms, external: source.external,
            consumer: source.consumer, domain: source.domain)
    }
}

/// Configuration of a key-value bucket.
public struct KeyValueConfig: Sendable {
    /// Maximum number of historical values kept per key.
    public static let maxHistory: UInt8 = 64

    /// Name of the bucket.
    public var bucket: String
    /// Description of the bucket.
    public var description: String?
    /// Maximum size of a single value.
    public var maxValueSize: Int32?
    /// Number of historical values kept per key, 1 by default.
    public var history: UInt8
    /// Time after which values expire.
    public var ttl: TimeInterval?
    /// Maximum total size of the bucket.
    public var maxBytes: Int64?
    /// Storage type of the bucket.
    public var storage: StorageType
    /// Number of replicas in clustered JetStream.
    public var replicas: Int
    /// Placement of the bucket in clustered JetStream.
    public var placement: Placement?
    /// Republishes values to the given subject after they are stored.
    public var rePublish: RePublish?
    /// Bucket mirrored by this bucket. Mirrors are read only.
    public var mirror: StreamSource?
    /// Buckets whose values are sourced into this bucket.
    public var sources: [StreamSource]?
    /// Enables S2 compression of the bucket.
    public var compression: Bool
    /// Enables markers for keys removed by limits (e.g. ttl), kept for the given time.
    /// Required for per-key TTLs, requires nats-server v2.11.0 or later.
    public var limitMarkerTTL: TimeInterval?
    /// Metadata of the bucket.
    public var metadata: [String: String]?

    /// Creates a value with the given properties, see the properties of the same name.
    public init(
        bucket: String, description: String? = nil, maxValueSize: Int32? = nil,
        history: UInt8 = 1, ttl: TimeInterval? = nil, maxBytes: Int64? = nil,
        storage: StorageType = .file, replicas: Int = 1, placement: Placement? = nil,
        rePublish: RePublish? = nil, mirror: StreamSource? = nil, sources: [StreamSource]? = nil,
        compression: Bool = false, limitMarkerTTL: TimeInterval? = nil,
        metadata: [String: String]? = nil
    ) {
        self.bucket = bucket
        self.description = description
        self.maxValueSize = maxValueSize
        self.history = history
        self.ttl = ttl
        self.maxBytes = maxBytes
        self.storage = storage
        self.replicas = replicas
        self.placement = placement
        self.rePublish = rePublish
        self.mirror = mirror
        self.sources = sources
        self.compression = compression
        self.limitMarkerTTL = limitMarkerTTL
        self.metadata = metadata
    }
}
