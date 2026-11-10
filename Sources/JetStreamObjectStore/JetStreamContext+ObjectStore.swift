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

    /// Retrieves an object store bucket.
    ///
    /// - Returns ``ObjectStore`` or nil if the bucket does not exist.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/ObjectStoreError/invalidStoreName(_:)``: if the bucket name is not valid.
    /// > - `JetStreamError.RequestError`: if the request fails if e.g. JetStream is not enabled.
    public func objectStore(bucket: String) async throws -> ObjectStore? {
        try ObjectStore.validate(bucket: bucket)
        guard let stream = try await getStream(name: ObjectStore.streamName(bucket)) else {
            return nil
        }
        return ObjectStore(ctx: self, bucket: bucket, stream: stream)
    }

    /// Creates an object store bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/ObjectStoreError/bucketExists(_:)``: if a bucket with different configuration exists.
    /// > - `JetStreamError.StreamError`: if the underlying stream could not be created.
    public func createObjectStore(cfg: ObjectStoreConfig) async throws -> ObjectStore {
        let streamConfig = try cfg.streamConfig()
        do {
            let stream = try await createStream(cfg: streamConfig)
            return ObjectStore(ctx: self, bucket: cfg.bucket, stream: stream)
        } catch JetStreamError.StreamError.streamNameExist {
            throw JetStreamError.ObjectStoreError.bucketExists(cfg.bucket)
        }
    }

    /// Updates an object store bucket.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/ObjectStoreError/bucketNotFound(_:)``: if the bucket does not exist.
    public func updateObjectStore(cfg: ObjectStoreConfig) async throws -> ObjectStore {
        let streamConfig = try cfg.streamConfig()
        do {
            let stream = try await updateStream(cfg: streamConfig)
            return ObjectStore(ctx: self, bucket: cfg.bucket, stream: stream)
        } catch JetStreamError.StreamError.streamNotFound {
            throw JetStreamError.ObjectStoreError.bucketNotFound(cfg.bucket)
        }
    }

    /// Creates an object store bucket or updates it if it exists.
    public func createOrUpdateObjectStore(cfg: ObjectStoreConfig) async throws -> ObjectStore {
        let stream = try await createOrUpdateStream(cfg: try cfg.streamConfig())
        return ObjectStore(ctx: self, bucket: cfg.bucket, stream: stream)
    }

    /// Lists the names of object store buckets.
    public func objectStoreNames() async -> AsyncThrowingStream<String, Error> {
        let names = await streamNames(subject: "$O.*.C.>")
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for try await name in names where name.hasPrefix(ObjectStore.streamPrefix) {
                        continuation.yield(String(name.dropFirst(ObjectStore.streamPrefix.count)))
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    /// Lists the statuses of object store buckets.
    public func objectStores() async -> AsyncThrowingStream<ObjectStoreStatus, Error> {
        let infos = await streams(subject: "$O.*.C.>")
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for try await info in infos
                    where info.config.name.hasPrefix(ObjectStore.streamPrefix) {
                        let bucket = String(
                            info.config.name.dropFirst(ObjectStore.streamPrefix.count))
                        continuation.yield(ObjectStoreStatus(bucket: bucket, info: info))
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    /// Deletes an object store bucket and all of its objects.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/ObjectStoreError/bucketNotFound(_:)``: if the bucket does not exist.
    public func deleteObjectStore(bucket: String) async throws {
        try ObjectStore.validate(bucket: bucket)
        do {
            try await deleteStream(name: ObjectStore.streamName(bucket))
        } catch JetStreamError.StreamError.streamNotFound {
            throw JetStreamError.ObjectStoreError.bucketNotFound(bucket)
        }
    }
}

/// Configuration of an object store bucket.
public struct ObjectStoreConfig: Sendable {
    /// Name of the bucket.
    public var bucket: String
    /// Description of the bucket.
    public var description: String?
    /// Time after which objects expire.
    public var ttl: TimeInterval?
    /// Maximum total size of the bucket.
    public var maxBytes: Int64?
    /// Storage type of the bucket.
    public var storage: StorageType
    /// Number of replicas in clustered JetStream.
    public var replicas: Int
    /// Placement of the bucket in clustered JetStream.
    public var placement: Placement?
    /// Enables S2 compression of the bucket.
    public var compression: Bool
    /// Metadata of the bucket.
    public var metadata: [String: String]?

    /// Creates a value with the given properties, see the properties of the same name.
    public init(
        bucket: String, description: String? = nil, ttl: TimeInterval? = nil,
        maxBytes: Int64? = nil, storage: StorageType = .file, replicas: Int = 1,
        placement: Placement? = nil, compression: Bool = false, metadata: [String: String]? = nil
    ) {
        self.bucket = bucket
        self.description = description
        self.ttl = ttl
        self.maxBytes = maxBytes
        self.storage = storage
        self.replicas = replicas
        self.placement = placement
        self.compression = compression
        self.metadata = metadata
    }

    internal func streamConfig() throws -> StreamConfig {
        try ObjectStore.validate(bucket: bucket)
        return StreamConfig(
            name: ObjectStore.streamName(bucket), description: description,
            subjects: ["$O.\(bucket).C.>", "$O.\(bucket).M.>"], maxBytes: maxBytes ?? -1,
            discard: .new, maxAge: NanoTimeInterval(ttl ?? 0), storage: storage,
            replicas: replicas, placement: placement, allowRollup: true,
            compression: compression ? .s2 : .none, allowDirect: true, metadata: metadata)
    }
}
