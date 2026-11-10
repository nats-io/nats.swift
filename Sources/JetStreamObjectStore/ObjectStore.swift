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

import CryptoKit
import Foundation
import JetStream
import Nats
import Nuid

/// An object store backed by a JetStream stream (`OBJ_<bucket>`).
/// Objects are split into chunks stored on `$O.<bucket>.C.<nuid>`,
/// with their metadata stored on `$O.<bucket>.M.<encoded name>`.
public final class ObjectStore: Sendable {
    /// Name of the bucket.
    public let bucket: String
    internal let ctx: JetStreamContext
    internal let stream: JetStream.Stream

    internal static let streamPrefix = "OBJ_"
    /// Default size of object chunks.
    public static let defaultChunkSize: UInt32 = 128 * 1024
    /// Maximum number of chunks published without waiting for acknowledgements.
    private static let maxPendingAcks = 64

    init(ctx: JetStreamContext, bucket: String, stream: JetStream.Stream) {
        self.ctx = ctx
        self.bucket = bucket
        self.stream = stream
    }

    /// Stores an object read from a sequence of data, replacing an existing object with the same name.
    ///
    /// - Parameters:
    ///  - meta: ``ObjectMeta`` of the object, ``ObjectMetaOptions/chunkSize`` sets the size of chunks.
    ///  - data: the content of the object.
    ///
    /// - Returns ``ObjectInfo`` of the stored object.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/ObjectStoreError/linkNotAllowed``: if the meta contains a link.
    /// > - `JetStreamError.PublishError`: if a chunk could not be stored.
    @discardableResult
    public func put<S: AsyncSequence>(_ meta: ObjectMeta, data: S) async throws -> ObjectInfo
    where S.Element == Data {
        try Self.validate(name: meta.name)
        guard meta.options?.link == nil else {
            throw JetStreamError.ObjectStoreError.linkNotAllowed
        }
        let chunkSize = Int(
            meta.options?.chunkSize.flatMap { $0 > 0 ? $0 : nil } ?? Self.defaultChunkSize)
        let existing = try await info(meta.name, showDeleted: true)

        let nuid = nextNuid()
        let chunkSubject = "$O.\(bucket).C.\(nuid)"
        var hasher = SHA256()
        var size: UInt64 = 0
        var chunks: UInt32 = 0
        var pending: [AckFuture] = []
        do {
            var buffer = Data()
            func publish(_ chunk: Data) async throws {
                hasher.update(data: chunk)
                size += UInt64(chunk.count)
                chunks += 1
                pending.append(try await ctx.publish(chunkSubject, message: chunk))
                if pending.count >= Self.maxPendingAcks {
                    _ = try await pending.removeFirst().wait()
                }
            }
            for try await data in data {
                buffer.append(data)
                while buffer.count >= chunkSize {
                    try await publish(buffer.prefix(chunkSize))
                    buffer = buffer.dropFirst(chunkSize)
                }
            }
            if !buffer.isEmpty {
                try await publish(buffer)
            }
            for ack in pending {
                _ = try await ack.wait()
            }
            pending = []

            var meta = meta
            if meta.options?.chunkSize == nil {
                meta.options = ObjectMetaOptions(
                    link: meta.options?.link, chunkSize: UInt32(chunkSize))
            }
            let digest = Data(hasher.finalize())
            let info = ObjectInfo(
                meta: meta, bucket: bucket, nuid: nuid, size: size, chunks: chunks,
                digest: "SHA-256=" + Self.base64URL(digest))
            try await publishMeta(info)

            // remove chunks of the replaced object
            if let existing, existing.deleted != true {
                _ = try? await stream.purge(subject: "$O.\(bucket).C.\(existing.nuid)")
            }
            return try await self.info(meta.name) ?? info
        } catch {
            for ack in pending {
                _ = try? await ack.wait()
            }
            _ = try? await stream.purge(subject: chunkSubject)
            throw error
        }
    }

    /// Stores an object. See the generic `put(_:data:)`.
    @discardableResult
    public func put(_ meta: ObjectMeta, data: Data) async throws -> ObjectInfo {
        try await put(
            meta,
            data: AsyncStream { continuation in
                continuation.yield(data)
                continuation.finish()
            })
    }

    /// Stores an object with the given name. See the generic `put(_:data:)`.
    @discardableResult
    public func put(name: String, data: Data) async throws -> ObjectInfo {
        try await put(ObjectMeta(name: name), data: data)
    }

    /// Stores a string with the given name. See the generic `put(_:data:)`.
    @discardableResult
    public func put(name: String, string: String) async throws -> ObjectInfo {
        try await put(ObjectMeta(name: name), data: Data(string.utf8))
    }

    /// Stores the content of a file, read in chunks.
    ///
    /// - Parameters:
    ///  - url: URL of the file.
    ///  - name: name of the object, the file name by default.
    @discardableResult
    public func putFile(_ url: URL, name: String? = nil) async throws -> ObjectInfo {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        let chunks = AsyncThrowingStream<Data, Error> { continuation in
            do {
                while let chunk = try handle.read(upToCount: Int(Self.defaultChunkSize)),
                    !chunk.isEmpty
                {
                    continuation.yield(chunk)
                }
                continuation.finish()
            } catch {
                continuation.finish(throwing: error)
            }
        }
        return try await put(ObjectMeta(name: name ?? url.lastPathComponent), data: chunks)
    }

    /// Writes the content of an object to a file, chunk by chunk.
    ///
    /// - Returns ``ObjectInfo`` of the object, or nil if it does not exist (no file is written).
    @discardableResult
    public func getFile(_ name: String, to url: URL) async throws -> ObjectInfo? {
        guard let result = try await get(name) else {
            return nil
        }
        FileManager.default.createFile(atPath: url.path, contents: nil)
        let handle = try FileHandle(forWritingTo: url)
        defer { try? handle.close() }
        for try await chunk in result {
            try handle.write(contentsOf: chunk)
        }
        return result.info
    }

    /// Retrieves an object, following links.
    ///
    /// - Returns ``ObjectResult`` with the object info and an `AsyncSequence` of its chunks,
    /// or nil if the object does not exist or was deleted.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/ObjectStoreError/digestMismatch(_:)`` (while iterating): if the content is corrupted.
    public func get(_ name: String, showDeleted: Bool = false) async throws -> ObjectResult? {
        guard let info = try await info(name, showDeleted: showDeleted) else {
            return nil
        }
        if let link = info.meta.options?.link {
            guard let target = link.name else {
                throw JetStreamError.ObjectStoreError.cantGetBucket(name)
            }
            if link.bucket == bucket {
                return try await get(target, showDeleted: showDeleted)
            }
            guard let linked = try await ctx.objectStore(bucket: link.bucket) else {
                throw JetStreamError.ObjectStoreError.bucketNotFound(link.bucket)
            }
            return try await linked.get(target, showDeleted: showDeleted)
        }
        return ObjectResult(store: self, info: info)
    }

    /// Retrieves the content of an object. See ``get(_:showDeleted:)``.
    public func getData(_ name: String) async throws -> Data? {
        guard let result = try await get(name) else {
            return nil
        }
        var data = Data()
        for try await chunk in result {
            data.append(chunk)
        }
        return data
    }

    /// Retrieves the content of an object as a string. See ``get(_:showDeleted:)``.
    public func getString(_ name: String) async throws -> String? {
        try await getData(name).map { String(decoding: $0, as: UTF8.self) }
    }

    /// Retrieves the info of an object.
    ///
    /// - Parameters:
    ///  - name: name of the object.
    ///  - showDeleted: returns the info of deleted objects as well.
    ///
    /// - Returns ``ObjectInfo`` or nil if the object does not exist (or was deleted).
    public func info(_ name: String, showDeleted: Bool = false) async throws -> ObjectInfo? {
        try Self.validate(name: name)
        let subject = "$O.\(bucket).M.\(Self.encode(name: name))"
        guard let message = try await stream.getMessageDirect(lastForSubject: subject) else {
            return nil
        }
        var info: ObjectInfo
        do {
            info = try JSONDecoder().decode(ObjectInfo.self, from: message.payload)
        } catch {
            throw JetStreamError.ObjectStoreError.badObjectMeta(name)
        }
        info.modified = message.time
        return info.deleted == true && !showDeleted ? nil : info
    }

    /// Deletes an object, keeping a delete marker.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/ObjectStoreError/objectNotFound(_:)``: if the object does not exist.
    public func delete(_ name: String) async throws {
        guard var info = try await info(name, showDeleted: true) else {
            throw JetStreamError.ObjectStoreError.objectNotFound(name)
        }
        info.deleted = true
        info.size = 0
        info.chunks = 0
        info.digest = nil
        try await publishMeta(info)
        _ = try await stream.purge(subject: "$O.\(bucket).C.\(info.nuid)")
    }

    /// Lists the objects of the bucket.
    ///
    /// - Parameter showDeleted: lists deleted objects as well.
    ///
    /// - Returns the infos of the objects, empty if there are none.
    public func list(showDeleted: Bool = false) async throws -> [ObjectInfo] {
        let watcher = try await watch(options: ObjectWatchOptions(ignoreDeletes: !showDeleted))
        var infos: [ObjectInfo] = []
        for try await info in watcher {
            guard let info else {
                break
            }
            infos.append(info)
        }
        await watcher.stop()
        return infos
    }

    /// Retrieves the status of the bucket.
    public func status() async throws -> ObjectStoreStatus {
        ObjectStoreStatus(bucket: bucket, info: try await stream.info())
    }

    internal func publishMeta(_ info: ObjectInfo) async throws {
        var headers = NatsHeaderMap()
        headers.insert(.natsRollup, NatsHeaderValue("sub"))
        let subject = "$O.\(bucket).M.\(Self.encode(name: info.meta.name))"
        _ = try await ctx.publish(
            subject, message: try JSONEncoder().encode(info), headers: headers
        ).wait()
    }

    internal static func streamName(_ bucket: String) -> String {
        streamPrefix + bucket
    }

    internal static func encode(name: String) -> String {
        base64URL(Data(name.utf8))
    }

    internal static func base64URL(_ data: Data) -> String {
        data.base64EncodedString().replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
    }

    internal static func format(nanos: String) -> String {
        guard let nanos = Double(nanos) else {
            return nanos
        }
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: Date(timeIntervalSince1970: nanos / 1_000_000_000))
    }

    internal static func validate(bucket: String) throws {
        guard !bucket.isEmpty,
            bucket.allSatisfy({
                $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "_" || $0 == "-")
            })
        else {
            throw JetStreamError.ObjectStoreError.invalidStoreName(bucket)
        }
    }

    internal static func validate(name: String) throws {
        guard !name.isEmpty else {
            throw JetStreamError.ObjectStoreError.nameRequired
        }
    }
}

/// Content of an object retrieved with ``ObjectStore/get(_:showDeleted:)``, delivered in chunks.
/// The digest of the content is verified when the last chunk is received.
public struct ObjectResult: AsyncSequence, Sendable {
    public typealias Element = Data

    /// Info of the object.
    public let info: ObjectInfo
    private let store: ObjectStore

    init(store: ObjectStore, info: ObjectInfo) {
        self.store = store
        self.info = info
    }

    public func makeAsyncIterator() -> AsyncThrowingStream<Data, Error>.Iterator {
        let store = store
        let info = info
        return AsyncThrowingStream<Data, Error> { continuation in
            let task = Task {
                guard info.size > 0 else {
                    continuation.finish()
                    return
                }
                let ordered = store.ctx.orderedConsumer(
                    stream: store.stream.info.config.name,
                    config: OrderedConsumerConfig(
                        filterSubjects: ["$O.\(store.bucket).C.\(info.nuid)"]))
                var hasher = SHA256()
                do {
                    let messages = try await ordered.consume(
                        options: ConsumeOptions(maxMessages: 64))
                    for try await message in messages {
                        let chunk = message.payload ?? Data()
                        hasher.update(data: chunk)
                        continuation.yield(chunk)
                        if try message.metadata().pending == 0 {
                            break
                        }
                    }
                    await messages.stop()
                    let digest = "SHA-256=" + ObjectStore.base64URL(Data(hasher.finalize()))
                    guard digest == info.digest else {
                        throw JetStreamError.ObjectStoreError.digestMismatch(info.meta.name)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }.makeAsyncIterator()
    }
}

/// Metadata of an object, set when storing it.
public struct ObjectMeta: Codable, Equatable, Sendable {
    /// Name of the object.
    public var name: String
    /// Description of the object.
    public var description: String?
    /// Headers of the object.
    public var headers: [String: [String]]?
    /// Application-defined key-value pairs.
    public var metadata: [String: String]?
    /// Options of the object.
    public var options: ObjectMetaOptions?

    /// Creates a value with the given properties, see the properties of the same name.
    public init(
        name: String, description: String? = nil, headers: [String: [String]]? = nil,
        metadata: [String: String]? = nil, options: ObjectMetaOptions? = nil
    ) {
        self.name = name
        self.description = description
        self.headers = headers
        self.metadata = metadata
        self.options = options
    }
}

/// Options of an object.
public struct ObjectMetaOptions: Codable, Equatable, Sendable {
    /// Link to another object or bucket, set for links only.
    public var link: ObjectLink?
    /// Size of chunks the object is split into.
    public var chunkSize: UInt32?

    /// Creates a value with the given properties, see the properties of the same name.
    public init(link: ObjectLink? = nil, chunkSize: UInt32? = nil) {
        self.link = link
        self.chunkSize = chunkSize
    }

    enum CodingKeys: String, CodingKey {
        case link
        case chunkSize = "max_chunk_size"
    }
}

/// Link to an object or a whole bucket.
public struct ObjectLink: Codable, Equatable, Sendable {
    /// Name of the linked bucket.
    public var bucket: String
    /// Name of the linked object, nil for links to buckets.
    public var name: String?

    /// Creates a value with the given properties, see the properties of the same name.
    public init(bucket: String, name: String? = nil) {
        self.bucket = bucket
        self.name = name
    }
}

/// Info of a stored object.
public struct ObjectInfo: Codable, Sendable {
    /// Metadata of the object (encoded inline).
    public var meta: ObjectMeta
    /// Name of the bucket.
    public var bucket: String
    /// Unique identifier of the stored content.
    public var nuid: String
    /// Size of the object in bytes.
    public var size: UInt64
    /// Time the object was last modified.
    public var modified: String
    /// Number of chunks of the object.
    public var chunks: UInt32
    /// Digest of the content (`SHA-256=<base64url>`).
    public var digest: String?
    /// Indicates whether the object was deleted.
    public var deleted: Bool?

    /// Name of the object.
    public var name: String { meta.name }

    enum CodingKeys: String, CodingKey {
        case bucket, nuid, size, chunks, digest, deleted
        case modified = "mtime"
    }

    init(
        meta: ObjectMeta, bucket: String, nuid: String, size: UInt64, chunks: UInt32,
        digest: String?
    ) {
        self.meta = meta
        self.bucket = bucket
        self.nuid = nuid
        self.size = size
        self.modified = ""
        self.chunks = chunks
        self.digest = digest
    }

    public init(from decoder: Decoder) throws {
        meta = try ObjectMeta(from: decoder)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        bucket = try container.decode(String.self, forKey: .bucket)
        nuid = try container.decode(String.self, forKey: .nuid)
        size = try container.decode(UInt64.self, forKey: .size)
        modified = try container.decodeIfPresent(String.self, forKey: .modified) ?? ""
        chunks = try container.decode(UInt32.self, forKey: .chunks)
        digest = try container.decodeIfPresent(String.self, forKey: .digest)
        deleted = try container.decodeIfPresent(Bool.self, forKey: .deleted)
    }

    public func encode(to encoder: Encoder) throws {
        try meta.encode(to: encoder)
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(bucket, forKey: .bucket)
        try container.encode(nuid, forKey: .nuid)
        try container.encode(size, forKey: .size)
        // the modification time is set by the server
        try container.encode("0001-01-01T00:00:00Z", forKey: .modified)
        try container.encode(chunks, forKey: .chunks)
        try container.encodeIfPresent(digest, forKey: .digest)
        try container.encodeIfPresent(deleted, forKey: .deleted)
    }
}

/// Status of an object store bucket.
public struct ObjectStoreStatus: Sendable {
    /// Name of the bucket.
    public let bucket: String
    /// Info of the underlying stream.
    public let streamInfo: StreamInfo

    /// Description of the bucket.
    public var description: String? { streamInfo.config.description }
    /// Time after which objects expire.
    public var ttl: NanoTimeInterval { streamInfo.config.maxAge }
    /// Storage type of the bucket.
    public var storage: StorageType { streamInfo.config.storage }
    /// Number of replicas of the bucket.
    public var replicas: Int { streamInfo.config.replicas }
    /// Indicates whether the bucket is sealed.
    public var sealed: Bool { streamInfo.config.sealed ?? false }
    /// Size of the bucket in bytes.
    public var size: UInt64 { streamInfo.state.bytes }
    /// Indicates whether the bucket is compressed.
    public var isCompressed: Bool { streamInfo.config.compression != .none }
    /// Metadata of the bucket.
    public var metadata: [String: String]? { streamInfo.config.metadata }

    init(bucket: String, info: StreamInfo) {
        self.bucket = bucket
        self.streamInfo = info
    }
}
