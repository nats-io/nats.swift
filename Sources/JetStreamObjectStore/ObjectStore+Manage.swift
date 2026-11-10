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
import Nuid

extension ObjectStore {

    /// Watches the objects of the bucket for changes.
    /// The infos of existing objects are delivered first, followed by a `nil` element
    /// marking that all initial infos were delivered, followed by live updates.
    public func watch(
        options: ObjectWatchOptions = ObjectWatchOptions()
    ) async throws -> ObjectWatcher {
        var config = OrderedConsumerConfig(
            filterSubjects: ["$O.\(bucket).M.>"],
            deliverPolicy: options.includeHistory ? .all : .lastPerSubject)
        if options.updatesOnly {
            config.deliverPolicy = .new
        }
        let ordered = ctx.orderedConsumer(stream: stream.info.config.name, config: config)
        let messages = try await ordered.consume()
        return ObjectWatcher(
            messages: messages,
            initialPending: options.updatesOnly ? nil : ordered.cachedInfo?.numPending ?? 0,
            ignoreDeletes: options.ignoreDeletes)
    }

    /// Updates the metadata of an object, which may rename it.
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/ObjectStoreError/updateMetaDeleted(_:)``: if the object does not exist or was deleted.
    /// > - ``JetStream/JetStreamError/ObjectStoreError/objectAlreadyExists(_:)``: if the new name is taken.
    public func updateMeta(_ name: String, meta: ObjectMeta) async throws {
        guard var info = try await info(name) else {
            throw JetStreamError.ObjectStoreError.updateMetaDeleted(name)
        }
        if name != meta.name, try await self.info(meta.name) != nil {
            throw JetStreamError.ObjectStoreError.objectAlreadyExists(meta.name)
        }
        info.meta.name = meta.name
        info.meta.description = meta.description
        info.meta.headers = meta.headers
        info.meta.metadata = meta.metadata
        try await publishMeta(info)
        if name != meta.name {
            _ = try await stream.purge(subject: "$O.\(bucket).M.\(Self.encode(name: name))")
        }
    }

    /// Adds a link to an object, which may be in a different bucket.
    ///
    /// - Parameters:
    ///  - name: name of the link
    ///  - object: info of the linked object
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/ObjectStoreError/noLinkToDeleted``, ``JetStream/JetStreamError/ObjectStoreError/noLinkToLink``:
    /// > if the object is deleted or is a link.
    /// > - ``JetStream/JetStreamError/ObjectStoreError/objectAlreadyExists(_:)``: if an object (not a link) exists with the name.
    @discardableResult
    public func addLink(_ name: String, to object: ObjectInfo) async throws -> ObjectInfo {
        guard object.deleted != true else {
            throw JetStreamError.ObjectStoreError.noLinkToDeleted
        }
        guard object.meta.options?.link == nil else {
            throw JetStreamError.ObjectStoreError.noLinkToLink
        }
        return try await addLink(name, ObjectLink(bucket: object.bucket, name: object.name))
    }

    /// Adds a link to another bucket.
    /// See ``addLink(_:to:)``.
    @discardableResult
    public func addBucketLink(_ name: String, to store: ObjectStore) async throws -> ObjectInfo {
        try await addLink(name, ObjectLink(bucket: store.bucket))
    }

    /// Seals the bucket, after which objects can no longer be added, changed or deleted.
    public func seal() async throws {
        var config = try await stream.info().config
        config.sealed = true
        _ = try await ctx.updateStream(cfg: config)
    }

    private func addLink(_ name: String, _ link: ObjectLink) async throws -> ObjectInfo {
        try Self.validate(name: name)
        if let existing = try await info(name, showDeleted: true),
            existing.meta.options?.link == nil
        {
            throw JetStreamError.ObjectStoreError.objectAlreadyExists(name)
        }
        let info = ObjectInfo(
            meta: ObjectMeta(name: name, options: ObjectMetaOptions(link: link)), bucket: bucket,
            nuid: nextNuid(), size: 0, chunks: 0, digest: nil)
        try await publishMeta(info)
        return try await self.info(name) ?? info
    }
}

/// Options of ``ObjectStore/watch(options:)``.
public struct ObjectWatchOptions: Sendable {
    /// Delivers all stored infos instead of only the latest one per object.
    public var includeHistory: Bool
    /// Skips deleted objects.
    public var ignoreDeletes: Bool
    /// Delivers only updates made after the watcher was created (no initial infos and `nil` marker).
    public var updatesOnly: Bool

    /// Creates a value with the given properties, see the properties of the same name.
    public init(
        includeHistory: Bool = false, ignoreDeletes: Bool = false, updatesOnly: Bool = false
    ) {
        self.includeHistory = includeHistory
        self.ignoreDeletes = ignoreDeletes
        self.updatesOnly = updatesOnly
    }
}

/// Delivers changes of objects, see ``ObjectStore/watch(options:)``.
/// A `nil` element marks that all initial infos were delivered.
public final class ObjectWatcher: AsyncSequence, Sendable {
    public typealias Element = ObjectInfo?

    private let messages: OrderedConsumeResult
    private let stream: AsyncThrowingStream<ObjectInfo?, Error>

    init(messages: OrderedConsumeResult, initialPending: UInt64?, ignoreDeletes: Bool) {
        self.messages = messages
        self.stream = AsyncThrowingStream { continuation in
            let task = Task {
                var initDone = initialPending == nil || initialPending == 0
                if initialPending == 0 {
                    continuation.yield(nil)
                }
                do {
                    for try await message in messages {
                        let meta = try message.metadata()
                        if var info = try? JSONDecoder().decode(
                            ObjectInfo.self, from: message.payload ?? Data()),
                            !ignoreDeletes || info.deleted != true
                        {
                            info.modified = ObjectStore.format(nanos: meta.timestamp)
                            continuation.yield(info)
                        }
                        if !initDone && meta.pending == 0 {
                            initDone = true
                            continuation.yield(nil)
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in
                task.cancel()
                Task { await messages.stop() }
            }
        }
    }

    /// Stops watching.
    public func stop() async {
        await messages.stop()
    }

    public func makeAsyncIterator() -> AsyncThrowingStream<ObjectInfo?, Error>.Iterator {
        stream.makeAsyncIterator()
    }
}
