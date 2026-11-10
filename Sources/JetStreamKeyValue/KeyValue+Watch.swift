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

extension KeyValue {

    /// Watches keys of the bucket for changes.
    /// The latest values of matching keys are delivered first, followed by a `nil` element
    /// marking that all initial values were delivered, followed by live updates.
    ///
    /// - Parameters:
    ///  - keys: keys to watch, may contain wildcards (`*` and `>`).
    ///  - options: ``KeyValueWatchOptions``
    ///
    /// > **Throws:**
    /// > - ``JetStream/JetStreamError/KeyValueError/invalidKey(_:)``: if a key is not valid.
    public func watch(
        _ keys: [String], options: KeyValueWatchOptions = KeyValueWatchOptions()
    ) async throws -> KeyValueWatcher {
        for key in keys {
            try Self.validate(key: key, wildcards: true)
        }
        let filters = (keys.isEmpty ? [">"] : keys).map { prefix + $0 }
        var config = OrderedConsumerConfig(
            filterSubjects: filters,
            deliverPolicy: options.includeHistory ? .all : .lastPerSubject,
            headersOnly: options.metaOnly ? true : nil)
        if options.updatesOnly {
            config.deliverPolicy = .new
        }
        if let revision = options.resumeFromRevision {
            config.deliverPolicy = .byStartSequence
            config.optStartSeq = revision
        }
        let ordered = ctx.orderedConsumer(stream: stream.info.config.name, config: config)
        let messages = try await ordered.consume()
        return KeyValueWatcher(
            kv: self, messages: messages,
            initialPending: options.updatesOnly ? nil : ordered.cachedInfo?.numPending ?? 0,
            ignoreDeletes: options.ignoreDeletes)
    }

    /// Watches a key (which may contain wildcards) for changes. See ``watch(_:options:)-([String],_)``.
    public func watch(
        _ key: String, options: KeyValueWatchOptions = KeyValueWatchOptions()
    ) async throws -> KeyValueWatcher {
        try await watch([key], options: options)
    }

    /// Watches all keys of the bucket for changes. See ``watch(_:options:)-([String],_)``.
    public func watchAll(
        options: KeyValueWatchOptions = KeyValueWatchOptions()
    ) async throws -> KeyValueWatcher {
        try await watch([], options: options)
    }

    /// Lists the keys of the bucket which are not deleted, optionally filtered.
    ///
    /// - Parameter filters: keys to match, may contain wildcards (`*` and `>`).
    ///
    /// - Returns sorted keys, empty if there are none.
    public func keys(filters: [String] = []) async throws -> [String] {
        var keys: [String] = []
        for try await key in try await listKeys(filters: filters) {
            keys.append(key)
        }
        return Array(Set(keys)).sorted()
    }

    /// Lists the keys of the bucket which are not deleted as they are received.
    /// See ``keys(filters:)``.
    public func listKeys(filters: [String] = []) async throws -> AsyncThrowingStream<String, Error>
    {
        let watcher = try await watch(
            filters, options: KeyValueWatchOptions(ignoreDeletes: true, metaOnly: true))
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for try await entry in watcher {
                        guard let entry else {
                            break
                        }
                        continuation.yield(entry.key)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
                await watcher.stop()
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    /// Retrieves all stored values of a key, including delete markers, oldest first.
    ///
    /// - Returns the entries, empty if the key has no values.
    public func history(_ key: String) async throws -> [KeyValueEntry] {
        let watcher = try await watch(key, options: KeyValueWatchOptions(includeHistory: true))
        var entries: [KeyValueEntry] = []
        for try await entry in watcher {
            guard let entry else {
                break
            }
            entries.append(entry)
        }
        await watcher.stop()
        return entries
    }

    /// Removes the history of deleted keys.
    ///
    /// - Parameter olderThan: delete markers younger than this are kept (with the rest of the history removed),
    /// so that watchers can still observe the deletion. Zero or negative removes all markers.
    public func purgeDeletes(olderThan: TimeInterval = 30 * 60) async throws {
        let watcher = try await watchAll()
        var markers: [KeyValueEntry] = []
        for try await entry in watcher {
            guard let entry else {
                break
            }
            if entry.operation != .put {
                markers.append(entry)
            }
        }
        await watcher.stop()

        let limit = Date().addingTimeInterval(-olderThan)
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        for marker in markers {
            let created = formatter.date(from: marker.created) ?? .distantPast
            if olderThan > 0 && created > limit {
                _ = try await stream.purge(keep: 1, subject: prefix + marker.key)
            } else {
                _ = try await stream.purge(subject: prefix + marker.key)
            }
        }
    }
}

/// Options of ``KeyValue/watch(_:options:)-([String],_)``.
public struct KeyValueWatchOptions: Sendable {
    /// Delivers all stored values instead of only the latest one per key.
    public var includeHistory: Bool
    /// Skips delete and purge markers.
    public var ignoreDeletes: Bool
    /// Delivers only metadata, without values.
    public var metaOnly: Bool
    /// Delivers only updates made after the watcher was created (no initial values and `nil` marker).
    public var updatesOnly: Bool
    /// Delivers values starting at the given revision.
    public var resumeFromRevision: UInt64?

    /// Creates a value with the given properties, see the properties of the same name.
    public init(
        includeHistory: Bool = false, ignoreDeletes: Bool = false, metaOnly: Bool = false,
        updatesOnly: Bool = false, resumeFromRevision: UInt64? = nil
    ) {
        self.includeHistory = includeHistory
        self.ignoreDeletes = ignoreDeletes
        self.metaOnly = metaOnly
        self.updatesOnly = updatesOnly
        self.resumeFromRevision = resumeFromRevision
    }
}

/// Delivers changes of watched keys, see ``KeyValue/watch(_:options:)-([String],_)``.
/// A `nil` element marks that all initial values were delivered.
public final class KeyValueWatcher: AsyncSequence, Sendable {
    public typealias Element = KeyValueEntry?

    private let messages: OrderedConsumeResult
    private let stream: AsyncThrowingStream<KeyValueEntry?, Error>

    init(
        kv: KeyValue, messages: OrderedConsumeResult, initialPending: UInt64?,
        ignoreDeletes: Bool
    ) {
        self.messages = messages
        let bucket = kv.bucket
        let prefix = kv.prefix
        self.stream = AsyncThrowingStream { continuation in
            let task = Task {
                var initDone = initialPending == nil || initialPending == 0
                if initialPending == 0 {
                    continuation.yield(nil)
                }
                var received: UInt64 = 0
                do {
                    for try await message in messages {
                        let meta = try message.metadata()
                        guard message.subject.count > prefix.count else {
                            continue
                        }
                        let entry = KeyValueEntry(
                            bucket: bucket, key: String(message.subject.dropFirst(prefix.count)),
                            value: message.payload ?? Data(), revision: meta.streamSequence,
                            created: Self.format(nanos: meta.timestamp), delta: meta.pending,
                            headers: message.headers)
                        if !ignoreDeletes || entry.operation == .put {
                            continuation.yield(entry)
                        }
                        if !initDone {
                            received += 1
                            if received >= initialPending ?? 0 || meta.pending == 0 {
                                initDone = true
                                continuation.yield(nil)
                            }
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

    public func makeAsyncIterator() -> AsyncThrowingStream<KeyValueEntry?, Error>.Iterator {
        stream.makeAsyncIterator()
    }

    private static func format(nanos: String) -> String {
        guard let nanos = Double(nanos) else {
            return nanos
        }
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: Date(timeIntervalSince1970: nanos / 1_000_000_000))
    }
}
