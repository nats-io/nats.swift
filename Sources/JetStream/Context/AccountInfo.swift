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

/// contains info about the `JetStream` usage from the current account.
public struct AccountInfo: Codable, Sendable {
    /// Memory storage used by the account, in bytes.
    public let memory: Int64
    /// File storage used by the account, in bytes.
    public let storage: Int64
    /// Number of streams in the account.
    public let streams: Int64
    /// Number of consumers in the account.
    public let consumers: Int64
    /// Memory storage reserved for streams, in bytes.
    public let reservedMemory: UInt64?
    /// File storage reserved for streams, in bytes.
    public let reservedStorage: UInt64?
    /// JetStream limits of the account.
    public let limits: AccountLimits?
    /// The JetStream domain of the account, if any.
    public let domain: String?
    /// Statistics of the JetStream API usage.
    public let api: APIStats?
    /// Usage per tier, set when the account uses tiered limits.
    public let tiers: [String: AccountTier]?

    enum CodingKeys: String, CodingKey {
        case memory, storage, streams, consumers, limits, domain, api, tiers
        case reservedMemory = "reserved_memory"
        case reservedStorage = "reserved_storage"
    }
}

/// Usage and limits of a single JetStream account tier.
public struct AccountTier: Codable, Sendable {
    /// Memory storage used by the tier, in bytes.
    public let memory: Int64
    /// File storage used by the tier, in bytes.
    public let storage: Int64
    /// Number of streams in the tier.
    public let streams: Int64
    /// Number of consumers in the tier.
    public let consumers: Int64
    /// Memory storage reserved for streams, in bytes.
    public let reservedMemory: UInt64?
    /// File storage reserved for streams, in bytes.
    public let reservedStorage: UInt64?
    /// JetStream limits of the tier.
    public let limits: AccountLimits?

    enum CodingKeys: String, CodingKey {
        case memory, storage, streams, consumers, limits
        case reservedMemory = "reserved_memory"
        case reservedStorage = "reserved_storage"
    }
}

/// JetStream limits of an account, `-1` means unlimited.
public struct AccountLimits: Codable, Sendable {
    /// Maximum memory storage, in bytes.
    public let maxMemory: Int64
    /// Maximum file storage, in bytes.
    public let maxStorage: Int64
    /// Maximum number of streams.
    public let maxStreams: Int
    /// Maximum number of consumers.
    public let maxConsumers: Int
    /// Maximum number of outstanding acks a consumer may set.
    public let maxAckPending: Int?
    /// Maximum size of a memory stream, in bytes.
    public let memoryMaxStreamBytes: Int64?
    /// Maximum size of a file stream, in bytes.
    public let storageMaxStreamBytes: Int64?
    /// Whether streams are required to set `maxBytes`.
    public let maxBytesRequired: Bool?

    enum CodingKeys: String, CodingKey {
        case maxMemory = "max_memory"
        case maxStorage = "max_storage"
        case maxStreams = "max_streams"
        case maxConsumers = "max_consumers"
        case maxAckPending = "max_ack_pending"
        case memoryMaxStreamBytes = "memory_max_stream_bytes"
        case storageMaxStreamBytes = "storage_max_stream_bytes"
        case maxBytesRequired = "max_bytes_required"
    }
}

/// Statistics of the JetStream API usage of an account.
public struct APIStats: Codable, Sendable {
    /// API level supported by the server.
    public let level: Int?
    /// Total number of API requests.
    public let total: UInt64
    /// Number of API requests which resulted in an error.
    public let errors: UInt64
    /// Number of API requests currently in flight.
    public let inflight: UInt64?
}
