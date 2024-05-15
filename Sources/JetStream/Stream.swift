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
import Nats

public class Stream {
    public internal(set) var info: StreamInfo
    internal let ctx: JetStreamContext

    init(ctx: JetStreamContext, info: StreamInfo) {
        self.ctx = ctx
        self.info = info
    }

    public func info() async throws -> StreamInfo {
        let subj = "STREAM.INFO.\(info.config.name)"
        let info: Response<StreamInfo> = try await ctx.request(subj)
        switch info {
        case .success(let info):
            self.info = info
            return info
        case .error(let apiResponse):
            throw apiResponse.error
        }
    }

    static func validate(name: String) throws {
        guard !name.isEmpty else {
            throw StreamValidationError.nameRequired
        }

        let invalidChars = CharacterSet(charactersIn: ">*. /\\")
        if name.rangeOfCharacter(from: invalidChars) != nil {
            throw StreamValidationError.invalidCharacterFound(name)
        }
    }
}

public enum StreamValidationError: NatsError {
    case nameRequired
    case invalidCharacterFound(String)

    public var description: String {
        switch self {
        case .nameRequired:
            return "Stream name is required."
        case .invalidCharacterFound(let name):
            return "Invalid character found in stream name: '\(name)'."
        }
    }
}

/// `StreamInfo` contains details about the configuration and state of a stream within JetStream.
public struct StreamInfo: Codable {
    /// The configuration settings of the stream, set upon creation or update.
    let config: StreamConfig

    /// The timestamp indicating when the stream was created.
    let created: String

    /// Provides the current state of the stream including metrics such as message count and total bytes.
    let state: StreamState

    /// Information about the cluster to which this stream belongs, if applicable.
    let cluster: ClusterInfo?

    /// Information about another stream that this one is mirroring, if applicable.
    let mirror: StreamSourceInfo?

    /// A list of source streams from which this stream collects data.
    let sources: [StreamSourceInfo]?

    /// The timestamp indicating when this information was gathered by the server.
    let timeStamp: String

    enum CodingKeys: String, CodingKey {
        case config, created, state, cluster, mirror, sources
        case timeStamp = "ts"
    }
}

/// `StreamConfig` defines the configuration for a JetStream stream.
public struct StreamConfig: Codable, Equatable {
    /// The name of the stream, required and must be unique across the JetStream account.
    let name: String

    /// An optional description of the stream.
    var description: String? = nil

    /// A list of subjects that the stream is listening on, cannot be set if the stream is a mirror.
    var subjects: [String]? = nil

    /// The message retention policy for the stream, defaults to `LimitsPolicy`.
    var retention: RetentionPolicy = .limits

    /// The maximum number of consumers allowed for the stream.
    var maxConsumers: Int = -1

    /// The maximum number of messages the stream will store.
    var maxMsgs: Int64 = -1

    /// The maximum total size of messages the stream will store.
    var maxBytes: Int64 = -1

    /// Defines the policy for handling messages when the stream's limits are reached.
    var discard: DiscardPolicy = .old

    /// A flag to enable discarding new messages per subject when limits are reached.
    var discardNewPerSubject: Bool? = nil

    /// The maximum age of messages that the stream will retain.
    var maxAge: NanoTimeInterval = NanoTimeInterval(0)

    /// The maximum number of messages per subject that the stream will retain.
    var maxMsgsPerSubject: Int64 = -1

    /// The maximum size of any single message in the stream.
    var maxMsgSize: Int32 = -1

    /// Specifies the type of storage backend used for the stream (file or memory).
    var storage: StorageType = .file

    /// The number of stream replicas in clustered JetStream.
    var replicas: Int = 1

    /// A flag to disable acknowledging messages received by this stream.
    var noAck: Bool? = nil

    /// The window within which to track duplicate messages.
    var duplicates: NanoTimeInterval? = nil

    /// Used to declare where the stream should be placed via tags or an explicit cluster name.
    var placement: Placement? = nil

    /// Configuration for mirroring another stream.
    var mirror: StreamSource? = nil

    /// A list of other streams this stream sources messages from.
    var sources: [StreamSource]? = nil

    /// Whether the stream does not allow messages to be published or deleted.
    var sealed: Bool? = nil

    /// Restricts the ability to delete messages from a stream via the API.
    var denyDelete: Bool? = nil

    /// Restricts the ability to purge messages from a stream via the API.
    var denyPurge: Bool? = nil

    /// Allows the use of the Nats-Rollup header to replace all contents of a stream or subject in a stream with a single new message.
    var allowRollup: Bool? = nil

    /// Specifies the message storage compression algorithm.
    var compression: StoreCompression = .none

    /// The initial sequence number of the first message in the stream.
    var firstSeq: UInt64? = nil

    /// Allows applying a transformation to matching messages' subjects.
    var subjectTransform: SubjectTransformConfig? = nil

    /// Allows immediate republishing a message to the configured subject after it's stored.
    var rePublish: RePublish? = nil

    /// Enables direct access to individual messages using direct get API.
    var allowDirect: Bool = false

    /// Enables direct access to individual messages from the origin stream using direct get API.
    var mirrorDirect: Bool = false

    /// Defines limits of certain values that consumers can set.
    var consumerLimits: StreamConsumerLimits? = nil

    /// A set of application-defined key-value pairs for associating metadata on the stream.
    var metadata: [String: String]? = nil

    enum CodingKeys: String, CodingKey {
        case name
        case description
        case subjects
        case retention
        case maxConsumers = "max_consumers"
        case maxMsgs = "max_msgs"
        case maxBytes = "max_bytes"
        case discard
        case discardNewPerSubject = "discard_new_per_subject"
        case maxAge = "max_age"
        case maxMsgsPerSubject = "max_msgs_per_subject"
        case maxMsgSize = "max_msg_size"
        case storage
        case replicas = "num_replicas"
        case noAck = "no_ack"
        case duplicates = "duplicate_window"
        case placement
        case mirror
        case sources
        case sealed
        case denyDelete = "deny_delete"
        case denyPurge = "deny_purge"
        case allowRollup = "allow_rollup_hdrs"
        case compression
        case firstSeq = "first_seq"
        case subjectTransform = "subject_transform"
        case rePublish = "republish"
        case allowDirect = "allow_direct"
        case mirrorDirect = "mirror_direct"
        case consumerLimits = "consumer_limits"
        case metadata
    }

    // use custom encoder to omit certain fields if they are assigned default values
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)

        try container.encode(name, forKey: .name)  // Always encode the name

        try description.map { try container.encode($0, forKey: .description) }
        if let subjects = subjects, !subjects.isEmpty {
            try container.encode(subjects, forKey: .subjects)
        }
        try retention != .limits ? container.encode(retention, forKey: .retention) : nil
        try maxConsumers != -1 ? container.encode(maxConsumers, forKey: .maxConsumers) : nil
        try maxMsgs != -1 ? container.encode(maxMsgs, forKey: .maxMsgs) : nil
        try maxBytes != -1 ? container.encode(maxBytes, forKey: .maxBytes) : nil
        try discard != .old ? container.encode(discard, forKey: .discard) : nil
        try discardNewPerSubject.map { try container.encode($0, forKey: .discardNewPerSubject) }
        try maxAge.value != 0 ? container.encode(maxAge, forKey: .maxAge) : nil
        try maxMsgsPerSubject != -1
            ? container.encode(maxMsgsPerSubject, forKey: .maxMsgsPerSubject) : nil
        try maxMsgSize != -1 ? container.encode(maxMsgSize, forKey: .maxMsgSize) : nil
        try storage != .file ? container.encode(storage, forKey: .storage) : nil
        try replicas != 1 ? container.encode(replicas, forKey: .replicas) : nil
        try noAck.map { try container.encode($0, forKey: .noAck) }
        try duplicates.map { try container.encode($0, forKey: .duplicates) }
        try placement.map { try container.encode($0, forKey: .placement) }
        try mirror.map { try container.encode($0, forKey: .mirror) }
        if let sources = sources, !sources.isEmpty {
            try container.encode(sources, forKey: .sources)
        }
        try sealed.map { try container.encode($0, forKey: .sealed) }
        try denyDelete.map { try container.encode($0, forKey: .denyDelete) }
        try denyPurge.map { try container.encode($0, forKey: .denyPurge) }
        try allowRollup.map { try container.encode($0, forKey: .allowRollup) }
        try compression != .none ? container.encode(compression, forKey: .compression) : nil
        try firstSeq.map { try container.encode($0, forKey: .firstSeq) }
        try subjectTransform.map { try container.encode($0, forKey: .subjectTransform) }
        try rePublish.map { try container.encode($0, forKey: .rePublish) }
        try allowDirect ? container.encode(allowDirect, forKey: .allowDirect) : nil
        try mirrorDirect ? container.encode(mirrorDirect, forKey: .mirrorDirect) : nil
        try consumerLimits.map { try container.encode($0, forKey: .consumerLimits) }
        if let metadata = metadata, !metadata.isEmpty {
            try container.encode(metadata, forKey: .metadata)
        }
    }
}

/// `RetentionPolicy` determines how messages in a stream are retained.
enum RetentionPolicy: String, Codable {
    /// Messages are retained until any given limit is reached (MaxMsgs, MaxBytes or MaxAge).
    case limits

    /// Messages are removed when all known observables have acknowledged a message.
    case interest

    /// Messages are removed when the first subscriber acknowledges the message.
    case workqueue
}

/// `DiscardPolicy` determines how to proceed when limits of messages or bytes are reached.
enum DiscardPolicy: String, Codable {
    /// Remove older messages to return to the limits.
    case old

    /// Fail to store new messages once the limits are reached.
    case new
}

/// `StorageType` determines how messages are stored for retention.
enum StorageType: String, Codable {
    /// Messages are stored on disk.
    case file

    /// Messages are stored in memory.
    case memory
}

/// `Placement` guides the placement of streams in clustered JetStream.
struct Placement: Codable, Equatable {
    /// Tags used to match streams to servers in the cluster.
    var tags: [String]? = nil

    /// Name of the cluster to which the stream should be assigned.
    var cluster: String? = nil
}

/// `StreamSource` defines how streams can source from other streams.
struct StreamSource: Codable, Equatable {
    /// Name of the stream to source from.
    let name: String

    /// Sequence number to start sourcing from.
    let optStartSeq: UInt64? = nil

    // Timestamp of messages to start sourcing from.
    let optStartTime: Date? = nil

    /// Subject filter to replicate only matching messages.
    let filterSubject: String? = nil

    /// Transforms applied to subjects.
    let subjectTransforms: [SubjectTransformConfig]? = nil

    /// Configuration for external stream sources.
    let external: ExternalStream? = nil

    enum CodingKeys: String, CodingKey {
        case name
        case optStartSeq = "opt_start_seq"
        case optStartTime = "opt_start_time"
        case filterSubject = "filter_subject"
        case subjectTransforms = "subject_transforms"
        case external
    }
}

/// `ExternalStream` qualifies access to a stream source in another account or JetStream domain.
struct ExternalStream: Codable, Equatable {

    /// Subject prefix for importing API subjects.
    let apiPrefix: String

    /// Delivery subject for push consumers.
    let deliverPrefix: String

    enum CodingKeys: String, CodingKey {
        case apiPrefix = "api"
        case deliverPrefix = "deliver"
    }
}

/// `StoreCompression` specifies the message storage compression algorithm.
enum StoreCompression: String, Codable {
    /// No compression is applied.
    case none

    /// Uses the S2 compression algorithm.
    case s2
}

/// `SubjectTransformConfig` configures subject transformations for incoming messages.
struct SubjectTransformConfig: Codable, Equatable {
    /// Subject pattern to match incoming messages.
    let source: String

    /// Subject pattern to remap the subject to.
    let destination: String

    enum CodingKeys: String, CodingKey {
        case source = "src"
        case destination = "dest"
    }
}

/// `RePublish` configures republishing of messages once they are committed to a stream.
struct RePublish: Codable, Equatable {
    /// Subject pattern to match incoming messages.
    let source: String? = nil

    /// Subject pattern to republish the subject to.
    let destination: String

    /// Flag to indicate if only headers should be republished.
    let headersOnly: Bool? = nil

    enum CodingKeys: String, CodingKey {
        case source = "src"
        case destination = "dest"
        case headersOnly = "headers_only"
    }
}

/// `StreamConsumerLimits` defines the limits for a consumer on a stream.
struct StreamConsumerLimits: Codable, Equatable {
    /// Duration to clean up the consumer if inactive.
    var inactiveThreshold: NanoTimeInterval? = nil

    /// Maximum number of outstanding unacknowledged messages.
    var maxAckPending: Int? = nil

    enum CodingKeys: String, CodingKey {
        case inactiveThreshold = "inactive_threshold"
        case maxAckPending = "max_ack_pending"
    }
}

/// `StreamState` represents the state of a JetStream stream at the time of the request.
public struct StreamState: Codable {
    /// Number of messages stored in the stream.
    let messages: UInt64

    /// Number of bytes stored in the stream.
    let bytes: UInt64

    /// Sequence number of the first message.
    let firstSeq: UInt64

    /// Timestamp of the first message.
    let firstTime: String

    /// Sequence number of the last message.
    let lastSeq: UInt64

    /// Timestamp of the last message.
    let lastTime: String

    /// Number of consumers on the stream.
    let consumers: Int

    /// Sequence numbers of deleted messages.
    let deleted: [UInt64]?

    /// Number of messages deleted causing gaps in sequence numbers.
    let numDeleted: Int?

    /// Number of unique subjects received messages.
    let numSubjects: UInt64?

    /// Message count per subject.
    let subjects: [String: UInt64]?

    enum CodingKeys: String, CodingKey {
        case messages
        case bytes
        case firstSeq = "first_seq"
        case firstTime = "first_ts"
        case lastSeq = "last_seq"
        case lastTime = "last_ts"
        case consumers = "consumer_count"
        case deleted
        case numDeleted = "num_deleted"
        case numSubjects = "num_subjects"
        case subjects
    }
}

/// `ClusterInfo` contains details about the cluster to which a stream belongs.
public struct ClusterInfo: Codable {
    /// The name of the cluster.
    let name: String?

    /// The server name of the RAFT leader within the cluster.
    let leader: String?

    /// A list of peers that are part of the cluster.
    let replicas: [PeerInfo]?
}

/// `StreamSourceInfo` provides information about an upstream stream source or mirror.
public struct StreamSourceInfo: Codable {
    /// The name of the stream that is being replicated or mirrored.
    let name: String

    /// The lag in messages between this stream and the stream it mirrors or sources from.
    let lag: UInt64

    /// The time since the last activity was detected for this stream.
    let active: NanoTimeInterval

    /// The subject filter used to replicate messages with matching subjects.
    let filterSubject: String?

    /// A list of subject transformations applied to messages as they are sourced.
    let subjectTransforms: [SubjectTransformConfig]?

    enum CodingKeys: String, CodingKey {
        case name
        case lag
        case active
        case filterSubject = "filter_subject"
        case subjectTransforms = "subject_transforms"
    }
}

/// `PeerInfo` provides details about the peers in a cluster that support the stream or consumer.
public struct PeerInfo: Codable {
    /// The server name of the peer within the cluster.
    let name: String

    /// Indicates if the peer is currently synchronized and up-to-date with the leader.
    let current: Bool

    /// Indicates if the peer is considered offline by the cluster.
    let offline: Bool?

    /// The time duration since this peer was last active.
    let active: NanoTimeInterval

    /// The number of uncommitted operations this peer is lagging behind the leader.
    let lag: UInt64?

    enum CodingKeys: String, CodingKey {
        case name
        case current
        case offline
        case active
        case lag
    }
}
