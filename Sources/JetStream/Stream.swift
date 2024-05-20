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

/// Exposes a set of operations performed on a stream:
/// - fetching stream info
/// - fetching individual messages from the stream
/// - deleting messages from a stream
/// - purging a stream
/// - operating on Consumers
public class Stream {

    /// Contains information about the stream.
    /// Note that this may be out of date and reading it does not query the server.
    /// For up-to-date stream info use ``Stream/info()``
    public internal(set) var info: StreamInfo
    internal let ctx: JetStreamContext

    init(ctx: JetStreamContext, info: StreamInfo) {
        self.ctx = ctx
        self.info = info
    }

    /// Retrieves information about the stream
    /// This also refreshes ``Stream/info``
    ///
    /// - Returns ``StreamInfo`` from the server
    ///
    /// - Throws:
    ///   - ``JetStreamRequestError`` if the request was unsuccesful
    ///   - ``JetStreamError`` if the server responded with an API error
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

    /// Retrieves a raw message from stream.
    ///
    /// - Parameters:
    ///   - sequence: The sequence of the message in the stream.
    ///   - subject: The stream subject the message should be retrieved from.
    ///     When combined with `seq` will return the first msg with seq >= of the specified sequence.
    ///
    /// - Returns ``RawMessage`` containing message payload, headers and metadata.
    ///
    /// - Throws:
    ///   - ``JetStreamRequestError`` if the request was unsuccesful.
    ///   - ``JetStreamError`` if the server responded with an API error.
    public func getMsg(sequence: UInt64, subject: String? = nil) async throws -> RawMessage {
        let request = GetMsgRequest(seq: sequence, next: subject)
        return try await getRawMsg(request: request)
    }

    /// Retrieves the first message on the stream for a given subject.
    ///
    /// - Parameter firstForSubject: The subject from which the first message should be retrieved.
    ///
    /// - Returns ``RawMessage`` containing message payload, headers and metadata.
    ///
    /// - Throws:
    ///   - ``JetStreamRequestError`` if the request was unsuccesful.
    ///   - ``JetStreamError`` if the server responded with an API error.
    public func getMsg(firstForSubject: String) async throws -> RawMessage {
        let request = GetMsgRequest(next: firstForSubject)
        return try await getRawMsg(request: request)
    }

    /// Retrieves last message on a stream for a given subject
    ///
    /// - Parameter lastForSubject: The stream subject for which the last available message should be retrieved.
    ///
    /// - Returns ``RawMessage`` containing message payload, headers and metadata.
    ///
    /// - Throws:
    ///   - ``JetStreamRequestError`` if the request was unsuccesful.
    ///   - ``JetStreamError`` if the server responded with an API error.
    public func getMsg(lastForSubject: String) async throws -> RawMessage {
        let request = GetMsgRequest(last: lastForSubject)
        return try await getRawMsg(request: request)
    }

    /// Retrieves a raw message from stream.
    ///
    /// Requires a ``Stream`` with ``StreamConfig/allowDirect`` set to `true`.
    /// This is different from ``Stream/getMsg(sequence:subject:)``, as it can fetch ``RawMessage``
    /// from any replica member. This means read after write is possible,
    /// as that given replica might not yet catch up with the leader.
    ///
    /// - Parameters:
    ///   - sequence: The sequence of the message in the stream.
    ///   - subject: The stream subject the message should be retrieved from.
    ///     When combined with `seq` will return the first msg with seq >= of the specified sequence.
    ///
    /// - Returns ``RawMessage`` containing message payload, headers and metadata.
    ///
    /// - Throws:
    ///   - ``JetStreamRequestError`` if the request was unsuccesful.
    ///   - ``JetStreamDirectGetError`` if the server responded with an error or the response is invalid
    public func getMsgDirect(sequence: UInt64, subject: String? = nil) async throws -> RawMessage {
        let request = GetMsgRequest(seq: sequence, next: subject)
        return try await getRawMsgDirect(request: request)
    }

    /// Retrieves the first message on the stream for a given subject.
    ///
    /// Requires a ``Stream`` with ``StreamConfig/allowDirect`` set to `true`.
    /// This is different from ``Stream/getMsg(firstForSubject:)``, as it can fetch ``RawMessage``
    /// from any replica member. This means read after write is possible,
    /// as that given replica might not yet catch up with the leader.
    ///
    /// - Parameter firstForSubject: The subject from which the first message should be retrieved.
    ///
    /// - Returns ``RawMessage`` containing message payload, headers and metadata.
    ///
    /// - Throws:
    ///   - ``JetStreamRequestError`` if the request was unsuccesful.
    ///   - ``JetStreamDirectGetError`` if the server responded with an error or the response is invalid
    public func getMsgDirect(firstForSubject: String) async throws -> RawMessage {
        let request = GetMsgRequest(next: firstForSubject)
        return try await getRawMsgDirect(request: request)
    }

    /// Retrieves last message on a stream for a given subject
    ///
    /// Requires a ``Stream`` with ``StreamConfig/allowDirect`` set to `true`.
    /// This is different from ``Stream/getMsg(lastForSubject:)``, as it can fetch ``RawMessage``
    /// from any replica member. This means read after write is possible,
    /// as that given replica might not yet catch up with the leader.
    ///
    /// - Parameter lastForSubject: The stream subject for which the last available message should be retrieved.
    ///
    /// - Returns ``RawMessage`` containing message payload, headers and metadata.
    ///
    /// - Throws:
    ///   - ``JetStreamRequestError`` if the request was unsuccesful.
    ///   - ``JetStreamDirectGetError`` if the server responded with an error or the response is invalid
    public func getMsgDirect(lastForSubject: String) async throws -> RawMessage {
        let request = GetMsgRequest(last: lastForSubject)
        return try await getRawMsgDirect(request: request)
    }

    private func getRawMsg(request: GetMsgRequest) async throws -> RawMessage {
        let subject = "STREAM.MSG.GET.\(info.config.name)"
        let requestData = try JSONEncoder().encode(request)

        let resp: Response<GetRawMessageResp> = try await ctx.request(subject, message: requestData)

        switch resp {
        case .success(let msg):
            return try RawMessage(from: msg.message)
        case .error(let err):
            throw err.error
        }
    }

    private func getRawMsgDirect(request: GetMsgRequest) async throws -> RawMessage {
        let subject = "DIRECT.GET.\(info.config.name)"
        let requestData = try JSONEncoder().encode(request)

        let resp = try await ctx.request(subject, message: requestData)

        if let status = resp.status {
            if status == StatusCode.notFound {
                throw JetStreamDirectGetError.msgNotFound
            }
            throw JetStreamDirectGetError.errorResponse(status, resp.description)
        }

        guard let headers = resp.headers else {
            throw JetStreamDirectGetError.invalidResponse("response should contain headers")
        }

        guard headers[.natsStream] != nil else {
            throw JetStreamDirectGetError.invalidResponse("missing Nats-Stream header")
        }

        guard let seqHdr = headers[.natsSequence] else {
            throw JetStreamDirectGetError.invalidResponse("missing Nats-Sequence header")
        }

        let seq = UInt64(seqHdr.description)
        if seq == nil {
            throw JetStreamDirectGetError.invalidResponse("invalid Nats-Sequence header: \(seqHdr)")
        }

        guard let timeStamp = headers[.natsTimestamp] else {
            throw JetStreamDirectGetError.invalidResponse("missing Nats-Timestamp header")
        }

        guard let subject = headers[.natsSubject] else {
            throw JetStreamDirectGetError.invalidResponse("missing Nats-Subject header")
        }

        let payload = resp.payload ?? Data()

        return RawMessage(
            subject: subject.description, sequence: seq!, payload: payload, headers: resp.headers,
            time: timeStamp.description)
    }

    internal struct GetMsgRequest: Codable {
        internal let seq: UInt64?
        internal let nextBySubject: String?
        internal let lastBySubject: String?

        internal init(seq: UInt64, next: String?) {
            self.seq = seq
            self.nextBySubject = next
            self.lastBySubject = nil
        }

        internal init(next: String) {
            self.seq = nil
            self.nextBySubject = next
            self.lastBySubject = nil
        }

        internal init(last: String) {
            self.seq = nil
            self.nextBySubject = nil
            self.lastBySubject = last
        }

        enum CodingKeys: String, CodingKey {
            case seq
            case nextBySubject = "next_by_subj"
            case lastBySubject = "last_by_subj"
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

public enum JetStreamDirectGetError: NatsError, Equatable {
    case msgNotFound
    case invalidResponse(String)
    case errorResponse(StatusCode, String?)

    public var description: String {
        switch self {
        case .msgNotFound:
            return "message not found"
        case .invalidResponse(let cause):
            return "invalid response: \(cause)"
        case .errorResponse(let code, let description):
            if let description {
                return "unable to get message: \(code) \(description)"
            } else {
                return "unable to get message: \(code)"
            }
        }
    }
}

/// Returned when a provided ``StreamConfig`` is not valid.
public enum StreamValidationError: NatsError, Equatable {
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
    public let config: StreamConfig

    /// The timestamp indicating when the stream was created.
    public let created: String

    /// Provides the current state of the stream including metrics such as message count and total bytes.
    public let state: StreamState

    /// Information about the cluster to which this stream belongs, if applicable.
    public let cluster: ClusterInfo?

    /// Information about another stream that this one is mirroring, if applicable.
    public let mirror: StreamSourceInfo?

    /// A list of source streams from which this stream collects data.
    public let sources: [StreamSourceInfo]?

    /// The timestamp indicating when this information was gathered by the server.
    public let timeStamp: String

    enum CodingKeys: String, CodingKey {
        case config, created, state, cluster, mirror, sources
        case timeStamp = "ts"
    }
}

/// `StreamConfig` defines the configuration for a JetStream stream.
public struct StreamConfig: Codable, Equatable {
    /// The name of the stream, required and must be unique across the JetStream account.
    public let name: String

    /// An optional description of the stream.
    public var description: String?

    /// A list of subjects that the stream is listening on, cannot be set if the stream is a mirror.
    public var subjects: [String]?

    /// The message retention policy for the stream, defaults to `LimitsPolicy`.
    public var retention: RetentionPolicy

    /// The maximum number of consumers allowed for the stream.
    public var maxConsumers: Int

    /// The maximum number of messages the stream will store.
    public var maxMsgs: Int64

    /// The maximum total size of messages the stream will store.
    public var maxBytes: Int64

    /// Defines the policy for handling messages when the stream's limits are reached.
    public var discard: DiscardPolicy

    /// A flag to enable discarding new messages per subject when limits are reached.
    public var discardNewPerSubject: Bool?

    /// The maximum age of messages that the stream will retain.
    public var maxAge: NanoTimeInterval

    /// The maximum number of messages per subject that the stream will retain.
    public var maxMsgsPerSubject: Int64

    /// The maximum size of any single message in the stream.
    public var maxMsgSize: Int32

    /// Specifies the type of storage backend used for the stream (file or memory).
    public var storage: StorageType

    /// The number of stream replicas in clustered JetStream.
    public var replicas: Int

    /// A flag to disable acknowledging messages received by this stream.
    public var noAck: Bool?

    /// The window within which to track duplicate messages.
    public var duplicates: NanoTimeInterval?

    /// Used to declare where the stream should be placed via tags or an explicit cluster name.
    public var placement: Placement?

    /// Configuration for mirroring another stream.
    public var mirror: StreamSource?

    /// A list of other streams this stream sources messages from.
    public var sources: [StreamSource]?

    /// Whether the stream does not allow messages to be published or deleted.
    public var sealed: Bool?

    /// Restricts the ability to delete messages from a stream via the API.
    public var denyDelete: Bool?

    /// Restricts the ability to purge messages from a stream via the API.
    public var denyPurge: Bool?

    /// Allows the use of the Nats-Rollup header to replace all contents of a stream or subject in a stream with a single new message.
    public var allowRollup: Bool?

    /// Specifies the message storage compression algorithm.
    public var compression: StoreCompression

    /// The initial sequence number of the first message in the stream.
    public var firstSeq: UInt64?

    /// Allows applying a transformation to matching messages' subjects.
    public var subjectTransform: SubjectTransformConfig?

    /// Allows immediate republishing a message to the configured subject after it's stored.
    public var rePublish: RePublish?

    /// Enables direct access to individual messages using direct get API.
    public var allowDirect: Bool

    /// Enables direct access to individual messages from the origin stream using direct get API.
    public var mirrorDirect: Bool

    /// Defines limits of certain values that consumers can set.
    public var consumerLimits: StreamConsumerLimits?

    /// A set of application-defined key-value pairs for associating metadata on the stream.
    public var metadata: [String: String]?

    public init(
        name: String,
        description: String? = nil,
        subjects: [String]? = nil,
        retention: RetentionPolicy = .limits,
        maxConsumers: Int = -1,
        maxMsgs: Int64 = -1,
        maxBytes: Int64 = -1,
        discard: DiscardPolicy = .old,
        discardNewPerSubject: Bool? = nil,
        maxAge: NanoTimeInterval = NanoTimeInterval(0),
        maxMsgsPerSubject: Int64 = -1,
        maxMsgSize: Int32 = -1,
        storage: StorageType = .file,
        replicas: Int = 1,
        noAck: Bool? = nil,
        duplicates: NanoTimeInterval? = nil,
        placement: Placement? = nil,
        mirror: StreamSource? = nil,
        sources: [StreamSource]? = nil,
        sealed: Bool? = nil,
        denyDelete: Bool? = nil,
        denyPurge: Bool? = nil,
        allowRollup: Bool? = nil,
        compression: StoreCompression = .none,
        firstSeq: UInt64? = nil,
        subjectTransform: SubjectTransformConfig? = nil,
        rePublish: RePublish? = nil,
        allowDirect: Bool = false,
        mirrorDirect: Bool = false,
        consumerLimits: StreamConsumerLimits? = nil,
        metadata: [String: String]? = nil
    ) {
        self.name = name
        self.description = description
        self.subjects = subjects
        self.retention = retention
        self.maxConsumers = maxConsumers
        self.maxMsgs = maxMsgs
        self.maxBytes = maxBytes
        self.discard = discard
        self.discardNewPerSubject = discardNewPerSubject
        self.maxAge = maxAge
        self.maxMsgsPerSubject = maxMsgsPerSubject
        self.maxMsgSize = maxMsgSize
        self.storage = storage
        self.replicas = replicas
        self.noAck = noAck
        self.duplicates = duplicates
        self.placement = placement
        self.mirror = mirror
        self.sources = sources
        self.sealed = sealed
        self.denyDelete = denyDelete
        self.denyPurge = denyPurge
        self.allowRollup = allowRollup
        self.compression = compression
        self.firstSeq = firstSeq
        self.subjectTransform = subjectTransform
        self.rePublish = rePublish
        self.allowDirect = allowDirect
        self.mirrorDirect = mirrorDirect
        self.consumerLimits = consumerLimits
        self.metadata = metadata
    }

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
public enum RetentionPolicy: String, Codable {
    /// Messages are retained until any given limit is reached (MaxMsgs, MaxBytes or MaxAge).
    case limits

    /// Messages are removed when all known observables have acknowledged a message.
    case interest

    /// Messages are removed when the first subscriber acknowledges the message.
    case workqueue
}

/// `DiscardPolicy` determines how to proceed when limits of messages or bytes are reached.
public enum DiscardPolicy: String, Codable {
    /// Remove older messages to return to the limits.
    case old

    /// Fail to store new messages once the limits are reached.
    case new
}

/// `StorageType` determines how messages are stored for retention.
public enum StorageType: String, Codable {
    /// Messages are stored on disk.
    case file

    /// Messages are stored in memory.
    case memory
}

/// `Placement` guides the placement of streams in clustered JetStream.
public struct Placement: Codable, Equatable {
    /// Tags used to match streams to servers in the cluster.
    public var tags: [String]?

    /// Name of the cluster to which the stream should be assigned.
    public var cluster: String?

    public init(tags: [String]? = nil, cluster: String? = nil) {
        self.tags = tags
        self.cluster = cluster
    }
}

/// `StreamSource` defines how streams can source from other streams.
public struct StreamSource: Codable, Equatable {
    /// Name of the stream to source from.
    public let name: String

    /// Sequence number to start sourcing from.
    public let optStartSeq: UInt64?

    // Timestamp of messages to start sourcing from.
    public let optStartTime: Date?

    /// Subject filter to replicate only matching messages.
    public let filterSubject: String?

    /// Transforms applied to subjects.
    public let subjectTransforms: [SubjectTransformConfig]?

    /// Configuration for external stream sources.
    public let external: ExternalStream?

    public init(
        name: String, optStartSeq: UInt64? = nil, optStartTime: Date? = nil,
        filterSubject: String? = nil, subjectTransforms: [SubjectTransformConfig]? = nil,
        external: ExternalStream? = nil
    ) {
        self.name = name
        self.optStartSeq = optStartSeq
        self.optStartTime = optStartTime
        self.filterSubject = filterSubject
        self.subjectTransforms = subjectTransforms
        self.external = external
    }

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
public struct ExternalStream: Codable, Equatable {

    /// Subject prefix for importing API subjects.
    public let apiPrefix: String

    /// Delivery subject for push consumers.
    public let deliverPrefix: String

    public init(apiPrefix: String, deliverPrefix: String) {
        self.apiPrefix = apiPrefix
        self.deliverPrefix = deliverPrefix
    }

    enum CodingKeys: String, CodingKey {
        case apiPrefix = "api"
        case deliverPrefix = "deliver"
    }
}

/// `StoreCompression` specifies the message storage compression algorithm.
public enum StoreCompression: String, Codable {
    /// No compression is applied.
    case none

    /// Uses the S2 compression algorithm.
    case s2
}

/// `SubjectTransformConfig` configures subject transformations for incoming messages.
public struct SubjectTransformConfig: Codable, Equatable {
    /// Subject pattern to match incoming messages.
    public let source: String

    /// Subject pattern to remap the subject to.
    public let destination: String

    public init(source: String, destination: String) {
        self.source = source
        self.destination = destination
    }

    enum CodingKeys: String, CodingKey {
        case source = "src"
        case destination = "dest"
    }
}

/// `RePublish` configures republishing of messages once they are committed to a stream.
public struct RePublish: Codable, Equatable {
    /// Subject pattern to match incoming messages.
    public let source: String?

    /// Subject pattern to republish the subject to.
    public let destination: String

    /// Flag to indicate if only headers should be republished.
    public let headersOnly: Bool?

    public init(destination: String, source: String? = nil, headersOnly: Bool? = nil) {
        self.destination = destination
        self.source = source
        self.headersOnly = headersOnly
    }

    enum CodingKeys: String, CodingKey {
        case source = "src"
        case destination = "dest"
        case headersOnly = "headers_only"
    }
}

/// `StreamConsumerLimits` defines the limits for a consumer on a stream.
public struct StreamConsumerLimits: Codable, Equatable {
    /// Duration to clean up the consumer if inactive.
    public var inactiveThreshold: NanoTimeInterval?

    /// Maximum number of outstanding unacknowledged messages.
    public var maxAckPending: Int?

    public init(inactiveThreshold: NanoTimeInterval? = nil, maxAckPending: Int? = nil) {
        self.inactiveThreshold = inactiveThreshold
        self.maxAckPending = maxAckPending
    }

    enum CodingKeys: String, CodingKey {
        case inactiveThreshold = "inactive_threshold"
        case maxAckPending = "max_ack_pending"
    }
}

/// `StreamState` represents the state of a JetStream stream at the time of the request.
public struct StreamState: Codable {
    /// Number of messages stored in the stream.
    public let messages: UInt64

    /// Number of bytes stored in the stream.
    public let bytes: UInt64

    /// Sequence number of the first message.
    public let firstSeq: UInt64

    /// Timestamp of the first message.
    public let firstTime: String

    /// Sequence number of the last message.
    public let lastSeq: UInt64

    /// Timestamp of the last message.
    public let lastTime: String

    /// Number of consumers on the stream.
    public let consumers: Int

    /// Sequence numbers of deleted messages.
    public let deleted: [UInt64]?

    /// Number of messages deleted causing gaps in sequence numbers.
    public let numDeleted: Int?

    /// Number of unique subjects received messages.
    public let numSubjects: UInt64?

    /// Message count per subject.
    public let subjects: [String: UInt64]?

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
    public let name: String?

    /// The server name of the RAFT leader within the cluster.
    public let leader: String?

    /// A list of peers that are part of the cluster.
    public let replicas: [PeerInfo]?
}

/// `StreamSourceInfo` provides information about an upstream stream source or mirror.
public struct StreamSourceInfo: Codable {
    /// The name of the stream that is being replicated or mirrored.
    public let name: String

    /// The lag in messages between this stream and the stream it mirrors or sources from.
    public let lag: UInt64

    /// The time since the last activity was detected for this stream.
    public let active: NanoTimeInterval

    /// The subject filter used to replicate messages with matching subjects.
    public let filterSubject: String?

    /// A list of subject transformations applied to messages as they are sourced.
    public let subjectTransforms: [SubjectTransformConfig]?

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
    public let name: String

    /// Indicates if the peer is currently synchronized and up-to-date with the leader.
    public let current: Bool

    /// Indicates if the peer is considered offline by the cluster.
    public let offline: Bool?

    /// The time duration since this peer was last active.
    public let active: NanoTimeInterval

    /// The number of uncommitted operations this peer is lagging behind the leader.
    public let lag: UInt64?

    enum CodingKeys: String, CodingKey {
        case name
        case current
        case offline
        case active
        case lag
    }
}

internal struct GetRawMessageResp: Codable {
    internal struct StoredRawMessage: Codable {
        public let subject: String
        public let sequence: UInt64
        public let payload: Data
        public let headers: Data?
        public let time: String

        enum CodingKeys: String, CodingKey {
            case subject
            case sequence = "seq"
            case payload = "data"
            case headers = "hdrs"
            case time
        }
    }

    internal let message: StoredRawMessage
}

public struct RawMessage {

    /// Subject of the message.
    public let subject: String

    /// Sequence of the message.
    public let sequence: UInt64

    /// Raw payload of the message as a base64 encoded string.
    public let payload: Data

    /// Raw header string, if any.
    public let headers: NatsHeaderMap?

    /// The time the message was published.
    public let time: String

    internal init(
        subject: String, sequence: UInt64, payload: Data, headers: NatsHeaderMap?, time: String
    ) {
        self.subject = subject
        self.sequence = sequence
        self.payload = payload
        self.headers = headers
        self.time = time
    }

    internal init(from storedMsg: GetRawMessageResp.StoredRawMessage) throws {
        self.subject = storedMsg.subject
        self.sequence = storedMsg.sequence
        self.payload = storedMsg.payload
        if let headers = storedMsg.headers, let headersStr = String(data: headers, encoding: .utf8)
        {
            self.headers = try NatsHeaderMap(from: headersStr)
        } else {
            self.headers = nil
        }
        self.time = storedMsg.time
    }
}
