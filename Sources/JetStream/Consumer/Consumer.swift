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
import Nuid

/// A JetStream consumer, used to pull messages from a stream.
///
/// Create consumers with ``Stream/createConsumer(cfg:)`` and retrieve messages with
/// ``fetch(batch:maxBytes:expires:idleHeartbeat:priorityGroup:)``, ``next(expires:idleHeartbeat:priorityGroup:)``
/// or ``consume(options:errorHandler:)``.
public final class Consumer: Sendable {

    private static let rdigits: [UInt8] = Array(
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".utf8)

    /// Contains information about the consumer.
    /// Note that this may be out of date and reading it does not query the server.
    /// For up-to-date stream info use ``Consumer/info()``
    public internal(set) var info: ConsumerInfo {
        get { cachedInfo.value }
        set { cachedInfo.value = newValue }
    }
    private let cachedInfo: Locked<ConsumerInfo>
    internal let ctx: JetStreamContext
    internal let pinId = PinId()

    init(ctx: JetStreamContext, info: ConsumerInfo) {
        self.ctx = ctx
        self.cachedInfo = Locked(info)
    }

    /// Retrieves information about the consumer
    /// This also refreshes ``Consumer/info``.
    ///
    /// - Returns ``ConsumerInfo`` from the server.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/RequestError`` if the request was unsuccessful.
    /// > - ``JetStreamError`` if the server responded with an API error.
    public func info() async throws -> ConsumerInfo {
        let subj = "CONSUMER.INFO.\(info.stream).\(info.name)"
        let info: Response<ConsumerInfo> = try await ctx.request(subj)
        switch info {
        case .success(let info):
            self.info = info
            return info
        case .error(let apiResponse):
            throw apiResponse.error
        }
    }

    internal static func validate(name: String) throws {
        guard !name.isEmpty else {
            throw JetStreamError.StreamError.nameRequired
        }

        let invalidChars = CharacterSet(charactersIn: ">*. /\\")
        if name.rangeOfCharacter(from: invalidChars) != nil {
            throw JetStreamError.StreamError.invalidStreamName(name)
        }
    }

    internal static func generateConsumerName() -> String {
        let name = nextNuid()

        let hash = SHA256.hash(data: Data(name.utf8))
        let hashData = Data(hash)

        // Convert the first 8 bytes of the hash to the required format.
        let base: UInt8 = 36

        var result = [UInt8]()
        for i in 0..<8 {
            let index = Int(hashData[i] % base)
            result.append(Consumer.rdigits[index])
        }

        // Convert the result array to a string and return it.
        return String(bytes: result, encoding: .utf8)!
    }
}

/// `ConsumerInfo` is the detailed information about a JetStream consumer.
public struct ConsumerInfo: Codable, Sendable {
    /// The name of the stream that the consumer is bound to.
    public let stream: String

    /// The unique identifier for the consumer.
    public let name: String

    /// The timestamp when the consumer was created.
    public let created: String

    /// The configuration settings of the consumer, set when creating or updating the consumer.
    public let config: ConsumerConfig

    /// Information about the most recently delivered message, including its sequence numbers and timestamp.
    public let delivered: SequenceInfo

    /// Indicates the message before the first unacknowledged message.
    public let ackFloor: SequenceInfo

    /// The number of messages that have been delivered but not yet acknowledged.
    public let numAckPending: Int

    /// The number of messages that have been redelivered and not yet acknowledged.
    public let numRedelivered: Int

    /// The count of active pull requests (relevant for pull-based consumers).
    public let numWaiting: Int

    /// The number of messages that match the consumer's filter but have not been delivered yet.
    public let numPending: UInt64

    /// Information about the cluster to which this consumer belongs (if applicable).
    public let cluster: ClusterInfo?

    /// Indicates whether at least one subscription exists for the delivery subject of this consumer (only for push-based consumers).
    public let pushBound: Bool?

    /// The timestamp indicating when this information was gathered by the server.
    public let timeStamp: String

    /// State of the priority groups defined on the consumer.
    public let priorityGroups: [PriorityGroupState]?

    /// Indicates whether the consumer is paused.
    public let paused: Bool?

    /// Time left until the consumer unpauses, set only if the consumer is paused.
    public let pauseRemaining: NanoTimeInterval?

    enum CodingKeys: String, CodingKey {
        case stream = "stream_name"
        case name
        case created
        case config
        case delivered
        case ackFloor = "ack_floor"
        case numAckPending = "num_ack_pending"
        case numRedelivered = "num_redelivered"
        case numWaiting = "num_waiting"
        case numPending = "num_pending"
        case cluster
        case pushBound = "push_bound"
        case timeStamp = "ts"
        case priorityGroups = "priority_groups"
        case paused
        case pauseRemaining = "pause_remaining"
    }
}

/// `PriorityGroupState` is the state of a priority group of a consumer.
public struct PriorityGroupState: Codable, Sendable {
    /// The name of the priority group.
    public let group: String

    /// The ID of the client currently pinned to the group, if any.
    public let pinnedClientId: String?

    /// The time the client was pinned.
    public let pinnedTime: String?

    enum CodingKeys: String, CodingKey {
        case group
        case pinnedClientId = "pinned_client_id"
        case pinnedTime = "pinned_ts"
    }
}

/// `ConsumerConfig` is the configuration of a JetStream consumer.
public struct ConsumerConfig: Codable, Equatable, Sendable {
    /// Optional name for the consumer.
    public var name: String?

    /// Optional durable name for the consumer.
    public var durable: String?

    /// Optional description of the consumer.
    public var description: String?

    /// Defines from which point to start delivering messages from the stream.
    public var deliverPolicy: DeliverPolicy

    /// Optional sequence number from which to start message delivery.
    public var optStartSeq: UInt64?

    /// Optional time from which to start message delivery.
    public var optStartTime: String?

    /// Defines the acknowledgment policy for the consumer.
    public var ackPolicy: AckPolicy

    /// Defines how long the server will wait for an acknowledgment before resending a message.
    public var ackWait: NanoTimeInterval?

    /// Defines the maximum number of delivery attempts for a message.
    public var maxDeliver: Int?

    /// Specifies the optional back-off intervals for retrying message delivery after a failed acknowledgment.
    public var backOff: [NanoTimeInterval]?

    /// Can be used to filter messages delivered from the stream by given subject.
    /// It is exclusive with ``ConsumerConfig/filterSubjects``
    public var filterSubject: String?

    /// Can be used to filter messages delivered from the stream by given subjects.
    /// It is exclusive with ``ConsumerConfig/filterSubject``
    public var filterSubjects: [String]?

    /// Defines the rate at which messages are sent to the consumer.
    public var replayPolicy: ReplayPolicy

    /// Specifies an optional maximum rate of message delivery in bits per second.
    public var rateLimit: UInt64?

    /// Optional frequency for sampling acknowledgments for observability.
    public var sampleFrequency: String?

    /// Maximum number of pull requests waiting to be fulfilled.
    public var maxWaiting: Int?

    /// Maximum number of outstanding unacknowledged messages.
    public var maxAckPending: Int?

    /// Indicates whether only headers of messages should be sent (and no payload).
    public var headersOnly: Bool?

    /// Optional maximum batch size a single pull request can make.
    public var maxRequestBatch: Int?

    /// Maximum duration a single pull request will wait for messages to be available to pull.
    public var maxRequestExpires: NanoTimeInterval?

    /// Optional maximum total bytes that can be requested in a given batch.
    public var maxRequestMaxBytes: Int?

    /// Duration which instructs the server to clean up the consumer if it has been inactive for the specified duration.
    public var inactiveThreshold: NanoTimeInterval?

    /// Number of replicas for the consumer's state.
    public var replicas: Int

    /// Flag to force the consumer to use memory storage rather than inherit the storage type from the stream.
    public var memoryStorage: Bool?

    /// A set of application-defined key-value pairs for associating metadata on the consumer.
    public var metadata: [String: String]?

    /// Suspends the consumer until the given time (RFC 3339), requires nats-server v2.11.0 or later.
    public var pauseUntil: String?

    /// Priority policy of the consumer, requires nats-server v2.11.0 or later.
    public var priorityPolicy: PriorityPolicy?

    /// Time after which a pinned client is unpinned if no new pull requests are sent.
    /// Used with ``PriorityPolicy/pinned``.
    public var pinnedTTL: NanoTimeInterval?

    /// List of priority groups supported by the consumer.
    public var priorityGroups: [String]?

    /// Subject to deliver messages to, setting it makes the consumer a push consumer.
    public var deliverSubject: String?

    /// Queue group name used to load balance messages of a push consumer.
    public var deliverGroup: String?

    /// Enables flow control for push consumers.
    public var flowControl: Bool?

    /// Interval in which the server sends heartbeats to idle push consumers.
    public var idleHeartbeat: NanoTimeInterval?

    /// Creates a value with the given properties, see the properties of the same name.
    public init(
        name: String? = nil,
        durable: String? = nil,
        description: String? = nil,
        deliverPolicy: DeliverPolicy = .all,
        optStartSeq: UInt64? = nil,
        optStartTime: String? = nil,
        ackPolicy: AckPolicy = .explicit,
        ackWait: NanoTimeInterval? = nil,
        maxDeliver: Int? = nil,
        backOff: [NanoTimeInterval]? = nil,
        filterSubject: String? = nil,
        filterSubjects: [String]? = nil,
        replayPolicy: ReplayPolicy = .instant,
        rateLimit: UInt64? = nil,
        sampleFrequency: String? = nil,
        maxWaiting: Int? = nil,
        maxAckPending: Int? = nil,
        headersOnly: Bool? = nil,
        maxRequestBatch: Int? = nil,
        maxRequestExpires: NanoTimeInterval? = nil,
        maxRequestMaxBytes: Int? = nil,
        inactiveThreshold: NanoTimeInterval? = nil,
        replicas: Int = 1,
        memoryStorage: Bool? = nil,
        metadata: [String: String]? = nil,
        pauseUntil: String? = nil,
        priorityPolicy: PriorityPolicy? = nil,
        pinnedTTL: NanoTimeInterval? = nil,
        priorityGroups: [String]? = nil,
        deliverSubject: String? = nil,
        deliverGroup: String? = nil,
        flowControl: Bool? = nil,
        idleHeartbeat: NanoTimeInterval? = nil
    ) {
        self.name = name
        self.durable = durable
        self.description = description
        self.deliverPolicy = deliverPolicy
        self.optStartSeq = optStartSeq
        self.optStartTime = optStartTime
        self.ackPolicy = ackPolicy
        self.ackWait = ackWait
        self.maxDeliver = maxDeliver
        self.backOff = backOff
        self.filterSubject = filterSubject
        self.replayPolicy = replayPolicy
        self.rateLimit = rateLimit
        self.sampleFrequency = sampleFrequency
        self.maxWaiting = maxWaiting
        self.maxAckPending = maxAckPending
        self.headersOnly = headersOnly
        self.maxRequestBatch = maxRequestBatch
        self.maxRequestExpires = maxRequestExpires
        self.maxRequestMaxBytes = maxRequestMaxBytes
        self.inactiveThreshold = inactiveThreshold
        self.replicas = replicas
        self.memoryStorage = memoryStorage
        self.filterSubjects = filterSubjects
        self.metadata = metadata
        self.pauseUntil = pauseUntil
        self.priorityPolicy = priorityPolicy
        self.pinnedTTL = pinnedTTL
        self.priorityGroups = priorityGroups
        self.deliverSubject = deliverSubject
        self.deliverGroup = deliverGroup
        self.flowControl = flowControl
        self.idleHeartbeat = idleHeartbeat
    }

    enum CodingKeys: String, CodingKey {
        case name
        case durable = "durable_name"
        case description
        case deliverPolicy = "deliver_policy"
        case optStartSeq = "opt_start_seq"
        case optStartTime = "opt_start_time"
        case ackPolicy = "ack_policy"
        case ackWait = "ack_wait"
        case maxDeliver = "max_deliver"
        case backOff = "backoff"
        case filterSubject = "filter_subject"
        case replayPolicy = "replay_policy"
        case rateLimit = "rate_limit_bps"
        case sampleFrequency = "sample_freq"
        case maxWaiting = "max_waiting"
        case maxAckPending = "max_ack_pending"
        case headersOnly = "headers_only"
        case maxRequestBatch = "max_batch"
        case maxRequestExpires = "max_expires"
        case maxRequestMaxBytes = "max_bytes"
        case inactiveThreshold = "inactive_threshold"
        case replicas = "num_replicas"
        case memoryStorage = "mem_storage"
        case filterSubjects = "filter_subjects"
        case metadata
        case pauseUntil = "pause_until"
        case priorityPolicy = "priority_policy"
        case pinnedTTL = "priority_timeout"
        case priorityGroups = "priority_groups"
        case deliverSubject = "deliver_subject"
        case deliverGroup = "deliver_group"
        case flowControl = "flow_control"
        case idleHeartbeat = "idle_heartbeat"
    }
}

/// `PriorityPolicy` determines how messages are distributed between clients of priority groups.
public enum PriorityPolicy: String, Codable, Sendable {
    /// No priority policy.
    case none = ""

    /// A single client is pinned to the group and receives all messages.
    case pinned = "pinned_client"

    /// Messages are delivered to clients only when the pending count exceeds a threshold.
    case overflow

    /// Messages are delivered to clients according to their priority.
    case prioritized
}

/// `SequenceInfo` has both the consumer and the stream sequence and last activity.
public struct SequenceInfo: Codable, Equatable, Sendable {
    /// Consumer sequence number.
    public let consumer: UInt64

    /// Stream sequence number.
    public let stream: UInt64

    /// Last activity timestamp.
    public let last: String?

    enum CodingKeys: String, CodingKey {
        case consumer = "consumer_seq"
        case stream = "stream_seq"
        case last = "last_active"
    }
}

/// `DeliverPolicy` determines from which point to start delivering messages.
public enum DeliverPolicy: String, Codable, Sendable {
    /// DeliverAllPolicy starts delivering messages from the very beginning of stream. This is the default.
    case all

    /// DeliverLastPolicy will start the consumer with the last received.
    case last

    /// DeliverNewPolicy will only deliver new messages that are sent after consumer is created.
    case new

    /// DeliverByStartSequencePolicy will deliver messages starting from a sequence configured with OptStartSeq in ConsumerConfig.
    case byStartSequence = "by_start_sequence"

    /// DeliverByStartTimePolicy will deliver messages starting from a given configured with OptStartTime in ConsumerConfig.
    case byStartTime = "by_start_time"

    /// DeliverLastPerSubjectPolicy will start the consumer with the last for all subjects received.
    case lastPerSubject = "last_per_subject"
}

/// `AckPolicy` determines how the consumer should acknowledge delivered messages.
public enum AckPolicy: String, Codable, Sendable {
    /// AckNonePolicy requires no acks for delivered messages./
    case none

    /// AckAllPolicy when acking a sequence number, this implicitly acks sequences below this one as well.
    case all

    /// AckExplicitPolicy requires ack or nack for all messages.
    case explicit
}

/// `ReplayPolicy` determines how the consumer should replay messages it already has queued in the stream.
public enum ReplayPolicy: String, Codable, Sendable {
    /// ReplayInstantPolicy will replay messages as fast as possible./
    case instant

    /// ReplayOriginalPolicy will maintain the same timing as the messages received.
    case original
}

internal struct CreateConsumerRequest: Codable {
    internal let stream: String
    internal let config: ConsumerConfig
    internal let action: String?

    enum CodingKeys: String, CodingKey {
        case stream = "stream_name"
        case config
        case action
    }
}

struct ConsumerDeleteResponse: Codable {
    let success: Bool
}
