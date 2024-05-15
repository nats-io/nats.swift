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

public struct JetStreamAPIResponse: Codable {
    public let type: String
    public let error: JetStreamError
}

public struct JetStreamError: Codable, Error {
    public var code: UInt
    //FIXME(jrm): This should be mapped to predefined JetStream errors from the server.
    public var errorCode: ErrorCode
    public var description: String?

    enum CodingKeys: String, CodingKey {
        case code = "code"
        case errorCode = "err_code"
        case description = "description"
    }
}

public struct ErrorCode: Codable, Equatable {
    public let rawValue: UInt64
    /// Peer not a member
    public static let clusterPeerNotMember = ErrorCode(rawValue: 10040)

    /// Consumer expected to be ephemeral but detected a durable name set in subject
    public static let consumerEphemeralWithDurable = ErrorCode(rawValue: 10019)

    /// Stream external delivery prefix overlaps with stream subject
    public static let streamExternalDeletePrefixOverlaps = ErrorCode(rawValue: 10022)

    /// Resource limits exceeded for account
    public static let accountResourcesExceeded = ErrorCode(rawValue: 10002)

    /// Jetstream system temporarily unavailable
    public static let clusterNotAvailable = ErrorCode(rawValue: 10008)

    /// Subjects overlap with an existing stream
    public static let streamSubjectOverlap = ErrorCode(rawValue: 10065)

    /// Wrong last sequence
    public static let streamWrongLastSequence = ErrorCode(rawValue: 10071)

    /// Template name in subject does not match request
    public static let nameNotMatchSubject = ErrorCode(rawValue: 10073)

    /// No suitable peers for placement
    public static let clusterNoPeers = ErrorCode(rawValue: 10005)

    /// Consumer expected to be ephemeral but a durable name was set in request
    public static let consumerEphemeralWithDurableName = ErrorCode(rawValue: 10020)

    /// Insufficient resources
    public static let insufficientResources = ErrorCode(rawValue: 10023)

    /// Stream mirror must have max message size >= source
    public static let mirrorMaxMessageSizeTooBig = ErrorCode(rawValue: 10030)

    /// Generic error from stream deletion operation
    public static let streamDeleteFailed = ErrorCode(rawValue: 10067)

    /// Bad request
    public static let badRequest = ErrorCode(rawValue: 10003)

    /// Not currently supported in clustered mode
    public static let notSupportedInClusterMode = ErrorCode(rawValue: 10036)

    /// Consumer not found
    public static let consumerNotFound = ErrorCode(rawValue: 10014)

    /// Stream source must have max message size >= target
    public static let sourceMaxMessageSizeTooBig = ErrorCode(rawValue: 10046)

    /// Generic error when stream operation fails.
    public static let streamAssignment = ErrorCode(rawValue: 10048)

    /// Message size exceeds maximum allowed
    public static let streamMessageExceedsMaximum = ErrorCode(rawValue: 10054)

    /// Generic error for stream creation error with a string
    public static let streamCreateTemplate = ErrorCode(rawValue: 10066)

    /// Invalid JSON
    public static let invalidJson = ErrorCode(rawValue: 10025)

    /// Stream external delivery prefix must not contain wildcards
    public static let streamInvalidExternalDeliverySubject = ErrorCode(rawValue: 10024)

    /// Restore failed
    public static let streamRestore = ErrorCode(rawValue: 10062)

    /// Incomplete results
    public static let clusterIncomplete = ErrorCode(rawValue: 10004)

    /// Account not found
    public static let noAccount = ErrorCode(rawValue: 10035)

    /// General RAFT error
    public static let raftGeneral = ErrorCode(rawValue: 10041)

    /// Jetstream unable to subscribe to restore snapshot
    public static let restoreSubscribeFailed = ErrorCode(rawValue: 10042)

    /// Stream deletion failed
    public static let streamDelete = ErrorCode(rawValue: 10050)

    /// Stream external api prefix must not overlap
    public static let streamExternalApiOverlap = ErrorCode(rawValue: 10021)

    /// Stream mirrors can not contain subjects
    public static let mirrorWithSubjects = ErrorCode(rawValue: 10034)

    /// Jetstream not enabled
    public static let jetstreamNotEnabled = ErrorCode(rawValue: 10076)

    /// Jetstream not enabled for account
    public static let jetstreamNotEnabledForAccount = ErrorCode(rawValue: 10039)

    /// Sequence not found
    public static let sequenceNotFound = ErrorCode(rawValue: 10043)

    /// Stream mirror configuration can not be updated
    public static let streamMirrorNotUpdatable = ErrorCode(rawValue: 10055)

    /// Expected stream sequence does not match
    public static let streamSequenceNotMatch = ErrorCode(rawValue: 10063)

    /// Wrong last msg id
    public static let streamWrongLastMessageId = ErrorCode(rawValue: 10070)

    /// Jetstream unable to open temp storage for restore
    public static let tempStorageFailed = ErrorCode(rawValue: 10072)

    /// Insufficient storage resources available
    public static let storageResourcesExceeded = ErrorCode(rawValue: 10047)

    /// Stream name in subject does not match request
    public static let streamMismatch = ErrorCode(rawValue: 10056)

    /// Expected stream does not match
    public static let streamNotMatch = ErrorCode(rawValue: 10060)

    /// Setting up consumer mirror failed
    public static let mirrorConsumerSetupFailed = ErrorCode(rawValue: 10029)

    /// Expected an empty request payload
    public static let notEmptyRequest = ErrorCode(rawValue: 10038)

    /// Stream name already in use with a different configuration
    public static let streamNameExist = ErrorCode(rawValue: 10058)

    /// Tags placement not supported for operation
    public static let clusterTags = ErrorCode(rawValue: 10011)

    /// Maximum consumers limit reached
    public static let maximumConsumersLimit = ErrorCode(rawValue: 10026)

    /// General source consumer setup failure
    public static let sourceConsumerSetupFailed = ErrorCode(rawValue: 10045)

    /// Consumer creation failed
    public static let consumerCreate = ErrorCode(rawValue: 10012)

    /// Consumer expected to be durable but no durable name set in subject
    public static let consumerDurableNameNotInSubject = ErrorCode(rawValue: 10016)

    /// Stream limits error
    public static let streamLimits = ErrorCode(rawValue: 10053)

    /// Replicas configuration can not be updated
    public static let streamReplicasNotUpdatable = ErrorCode(rawValue: 10061)

    /// Template not found
    public static let streamTemplateNotFound = ErrorCode(rawValue: 10068)

    /// Jetstream cluster not assigned to this server
    public static let clusterNotAssigned = ErrorCode(rawValue: 10007)

    /// Jetstream cluster can't handle request
    public static let clusterNotLeader = ErrorCode(rawValue: 10009)

    /// Consumer name already in use
    public static let consumerNameExist = ErrorCode(rawValue: 10013)

    /// Stream mirrors can't also contain other sources
    public static let mirrorWithSources = ErrorCode(rawValue: 10031)

    /// Stream not found
    public static let streamNotFound = ErrorCode(rawValue: 10059)

    /// Jetstream clustering support required
    public static let clusterRequired = ErrorCode(rawValue: 10010)

    /// Consumer expected to be durable but a durable name was not set
    public static let consumerDurableNameNotSet = ErrorCode(rawValue: 10018)

    /// Maximum number of streams reached
    public static let maximumStreamsLimit = ErrorCode(rawValue: 10027)

    /// Stream mirrors can not have both start seq and start time configured
    public static let mirrorWithStartSequenceAndTime = ErrorCode(rawValue: 10032)

    /// Stream snapshot failed
    public static let streamSnapshot = ErrorCode(rawValue: 10064)

    /// Stream update failed
    public static let streamUpdate = ErrorCode(rawValue: 10069)

    /// Jetstream not in clustered mode
    public static let clusterNotActive = ErrorCode(rawValue: 10006)

    /// Consumer name in subject does not match durable name in request
    public static let consumerDurableNameNotMatchSubject = ErrorCode(rawValue: 10017)

    /// Insufficient memory resources available
    public static let memoryResourcesExceeded = ErrorCode(rawValue: 10028)

    /// Stream mirrors can not contain filtered subjects
    public static let mirrorWithSubjectFilters = ErrorCode(rawValue: 10033)

    /// Stream create failed with a string
    public static let streamCreate = ErrorCode(rawValue: 10049)

    /// Server is not a member of the cluster
    public static let clusterServerNotMember = ErrorCode(rawValue: 10044)

    /// No message found
    public static let noMessageFound = ErrorCode(rawValue: 10037)

    /// Deliver subject not valid
    public static let snapshotDeliverSubjectInvalid = ErrorCode(rawValue: 10015)

    /// General stream failure
    public static let streamGeneralor = ErrorCode(rawValue: 10051)

    /// Invalid stream config
    public static let streamInvalidConfig = ErrorCode(rawValue: 10052)

    /// Replicas > 1 not supported in non-clustered mode
    public static let streamReplicasNotSupported = ErrorCode(rawValue: 10074)

    /// Stream message delete failed
    public static let streamMessageDeleteFailed = ErrorCode(rawValue: 10057)

    /// Peer remap failed
    public static let peerRemap = ErrorCode(rawValue: 10075)

    /// Stream store failed
    public static let streamStoreFailed = ErrorCode(rawValue: 10077)

    /// Consumer config required
    public static let consumerConfigRequired = ErrorCode(rawValue: 10078)

    /// Consumer deliver subject has wildcards
    public static let consumerDeliverToWildcards = ErrorCode(rawValue: 10079)

    /// Consumer in push mode can not set max waiting
    public static let consumerPushMaxWaiting = ErrorCode(rawValue: 10080)

    /// Consumer deliver subject forms a cycle
    public static let consumerDeliverCycle = ErrorCode(rawValue: 10081)

    /// Consumer requires ack policy for max ack pending
    public static let consumerMaxPendingAckPolicyRequired = ErrorCode(rawValue: 10082)

    /// Consumer idle heartbeat needs to be >= 100ms
    public static let consumerSmallHeartbeat = ErrorCode(rawValue: 10083)

    /// Consumer in pull mode requires ack policy
    public static let consumerPullRequiresAck = ErrorCode(rawValue: 10084)

    /// Consumer in pull mode requires a durable name
    public static let consumerPullNotDurable = ErrorCode(rawValue: 10085)

    /// Consumer in pull mode can not have rate limit set
    public static let consumerPullWithRateLimit = ErrorCode(rawValue: 10086)

    /// Consumer max waiting needs to be positive
    public static let consumerMaxWaitingNegative = ErrorCode(rawValue: 10087)

    /// Consumer idle heartbeat requires a push based consumer
    public static let consumerHeartbeatRequiresPush = ErrorCode(rawValue: 10088)

    /// Consumer flow control requires a push based consumer
    public static let consumerFlowControlRequiresPush = ErrorCode(rawValue: 10089)

    /// Consumer direct requires a push based consumer
    public static let consumerDirectRequiresPush = ErrorCode(rawValue: 10090)

    /// Consumer direct requires an ephemeral consumer
    public static let consumerDirectRequiresEphemeral = ErrorCode(rawValue: 10091)

    /// Consumer direct on a mapped consumer
    public static let consumerOnMapped = ErrorCode(rawValue: 10092)

    /// Consumer filter subject is not a valid subset of the interest subjects
    public static let consumerFilterNotSubset = ErrorCode(rawValue: 10093)

    /// Invalid consumer policy
    public static let consumerInvalidPolicy = ErrorCode(rawValue: 10094)

    /// Failed to parse consumer sampling configuration
    public static let consumerInvalidSampling = ErrorCode(rawValue: 10095)

    /// Stream not valid
    public static let streamInvalid = ErrorCode(rawValue: 10096)

    /// Workqueue stream requires explicit ack
    public static let consumerWqRequiresExplicitAck = ErrorCode(rawValue: 10098)

    /// Multiple non-filtered consumers not allowed on workqueue stream
    public static let consumerWqMultipleUnfiltered = ErrorCode(rawValue: 10099)

    /// Filtered consumer not unique on workqueue stream
    public static let consumerWqConsumerNotUnique = ErrorCode(rawValue: 10100)

    /// Consumer must be deliver all on workqueue stream
    public static let consumerWqConsumerNotDeliverAll = ErrorCode(rawValue: 10101)

    /// Consumer name is too long
    public static let consumerNameTooLong = ErrorCode(rawValue: 10102)

    /// Durable name can not contain token separators and wildcards
    public static let consumerBadDurableName = ErrorCode(rawValue: 10103)

    /// Error creating store for consumer
    public static let consumerStoreFailed = ErrorCode(rawValue: 10104)

    /// Consumer already exists and is still active
    public static let consumerExistingActive = ErrorCode(rawValue: 10105)

    /// Consumer replacement durable config not the same
    public static let consumerReplacementWithDifferentName = ErrorCode(rawValue: 10106)

    /// Consumer description is too long
    public static let consumerDescriptionTooLong = ErrorCode(rawValue: 10107)

    /// Header size exceeds maximum allowed of 64k
    public static let streamHeaderExceedsMaximum = ErrorCode(rawValue: 10097)

    /// Consumer with flow control also needs heartbeats
    public static let consumerWithFlowControlNeedsHeartbeats = ErrorCode(rawValue: 10108)

    /// Invalid operation on sealed stream
    public static let streamSealed = ErrorCode(rawValue: 10109)

    /// Stream purge failed
    public static let streamPurgeFailed = ErrorCode(rawValue: 10110)

    /// Stream rollup failed
    public static let streamRollupFailed = ErrorCode(rawValue: 10111)

    /// Invalid push consumer deliver subject
    public static let consumerInvalidDeliverSubject = ErrorCode(rawValue: 10112)

    /// Account requires a stream config to have max bytes set
    public static let streamMaxBytesRequired = ErrorCode(rawValue: 10113)

    /// Consumer max request batch needs to be > 0
    public static let consumerMaxRequestBatchNegative = ErrorCode(rawValue: 10114)

    /// Consumer max request expires needs to be >= 1ms
    public static let consumerMaxRequestExpiresToSmall = ErrorCode(rawValue: 10115)

    /// Max deliver is required to be > length of backoff values
    public static let consumerMaxDeliverBackoff = ErrorCode(rawValue: 10116)

    /// Subject details would exceed maximum allowed
    public static let streamInfoMaxSubjects = ErrorCode(rawValue: 10117)

    /// Stream is offline
    public static let streamOffline = ErrorCode(rawValue: 10118)

    /// Consumer is offline
    public static let consumerOffline = ErrorCode(rawValue: 10119)

    /// No jetstream default or applicable tiered limit present
    public static let noLimits = ErrorCode(rawValue: 10120)

    /// Consumer max ack pending exceeds system limit
    public static let consumerMaxPendingAckExcess = ErrorCode(rawValue: 10121)

    /// Stream max bytes exceeds account limit max stream bytes
    public static let streamMaxStreamBytesExceeded = ErrorCode(rawValue: 10122)

    /// Can not move and scale a stream in a single update
    public static let streamMoveAndScale = ErrorCode(rawValue: 10123)

    /// Stream move already in progress
    public static let streamMoveInProgress = ErrorCode(rawValue: 10124)

    /// Consumer max request batch exceeds server limit
    public static let consumerMaxRequestBatchExceeded = ErrorCode(rawValue: 10125)

    /// Consumer config replica count exceeds parent stream
    public static let consumerReplicasExceedsStream = ErrorCode(rawValue: 10126)

    /// Consumer name can not contain path separators
    public static let consumerNameContainsPathSeparators = ErrorCode(rawValue: 10127)

    /// Stream name can not contain path separators
    public static let streamNameContainsPathSeparators = ErrorCode(rawValue: 10128)

    /// Stream move not in progress
    public static let streamMoveNotInProgress = ErrorCode(rawValue: 10129)

    /// Stream name already in use, cannot restore
    public static let streamNameExistRestoreFailed = ErrorCode(rawValue: 10130)

    /// Consumer create request did not match filtered subject from create subject
    public static let consumerCreateFilterSubjectMismatch = ErrorCode(rawValue: 10131)

    /// Consumer durable and name have to be equal if both are provided
    public static let consumerCreateDurableAndNameMismatch = ErrorCode(rawValue: 10132)

    /// Replicas count cannot be negative
    public static let replicasCountCannotBeNegative = ErrorCode(rawValue: 10133)

    /// Consumer config replicas must match interest retention stream's replicas
    public static let consumerReplicasShouldMatchStream = ErrorCode(rawValue: 10134)

    /// Consumer metadata exceeds maximum size
    public static let consumerMetadataLength = ErrorCode(rawValue: 10135)

    /// Consumer cannot have both filter_subject and filter_subjects specified
    public static let consumerDuplicateFilterSubjects = ErrorCode(rawValue: 10136)

    /// Consumer with multiple subject filters cannot use subject based api
    public static let consumerMultipleFiltersNotAllowed = ErrorCode(rawValue: 10137)

    /// Consumer subject filters cannot overlap
    public static let consumerOverlappingSubjectFilters = ErrorCode(rawValue: 10138)

    /// Consumer filter in filter_subjects cannot be empty
    public static let consumerEmptyFilter = ErrorCode(rawValue: 10139)

    /// Duplicate source configuration detected
    public static let sourceDuplicateDetected = ErrorCode(rawValue: 10140)

    /// Sourced stream name is invalid
    public static let sourceInvalidStreamName = ErrorCode(rawValue: 10141)

    /// Mirrored stream name is invalid
    public static let mirrorInvalidStreamName = ErrorCode(rawValue: 10142)

    /// Source with multiple subject transforms cannot also have a single subject filter
    public static let sourceMultipleFiltersNotAllowed = ErrorCode(rawValue: 10144)

    /// Source subject filter is invalid
    public static let sourceInvalidSubjectFilter = ErrorCode(rawValue: 10145)

    /// Source transform destination is invalid
    public static let sourceInvalidTransformDestination = ErrorCode(rawValue: 10146)

    /// Source filters cannot overlap
    public static let sourceOverlappingSubjectFilters = ErrorCode(rawValue: 10147)

    /// Consumer already exists
    public static let consumerAlreadyExists = ErrorCode(rawValue: 10148)

    /// Consumer does not exist
    public static let consumerDoesNotExist = ErrorCode(rawValue: 10149)

    /// Mirror with multiple subject transforms cannot also have a single subject filter
    public static let mirrorMultipleFiltersNotAllowed = ErrorCode(rawValue: 10150)

    /// Mirror subject filter is invalid
    public static let mirrorInvalidSubjectFilter = ErrorCode(rawValue: 10151)

    /// Mirror subject filters cannot overlap
    public static let mirrorOverlappingSubjectFilters = ErrorCode(rawValue: 10152)

    /// Consumer inactive threshold exceeds system limit
    public static let consumerInactiveThresholdExcess = ErrorCode(rawValue: 10153)

}

extension ErrorCode {
    // Encoding
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    // Decoding
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let decodedValue = try container.decode(UInt64.self)
        self = ErrorCode(rawValue: decodedValue)
    }
}

public enum Response<T: Codable>: Codable {
    case success(T)
    case error(JetStreamAPIResponse)

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()

        // Try to decode the expected success type T first
        if let successResponse = try? container.decode(T.self) {
            self = .success(successResponse)
            return
        }

        // If that fails, try to decode ErrorResponse
        let errorResponse = try container.decode(JetStreamAPIResponse.self)
        self = .error(errorResponse)
        return
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .success(let successData):
            try container.encode(successData)
        case .error(let errorData):
            try container.encode(errorData)
        }
    }
}
