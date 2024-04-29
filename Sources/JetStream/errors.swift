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
    let type: String
    let error: JetStreamError
}

public struct JetStreamError: Codable {
    var code: UInt
    //FIXME(jrm): This should be mapped to predefined JetStream errors from the server.
    var errorCode: ErrorCode
    var description: String?

    enum CodingKeys: String, CodingKey {
        case code = "code"
        case errorCode = "err_code"
        case description = "description"
    }
}

struct ErrorCode: Codable, Equatable {
    let rawValue: UInt64
    /// Peer not a member
    static let clusterPeerNotMember = ErrorCode(rawValue: 10040)

    /// Consumer expected to be ephemeral but detected a durable name set in subject
    static let consumerEphemeralWithDurable = ErrorCode(rawValue: 10019)

    /// Stream external delivery prefix overlaps with stream subject
    static let streamExternalDeletePrefixOverlaps = ErrorCode(rawValue: 10022)

    /// Resource limits exceeded for account
    static let accountResourcesExceeded = ErrorCode(rawValue: 10002)

    /// Jetstream system temporarily unavailable
    static let clusterNotAvailable = ErrorCode(rawValue: 10008)

    /// Subjects overlap with an existing stream
    static let streamSubjectOverlap = ErrorCode(rawValue: 10065)

    /// Wrong last sequence
    static let streamWrongLastSequence = ErrorCode(rawValue: 10071)

    /// Template name in subject does not match request
    static let nameNotMatchSubject = ErrorCode(rawValue: 10073)

    /// No suitable peers for placement
    static let clusterNoPeers = ErrorCode(rawValue: 10005)

    /// Consumer expected to be ephemeral but a durable name was set in request
    static let consumerEphemeralWithDurableName = ErrorCode(rawValue: 10020)

    /// Insufficient resources
    static let insufficientResources = ErrorCode(rawValue: 10023)

    /// Stream mirror must have max message size >= source
    static let mirrorMaxMessageSizeTooBig = ErrorCode(rawValue: 10030)

    /// Generic error from stream deletion operation
    static let streamDeleteFailed = ErrorCode(rawValue: 10067)

    /// Bad request
    static let badRequest = ErrorCode(rawValue: 10003)

    /// Not currently supported in clustered mode
    static let notSupportedInClusterMode = ErrorCode(rawValue: 10036)

    /// Consumer not found
    static let consumerNotFound = ErrorCode(rawValue: 10014)

    /// Stream source must have max message size >= target
    static let sourceMaxMessageSizeTooBig = ErrorCode(rawValue: 10046)

    /// Generic error when stream operation fails.
    static let streamAssignment = ErrorCode(rawValue: 10048)

    /// Message size exceeds maximum allowed
    static let streamMessageExceedsMaximum = ErrorCode(rawValue: 10054)

    /// Generic error for stream creation error with a string
    static let streamCreateTemplate = ErrorCode(rawValue: 10066)

    /// Invalid JSON
    static let invalidJson = ErrorCode(rawValue: 10025)

    /// Stream external delivery prefix must not contain wildcards
    static let streamInvalidExternalDeliverySubject = ErrorCode(rawValue: 10024)

    /// Restore failed
    static let streamRestore = ErrorCode(rawValue: 10062)

    /// Incomplete results
    static let clusterIncomplete = ErrorCode(rawValue: 10004)

    /// Account not found
    static let noAccount = ErrorCode(rawValue: 10035)

    /// General RAFT error
    static let raftGeneral = ErrorCode(rawValue: 10041)

    /// Jetstream unable to subscribe to restore snapshot
    static let restoreSubscribeFailed = ErrorCode(rawValue: 10042)

    /// Stream deletion failed
    static let streamDelete = ErrorCode(rawValue: 10050)

    /// Stream external api prefix must not overlap
    static let streamExternalApiOverlap = ErrorCode(rawValue: 10021)

    /// Stream mirrors can not contain subjects
    static let mirrorWithSubjects = ErrorCode(rawValue: 10034)

    /// Jetstream not enabled
    static let jetstreamNotEnabled = ErrorCode(rawValue: 10076)

    /// Jetstream not enabled for account
    static let jetstreamNotEnabledForAccount = ErrorCode(rawValue: 10039)

    /// Sequence not found
    static let sequenceNotFound = ErrorCode(rawValue: 10043)

    /// Stream mirror configuration can not be updated
    static let streamMirrorNotUpdatable = ErrorCode(rawValue: 10055)

    /// Expected stream sequence does not match
    static let streamSequenceNotMatch = ErrorCode(rawValue: 10063)

    /// Wrong last msg id
    static let streamWrongLastMessageId = ErrorCode(rawValue: 10070)

    /// Jetstream unable to open temp storage for restore
    static let tempStorageFailed = ErrorCode(rawValue: 10072)

    /// Insufficient storage resources available
    static let storageResourcesExceeded = ErrorCode(rawValue: 10047)

    /// Stream name in subject does not match request
    static let streamMismatch = ErrorCode(rawValue: 10056)

    /// Expected stream does not match
    static let streamNotMatch = ErrorCode(rawValue: 10060)

    /// Setting up consumer mirror failed
    static let mirrorConsumerSetupFailed = ErrorCode(rawValue: 10029)

    /// Expected an empty request payload
    static let notEmptyRequest = ErrorCode(rawValue: 10038)

    /// Stream name already in use with a different configuration
    static let streamNameExist = ErrorCode(rawValue: 10058)

    /// Tags placement not supported for operation
    static let clusterTags = ErrorCode(rawValue: 10011)

    /// Maximum consumers limit reached
    static let maximumConsumersLimit = ErrorCode(rawValue: 10026)

    /// General source consumer setup failure
    static let sourceConsumerSetupFailed = ErrorCode(rawValue: 10045)

    /// Consumer creation failed
    static let consumerCreate = ErrorCode(rawValue: 10012)

    /// Consumer expected to be durable but no durable name set in subject
    static let consumerDurableNameNotInSubject = ErrorCode(rawValue: 10016)

    /// Stream limits error
    static let streamLimits = ErrorCode(rawValue: 10053)

    /// Replicas configuration can not be updated
    static let streamReplicasNotUpdatable = ErrorCode(rawValue: 10061)

    /// Template not found
    static let streamTemplateNotFound = ErrorCode(rawValue: 10068)

    /// Jetstream cluster not assigned to this server
    static let clusterNotAssigned = ErrorCode(rawValue: 10007)

    /// Jetstream cluster can't handle request
    static let clusterNotLeader = ErrorCode(rawValue: 10009)

    /// Consumer name already in use
    static let consumerNameExist = ErrorCode(rawValue: 10013)

    /// Stream mirrors can't also contain other sources
    static let mirrorWithSources = ErrorCode(rawValue: 10031)

    /// Stream not found
    static let streamNotFound = ErrorCode(rawValue: 10059)

    /// Jetstream clustering support required
    static let clusterRequired = ErrorCode(rawValue: 10010)

    /// Consumer expected to be durable but a durable name was not set
    static let consumerDurableNameNotSet = ErrorCode(rawValue: 10018)

    /// Maximum number of streams reached
    static let maximumStreamsLimit = ErrorCode(rawValue: 10027)

    /// Stream mirrors can not have both start seq and start time configured
    static let mirrorWithStartSequenceAndTime = ErrorCode(rawValue: 10032)

    /// Stream snapshot failed
    static let streamSnapshot = ErrorCode(rawValue: 10064)

    /// Stream update failed
    static let streamUpdate = ErrorCode(rawValue: 10069)

    /// Jetstream not in clustered mode
    static let clusterNotActive = ErrorCode(rawValue: 10006)

    /// Consumer name in subject does not match durable name in request
    static let consumerDurableNameNotMatchSubject = ErrorCode(rawValue: 10017)

    /// Insufficient memory resources available
    static let memoryResourcesExceeded = ErrorCode(rawValue: 10028)

    /// Stream mirrors can not contain filtered subjects
    static let mirrorWithSubjectFilters = ErrorCode(rawValue: 10033)

    /// Stream create failed with a string
    static let streamCreate = ErrorCode(rawValue: 10049)

    /// Server is not a member of the cluster
    static let clusterServerNotMember = ErrorCode(rawValue: 10044)

    /// No message found
    static let noMessageFound = ErrorCode(rawValue: 10037)

    /// Deliver subject not valid
    static let snapshotDeliverSubjectInvalid = ErrorCode(rawValue: 10015)

    /// General stream failure
    static let streamGeneralor = ErrorCode(rawValue: 10051)

    /// Invalid stream config
    static let streamInvalidConfig = ErrorCode(rawValue: 10052)

    /// Replicas > 1 not supported in non-clustered mode
    static let streamReplicasNotSupported = ErrorCode(rawValue: 10074)

    /// Stream message delete failed
    static let streamMessageDeleteFailed = ErrorCode(rawValue: 10057)

    /// Peer remap failed
    static let peerRemap = ErrorCode(rawValue: 10075)

    /// Stream store failed
    static let streamStoreFailed = ErrorCode(rawValue: 10077)

    /// Consumer config required
    static let consumerConfigRequired = ErrorCode(rawValue: 10078)

    /// Consumer deliver subject has wildcards
    static let consumerDeliverToWildcards = ErrorCode(rawValue: 10079)

    /// Consumer in push mode can not set max waiting
    static let consumerPushMaxWaiting = ErrorCode(rawValue: 10080)

    /// Consumer deliver subject forms a cycle
    static let consumerDeliverCycle = ErrorCode(rawValue: 10081)

    /// Consumer requires ack policy for max ack pending
    static let consumerMaxPendingAckPolicyRequired = ErrorCode(rawValue: 10082)

    /// Consumer idle heartbeat needs to be >= 100ms
    static let consumerSmallHeartbeat = ErrorCode(rawValue: 10083)

    /// Consumer in pull mode requires ack policy
    static let consumerPullRequiresAck = ErrorCode(rawValue: 10084)

    /// Consumer in pull mode requires a durable name
    static let consumerPullNotDurable = ErrorCode(rawValue: 10085)

    /// Consumer in pull mode can not have rate limit set
    static let consumerPullWithRateLimit = ErrorCode(rawValue: 10086)

    /// Consumer max waiting needs to be positive
    static let consumerMaxWaitingNegative = ErrorCode(rawValue: 10087)

    /// Consumer idle heartbeat requires a push based consumer
    static let consumerHeartbeatRequiresPush = ErrorCode(rawValue: 10088)

    /// Consumer flow control requires a push based consumer
    static let consumerFlowControlRequiresPush = ErrorCode(rawValue: 10089)

    /// Consumer direct requires a push based consumer
    static let consumerDirectRequiresPush = ErrorCode(rawValue: 10090)

    /// Consumer direct requires an ephemeral consumer
    static let consumerDirectRequiresEphemeral = ErrorCode(rawValue: 10091)

    /// Consumer direct on a mapped consumer
    static let consumerOnMapped = ErrorCode(rawValue: 10092)

    /// Consumer filter subject is not a valid subset of the interest subjects
    static let consumerFilterNotSubset = ErrorCode(rawValue: 10093)

    /// Invalid consumer policy
    static let consumerInvalidPolicy = ErrorCode(rawValue: 10094)

    /// Failed to parse consumer sampling configuration
    static let consumerInvalidSampling = ErrorCode(rawValue: 10095)

    /// Stream not valid
    static let streamInvalid = ErrorCode(rawValue: 10096)

    /// Workqueue stream requires explicit ack
    static let consumerWqRequiresExplicitAck = ErrorCode(rawValue: 10098)

    /// Multiple non-filtered consumers not allowed on workqueue stream
    static let consumerWqMultipleUnfiltered = ErrorCode(rawValue: 10099)

    /// Filtered consumer not unique on workqueue stream
    static let consumerWqConsumerNotUnique = ErrorCode(rawValue: 10100)

    /// Consumer must be deliver all on workqueue stream
    static let consumerWqConsumerNotDeliverAll = ErrorCode(rawValue: 10101)

    /// Consumer name is too long
    static let consumerNameTooLong = ErrorCode(rawValue: 10102)

    /// Durable name can not contain token separators and wildcards
    static let consumerBadDurableName = ErrorCode(rawValue: 10103)

    /// Error creating store for consumer
    static let consumerStoreFailed = ErrorCode(rawValue: 10104)

    /// Consumer already exists and is still active
    static let consumerExistingActive = ErrorCode(rawValue: 10105)

    /// Consumer replacement durable config not the same
    static let consumerReplacementWithDifferentName = ErrorCode(rawValue: 10106)

    /// Consumer description is too long
    static let consumerDescriptionTooLong = ErrorCode(rawValue: 10107)

    /// Header size exceeds maximum allowed of 64k
    static let streamHeaderExceedsMaximum = ErrorCode(rawValue: 10097)

    /// Consumer with flow control also needs heartbeats
    static let consumerWithFlowControlNeedsHeartbeats = ErrorCode(rawValue: 10108)

    /// Invalid operation on sealed stream
    static let streamSealed = ErrorCode(rawValue: 10109)

    /// Stream purge failed
    static let streamPurgeFailed = ErrorCode(rawValue: 10110)

    /// Stream rollup failed
    static let streamRollupFailed = ErrorCode(rawValue: 10111)

    /// Invalid push consumer deliver subject
    static let consumerInvalidDeliverSubject = ErrorCode(rawValue: 10112)

    /// Account requires a stream config to have max bytes set
    static let streamMaxBytesRequired = ErrorCode(rawValue: 10113)

    /// Consumer max request batch needs to be > 0
    static let consumerMaxRequestBatchNegative = ErrorCode(rawValue: 10114)

    /// Consumer max request expires needs to be >= 1ms
    static let consumerMaxRequestExpiresToSmall = ErrorCode(rawValue: 10115)

    /// Max deliver is required to be > length of backoff values
    static let consumerMaxDeliverBackoff = ErrorCode(rawValue: 10116)

    /// Subject details would exceed maximum allowed
    static let streamInfoMaxSubjects = ErrorCode(rawValue: 10117)

    /// Stream is offline
    static let streamOffline = ErrorCode(rawValue: 10118)

    /// Consumer is offline
    static let consumerOffline = ErrorCode(rawValue: 10119)

    /// No jetstream default or applicable tiered limit present
    static let noLimits = ErrorCode(rawValue: 10120)

    /// Consumer max ack pending exceeds system limit
    static let consumerMaxPendingAckExcess = ErrorCode(rawValue: 10121)

    /// Stream max bytes exceeds account limit max stream bytes
    static let streamMaxStreamBytesExceeded = ErrorCode(rawValue: 10122)

    /// Can not move and scale a stream in a single update
    static let streamMoveAndScale = ErrorCode(rawValue: 10123)

    /// Stream move already in progress
    static let streamMoveInProgress = ErrorCode(rawValue: 10124)

    /// Consumer max request batch exceeds server limit
    static let consumerMaxRequestBatchExceeded = ErrorCode(rawValue: 10125)

    /// Consumer config replica count exceeds parent stream
    static let consumerReplicasExceedsStream = ErrorCode(rawValue: 10126)

    /// Consumer name can not contain path separators
    static let consumerNameContainsPathSeparators = ErrorCode(rawValue: 10127)

    /// Stream name can not contain path separators
    static let streamNameContainsPathSeparators = ErrorCode(rawValue: 10128)

    /// Stream move not in progress
    static let streamMoveNotInProgress = ErrorCode(rawValue: 10129)

    /// Stream name already in use, cannot restore
    static let streamNameExistRestoreFailed = ErrorCode(rawValue: 10130)

    /// Consumer create request did not match filtered subject from create subject
    static let consumerCreateFilterSubjectMismatch = ErrorCode(rawValue: 10131)

    /// Consumer durable and name have to be equal if both are provided
    static let consumerCreateDurableAndNameMismatch = ErrorCode(rawValue: 10132)

    /// Replicas count cannot be negative
    static let replicasCountCannotBeNegative = ErrorCode(rawValue: 10133)

    /// Consumer config replicas must match interest retention stream's replicas
    static let consumerReplicasShouldMatchStream = ErrorCode(rawValue: 10134)

    /// Consumer metadata exceeds maximum size
    static let consumerMetadataLength = ErrorCode(rawValue: 10135)

    /// Consumer cannot have both filter_subject and filter_subjects specified
    static let consumerDuplicateFilterSubjects = ErrorCode(rawValue: 10136)

    /// Consumer with multiple subject filters cannot use subject based api
    static let consumerMultipleFiltersNotAllowed = ErrorCode(rawValue: 10137)

    /// Consumer subject filters cannot overlap
    static let consumerOverlappingSubjectFilters = ErrorCode(rawValue: 10138)

    /// Consumer filter in filter_subjects cannot be empty
    static let consumerEmptyFilter = ErrorCode(rawValue: 10139)

    /// Duplicate source configuration detected
    static let sourceDuplicateDetected = ErrorCode(rawValue: 10140)

    /// Sourced stream name is invalid
    static let sourceInvalidStreamName = ErrorCode(rawValue: 10141)

    /// Mirrored stream name is invalid
    static let mirrorInvalidStreamName = ErrorCode(rawValue: 10142)

    /// Source with multiple subject transforms cannot also have a single subject filter
    static let sourceMultipleFiltersNotAllowed = ErrorCode(rawValue: 10144)

    /// Source subject filter is invalid
    static let sourceInvalidSubjectFilter = ErrorCode(rawValue: 10145)

    /// Source transform destination is invalid
    static let sourceInvalidTransformDestination = ErrorCode(rawValue: 10146)

    /// Source filters cannot overlap
    static let sourceOverlappingSubjectFilters = ErrorCode(rawValue: 10147)

    /// Consumer already exists
    static let consumerAlreadyExists = ErrorCode(rawValue: 10148)

    /// Consumer does not exist
    static let consumerDoesNotExist = ErrorCode(rawValue: 10149)

    /// Mirror with multiple subject transforms cannot also have a single subject filter
    static let mirrorMultipleFiltersNotAllowed = ErrorCode(rawValue: 10150)

    /// Mirror subject filter is invalid
    static let mirrorInvalidSubjectFilter = ErrorCode(rawValue: 10151)

    /// Mirror subject filters cannot overlap
    static let mirrorOverlappingSubjectFilters = ErrorCode(rawValue: 10152)

    /// Consumer inactive threshold exceeds system limit
    static let consumerInactiveThresholdExcess = ErrorCode(rawValue: 10153)

}

extension ErrorCode {
    // Encoding
    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    // Decoding
    init(from decoder: Decoder) throws {
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

func test() {
    //    JetStreamError(code: 400, errorCode: ErrorCode.accountResourcesExceeded, description: nil)
}
