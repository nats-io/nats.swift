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
import Nuid

extension JetStreamContext {

    /// Creates a publisher of an atomic batch: the server stores either all messages of the batch
    /// or none of them. Requires a stream with ``StreamConfig/allowAtomicPublish``.
    ///
    /// ```swift
    /// let batch = js.batchPublisher()
    /// try await batch.add("orders.created", message: order)
    /// try await batch.add("inventory.reserved", message: reservation)
    /// let ack = try await batch.commit("payments.charged", message: payment)
    /// ```
    ///
    /// - Parameter flowControl: when the server acknowledges messages before the commit.
    public func batchPublisher(flowControl: BatchFlowControl = BatchFlowControl()) -> BatchPublisher
    {
        BatchPublisher(ctx: self, flowControl: flowControl)
    }

    /// Publishes messages as an atomic batch, committed with the last message.
    /// See ``batchPublisher(flowControl:)``.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/BatchPublishError`` if the batch was rejected.
    @discardableResult
    public func publishBatch(
        _ messages: [BatchMessage], flowControl: BatchFlowControl = BatchFlowControl()
    ) async throws -> BatchAck {
        guard let last = messages.last else {
            throw JetStreamError.BatchPublishError.emptyBatch
        }
        let batch = batchPublisher(flowControl: flowControl)
        for message in messages.dropLast() {
            try await batch.add(
                message.subject, message: message.payload, headers: message.headers,
                options: message.options)
        }
        return try await batch.commit(
            last.subject, message: last.payload, headers: last.headers, options: last.options)
    }
}

/// A message of an atomic batch published with ``JetStreamContext/publishBatch(_:flowControl:)``.
public struct BatchMessage: Sendable {
    /// Subject the message is published on.
    public var subject: String
    /// Content of the message.
    public var payload: Data
    /// Headers of the message.
    public var headers: NatsHeaderMap?
    /// Expectations and TTL of the message.
    public var options: BatchMessageOptions?

    public init(
        subject: String, payload: Data, headers: NatsHeaderMap? = nil,
        options: BatchMessageOptions? = nil
    ) {
        self.subject = subject
        self.payload = payload
        self.headers = headers
        self.options = options
    }
}

/// Expectations and TTL of a message of an atomic batch.
public struct BatchMessageOptions: Sendable {
    /// Time to live of the message, requires ``StreamConfig/allowMsgTTL``.
    public var ttl: MessageTTL?
    /// Name of the stream the message is expected to be stored in.
    public var expectedStream: String?
    /// Expected sequence of the last message in the stream; only allowed on the first message.
    public var expectedLastSequence: UInt64?
    /// Expected sequence of the last message on the subject.
    public var expectedLastSubjectSequence: UInt64?
    /// Subject ``expectedLastSubjectSequence`` is checked against, instead of the published subject.
    public var expectedLastSubjectSequenceSubject: String?

    public init(
        ttl: MessageTTL? = nil, expectedStream: String? = nil,
        expectedLastSequence: UInt64? = nil, expectedLastSubjectSequence: UInt64? = nil,
        expectedLastSubjectSequenceSubject: String? = nil
    ) {
        self.ttl = ttl
        self.expectedStream = expectedStream
        self.expectedLastSequence = expectedLastSequence
        self.expectedLastSubjectSequence = expectedLastSubjectSequence
        self.expectedLastSubjectSequenceSubject = expectedLastSubjectSequenceSubject
    }

    internal var publishOptions: PublishOptions {
        PublishOptions(
            expectedStream: expectedStream, expectedLastSequence: expectedLastSequence,
            expectedLastSubjectSequence: expectedLastSubjectSequence,
            expectedLastSubjectSequenceSubject: expectedLastSubjectSequenceSubject, ttl: ttl)
    }
}

/// When the server acknowledges messages of an atomic batch before it is committed.
/// Acknowledgements slow publishing down to the pace of the server.
public struct BatchFlowControl: Sendable {
    /// Waits for the acknowledgement of the first message, which validates the batch early.
    public var ackFirst: Bool
    /// Waits for an acknowledgement every given number of messages.
    public var ackEvery: Int?
    /// Timeout of acknowledgements, the timeout of the context by default.
    public var ackTimeout: TimeInterval?

    public init(ackFirst: Bool = true, ackEvery: Int? = nil, ackTimeout: TimeInterval? = nil) {
        self.ackFirst = ackFirst
        self.ackEvery = ackEvery
        self.ackTimeout = ackTimeout
    }
}

/// The acknowledgement of a committed atomic batch.
public struct BatchAck: Codable, Sendable {
    /// The stream the batch was stored in.
    public let stream: String
    /// The sequence of the last message of the batch in the stream.
    public let seq: UInt64
    /// The JetStream domain of the stream.
    public let domain: String?
    /// The value of a counter, for streams with ``StreamConfig/allowMsgCounter``.
    public let value: String?
    /// The ID of the batch.
    public let batchId: String
    /// The number of messages in the batch.
    public let batchSize: UInt64

    enum CodingKeys: String, CodingKey {
        case stream, seq, domain
        case value = "val"
        case batchId = "batch"
        case batchSize = "count"
    }
}

/// Publishes the messages of an atomic batch, created with
/// ``JetStreamContext/batchPublisher(flowControl:)``.
///
/// Messages added with ``add(_:message:headers:options:)`` are stored by the server only when the
/// batch is committed with ``commit(_:message:headers:options:)`` or ``close()``. A batch which is
/// not committed is abandoned by the server.
public actor BatchPublisher {
    private let ctx: JetStreamContext
    private let flowControl: BatchFlowControl
    /// The ID of the batch.
    public nonisolated let batchId = nextNuid()
    private var sequence: UInt64 = 0
    private var firstSubject: String?
    /// Indicates whether the batch was committed, closed or discarded.
    public private(set) var isClosed = false

    init(ctx: JetStreamContext, flowControl: BatchFlowControl) {
        self.ctx = ctx
        self.flowControl = flowControl
    }

    /// The number of messages added to the batch.
    public var size: Int { Int(sequence) }

    /// Adds a message to the batch.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/BatchPublishError/closed`` if the batch was committed or discarded.
    /// > - ``JetStreamError/BatchPublishError/expectedLastSequenceNotFirst`` if an expected last
    /// > sequence is set on a message other than the first one.
    /// > - ``JetStreamError/BatchPublishError`` if the server rejected the batch.
    public func add(
        _ subject: String, message: Data, headers: NatsHeaderMap? = nil,
        options: BatchMessageOptions? = nil
    ) async throws {
        let headers = try prepare(headers: headers, options: options, commit: nil)
        firstSubject = firstSubject ?? subject

        let acknowledged =
            (flowControl.ackFirst && sequence == 1)
            || flowControl.ackEvery.map { $0 > 0 && sequence % UInt64($0) == 0 } ?? false
        guard acknowledged else {
            try await ctx.client.publish(message, subject: subject, headers: headers)
            return
        }
        let response = try await ctx.client.request(
            message, subject: subject, headers: headers,
            timeout: flowControl.ackTimeout ?? ctx.timeout)
        if let payload = response.payload, !payload.isEmpty,
            case .error(let apiResponse) = try JSONDecoder().decode(
                Response<EmptyAck>.self, from: payload)
        {
            throw JetStreamError.BatchPublishError(from: apiResponse.error)
        }
    }

    /// Adds the last message to the batch and commits it.
    ///
    /// - Returns ``BatchAck`` once the server stored all messages of the batch.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/BatchPublishError`` if the batch was rejected; no message is stored.
    public func commit(
        _ subject: String, message: Data, headers: NatsHeaderMap? = nil,
        options: BatchMessageOptions? = nil
    ) async throws -> BatchAck {
        let headers = try prepare(headers: headers, options: options, commit: "1")
        let response = try await ctx.client.request(
            message, subject: subject, headers: headers, timeout: ctx.timeout)
        isClosed = true
        return try parseAck(response)
    }

    /// Commits the batch without adding a message, using an end of batch marker which is not stored.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/BatchPublishError/emptyBatch`` if no message was added.
    public func close() async throws -> BatchAck {
        guard !isClosed else {
            throw JetStreamError.BatchPublishError.closed
        }
        guard let subject = firstSubject else {
            throw JetStreamError.BatchPublishError.emptyBatch
        }
        var headers = NatsHeaderMap()
        headers.insert(.natsBatchId, NatsHeaderValue(batchId))
        headers.insert(.natsBatchSequence, NatsHeaderValue(String(sequence + 1)))
        headers.insert(.natsBatchCommit, NatsHeaderValue("eob"))
        let response = try await ctx.client.request(
            Data(), subject: subject, headers: headers, timeout: ctx.timeout)
        isClosed = true
        return try parseAck(response)
    }

    /// Discards the batch. Messages which were already sent are abandoned by the server.
    public func discard() throws {
        guard !isClosed else {
            throw JetStreamError.BatchPublishError.closed
        }
        isClosed = true
    }

    private func prepare(
        headers: NatsHeaderMap?, options: BatchMessageOptions?, commit: String?
    ) throws -> NatsHeaderMap {
        guard !isClosed else {
            throw JetStreamError.BatchPublishError.closed
        }
        if sequence > 0, options?.expectedLastSequence != nil {
            throw JetStreamError.BatchPublishError.expectedLastSequenceNotFirst
        }
        var headers =
            options?.publishOptions.apply(to: headers ?? NatsHeaderMap()) ?? headers
            ?? NatsHeaderMap()
        sequence += 1
        headers.insert(.natsBatchId, NatsHeaderValue(batchId))
        headers.insert(.natsBatchSequence, NatsHeaderValue(String(sequence)))
        if let commit {
            headers.insert(.natsBatchCommit, NatsHeaderValue(commit))
        }
        return headers
    }

    private func parseAck(_ response: NatsMessage) throws -> BatchAck {
        if response.status == .noResponders {
            throw JetStreamError.PublishError.streamNotFound
        }
        guard let payload = response.payload, !payload.isEmpty else {
            throw JetStreamError.RequestError.emptyResponsePayload
        }
        switch try JSONDecoder().decode(Response<BatchAck>.self, from: payload) {
        case .success(let ack):
            guard ack.batchId == batchId, ack.batchSize == sequence else {
                throw JetStreamError.BatchPublishError.invalidAck
            }
            return ack
        case .error(let apiResponse):
            throw JetStreamError.BatchPublishError(from: apiResponse.error)
        }
    }

    private struct EmptyAck: Codable {}
}

extension JetStreamError {
    /// Errors of atomic batch publishing.
    public enum BatchPublishError: JetStreamErrorProtocol {
        /// The batch was already committed, closed or discarded.
        case closed
        /// The batch has no messages.
        case emptyBatch
        /// An expected last sequence can only be set on the first message of a batch.
        case expectedLastSequenceNotFirst
        /// The acknowledgement does not match the batch.
        case invalidAck
        /// The stream does not allow atomic batch publishing.
        case notEnabled(APIError)
        /// The batch was abandoned, e.g. because it was not committed in time.
        case incomplete(APIError)
        /// The batch exceeds the maximum batch size of the server.
        case exceedsLimit(APIError)
        /// Message IDs and expected last message IDs are not supported in batches.
        case unsupportedHeader(APIError)
        /// The server has too many batches in flight.
        case tooManyInflight(APIError)
        /// Any other error of the batch, e.g. a failed expectation.
        case rejected(APIError)

        /// A description of the error.
        public var description: String {
            switch self {
            case .closed:
                return "nats: batch publisher is closed"
            case .emptyBatch:
                return "nats: batch is empty"
            case .expectedLastSequenceNotFirst:
                return "nats: expected last sequence can only be set on the first message"
            case .invalidAck:
                return "nats: invalid batch acknowledgement"
            case .notEnabled(let err), .incomplete(let err), .exceedsLimit(let err),
                .unsupportedHeader(let err), .tooManyInflight(let err), .rejected(let err):
                return "nats: \(err.description)"
            }
        }

        internal init(from err: APIError) {
            switch err.errorCode.rawValue {
            case 10174:
                self = .notEnabled(err)
            case 10176:
                self = .incomplete(err)
            case 10199:
                self = .exceedsLimit(err)
            case 10177:
                self = .unsupportedHeader(err)
            case 10211:
                self = .tooManyInflight(err)
            default:
                self = .rejected(err)
            }
        }
    }
}
