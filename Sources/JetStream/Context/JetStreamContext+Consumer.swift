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

extension JetStreamContext {

    /// Creates a consumer with the specified configuration.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer will be created on
    ///  - cfg: consumer config
    ///
    /// - Returns: ``Consumer`` object containing ``ConsumerConfig`` and exposing operations on the consumer
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError``: if there was am error creating the consumer. There are several errors which may occur, most common being:
    /// >   - ``JetStreamError/ConsumerError/invalidConfig(_:)``: if the provided configuration is not valid.
    /// >   - ``JetStreamError/ConsumerError/consumerNameExist(_:)``: if attempting to overwrite an existing consumer (with different configuration)
    /// >   - ``JetStreamError/ConsumerError/maximumConsumersLimit(_:)``: if a max number of consumers (specified on stream/account level) has been reached.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func createConsumer(stream: String, cfg: ConsumerConfig) async throws -> Consumer {
        try Stream.validate(name: stream)
        return try await upsertConsumer(stream: stream, cfg: cfg, action: "create")
    }

    /// Updates an existing consumer using specified config.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer will be updated on
    ///  - cfg: consumer config
    ///
    /// - Returns: ``Consumer`` object containing ``ConsumerConfig`` and exposing operations on the consumer
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError``: if there was am error updating the consumer. There are several errors which may occur, most common being:
    /// >   - ``JetStreamError/ConsumerError/invalidConfig(_:)``: if the provided configuration is not valid or atteppting to update an illegal property
    /// >   - ``JetStreamError/ConsumerError/consumerDoesNotExist(_:)``: if attempting to update a non-existing consumer
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func updateConsumer(stream: String, cfg: ConsumerConfig) async throws -> Consumer {
        try Stream.validate(name: stream)
        return try await upsertConsumer(stream: stream, cfg: cfg, action: "update")
    }

    /// Creates a consumer with the specified configuration or updates an existing consumer.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer will be created on
    ///  - cfg: consumer config
    ///
    /// - Returns: ``Consumer`` object containing ``ConsumerConfig`` and exposing operations on the consumer
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError``: if there was am error creating or updatig the consumer. There are several errors which may occur, most common being:
    /// >   - ``JetStreamError/ConsumerError/invalidConfig(_:)``: if the provided configuration is not valid  or atteppting to update an illegal property
    /// >   - ``JetStreamError/ConsumerError/maximumConsumersLimit(_:)``: if a max number of consumers (specified on stream/account level) has been reached.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func createOrUpdateConsumer(stream: String, cfg: ConsumerConfig) async throws -> Consumer
    {
        try Stream.validate(name: stream)
        return try await upsertConsumer(stream: stream, cfg: cfg)
    }

    /// Retrieves a consumer with given name from a stream.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer is retrieved from
    ///  - name: name of the stream
    ///
    /// - Returns a ``Stream`` object containing ``StreamInfo`` and exposing operations on the stream or nil if stream with given name does not exist.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError/consumerNotFound(_:)``: if the consumer with given name does not exist on a given stream.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func getConsumer(stream: String, name: String) async throws -> Consumer? {
        try Stream.validate(name: stream)
        try Consumer.validate(name: name)

        let subj = "CONSUMER.INFO.\(stream).\(name)"
        let info: Response<ConsumerInfo> = try await request(subj)
        switch info {
        case .success(let info):
            return Consumer(ctx: self, info: info)
        case .error(let apiResponse):
            if apiResponse.error.errorCode == .consumerNotFound {
                return nil
            }
            if let consumerError = JetStreamError.ConsumerError(from: apiResponse.error) {
                throw consumerError
            }
            throw apiResponse.error
        }
    }

    /// Deletes a consumer from a stream.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer will be created on
    ///  - name: consumer name
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError/consumerNotFound(_:)``: if the consumer with given name does not exist on a given stream.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func deleteConsumer(stream: String, name: String) async throws {
        try Stream.validate(name: stream)
        try Consumer.validate(name: name)

        let subject = "CONSUMER.DELETE.\(stream).\(name)"
        let resp: Response<ConsumerDeleteResponse> = try await request(subject)

        switch resp {
        case .success(_):
            return
        case .error(let apiResponse):
            if let streamErr = JetStreamError.ConsumerError(from: apiResponse.error) {
                throw streamErr
            }
            throw apiResponse.error
        }
    }

    internal func upsertConsumer(
        stream: String, cfg: ConsumerConfig, action: String? = nil
    ) async throws -> Consumer {
        let consumerName = cfg.name ?? cfg.durable ?? Consumer.generateConsumerName()

        try Consumer.validate(name: consumerName)

        let createReq = CreateConsumerRequest(stream: stream, config: cfg, action: action)
        let req = try JSONEncoder().encode(createReq)

        var subject: String
        if let filterSubject = cfg.filterSubject {
            subject = "CONSUMER.CREATE.\(stream).\(consumerName).\(filterSubject)"
        } else {
            subject = "CONSUMER.CREATE.\(stream).\(consumerName)"
        }

        let info: Response<ConsumerInfo> = try await request(subject, message: req)

        switch info {
        case .success(let info):
            return Consumer(ctx: self, info: info)
        case .error(let apiResponse):
            if let consumerError = JetStreamError.ConsumerError(from: apiResponse.error) {
                throw consumerError
            }
            throw apiResponse.error
        }
    }

    /// Used to list consumer names.
    ///
    /// - Parameters:
    ///  - stream: the name of the strem to list the consumers from.
    ///
    /// - Returns ``Consumers`` which implements AsyncSequence allowing iteration over stream infos.
    public func consumers(stream: String) async -> Consumers {
        return Consumers(ctx: self, stream: stream)
    }

    /// Used to list consumer names.
    ///
    /// - Parameters:
    ///  - stream: the name of the strem to list the consumers from.
    ///
    /// - Returns ``ConsumerNames`` which implements AsyncSequence allowing iteration over consumer names.
    public func consumerNames(stream: String) async -> ConsumerNames {
        return ConsumerNames(ctx: self, stream: stream)
    }
}

internal struct ConsumersPagedRequest: Codable {
    let offset: Int
}

/// Used to iterate over consumer names when using ``JetStreamContext/consumerNames(stream:)``
extension JetStreamContext {

    /// Pauses a consumer until the given date. Paused consumers do not deliver messages.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer is on
    ///  - name: consumer name
    ///  - until: date until which the consumer is paused
    ///
    /// - Returns ``ConsumerPauseResponse`` with the pause state of the consumer.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError/consumerNotFound(_:)``: if the consumer does not exist.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func pauseConsumer(
        stream: String, name: String, until: Date
    ) async throws -> ConsumerPauseResponse {
        try await pauseConsumer(
            stream: stream, name: name, until: ISO8601DateFormatter().string(from: until))
    }

    /// Resumes a paused consumer.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer is on
    ///  - name: consumer name
    ///
    /// - Returns ``ConsumerPauseResponse`` with the pause state of the consumer.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError/consumerNotFound(_:)``: if the consumer does not exist.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func resumeConsumer(stream: String, name: String) async throws -> ConsumerPauseResponse {
        try await pauseConsumer(stream: stream, name: name, until: nil)
    }

    /// Resets the delivery state of a consumer, so that messages are redelivered from the given
    /// stream sequence, or from the first unacknowledged message if no sequence is provided.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer is on
    ///  - name: consumer name
    ///  - sequence: stream sequence to reset the consumer to
    ///
    /// - Returns ``ConsumerResetResponse`` with the updated consumer info and the reset sequence.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError/invalidReset(_:)``: if the sequence is not valid for the consumer.
    /// > - ``JetStreamError/ConsumerError/consumerNotFound(_:)``: if the consumer does not exist.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func resetConsumer(
        stream: String, name: String, sequence: UInt64? = nil
    ) async throws -> ConsumerResetResponse {
        try Stream.validate(name: stream)
        try Consumer.validate(name: name)
        let req = try JSONEncoder().encode(ConsumerResetRequest(seq: sequence))
        let resp: Response<ConsumerResetResponse> = try await request(
            "CONSUMER.RESET.\(stream).\(name)", message: req)
        return try Self.unwrapConsumerResponse(resp)
    }

    /// Unpins the client currently pinned to a priority group of a consumer
    /// using ``PriorityPolicy/pinned``.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer is on
    ///  - name: consumer name
    ///  - group: name of the priority group
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError/consumerNotFound(_:)``: if the consumer does not exist.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func unpinConsumer(stream: String, name: String, group: String) async throws {
        try Stream.validate(name: stream)
        try Consumer.validate(name: name)
        let req = try JSONEncoder().encode(["group": group])
        let resp: Response<EmptyResponse> = try await request(
            "CONSUMER.UNPIN.\(stream).\(name)", message: req)
        _ = try Self.unwrapConsumerResponse(resp)
    }

    private func pauseConsumer(
        stream: String, name: String, until: String?
    ) async throws -> ConsumerPauseResponse {
        try Stream.validate(name: stream)
        try Consumer.validate(name: name)
        let req = try JSONEncoder().encode(ConsumerPauseRequest(pauseUntil: until))
        let resp: Response<ConsumerPauseResponse> = try await request(
            "CONSUMER.PAUSE.\(stream).\(name)", message: req)
        return try Self.unwrapConsumerResponse(resp)
    }

    private static func unwrapConsumerResponse<T>(_ resp: Response<T>) throws -> T {
        switch resp {
        case .success(let value):
            return value
        case .error(let apiResponse):
            if let consumerError = JetStreamError.ConsumerError(from: apiResponse.error) {
                throw consumerError
            }
            throw apiResponse.error
        }
    }
}

/// Pause state of a consumer returned by ``JetStreamContext/pauseConsumer(stream:name:until:)``.
public struct ConsumerPauseResponse: Codable, Sendable {
    /// Indicates whether the consumer is paused.
    public let paused: Bool

    /// Date until which the consumer is paused.
    public let pauseUntil: String?

    /// Time left until the consumer unpauses.
    public let pauseRemaining: NanoTimeInterval?

    enum CodingKeys: String, CodingKey {
        case paused
        case pauseUntil = "pause_until"
        case pauseRemaining = "pause_remaining"
    }
}

/// Result of ``JetStreamContext/resetConsumer(stream:name:sequence:)``.
public struct ConsumerResetResponse: Codable, Sendable {
    /// The updated consumer info.
    public let info: ConsumerInfo

    /// The stream sequence the consumer was reset to.
    public let resetSequence: UInt64

    enum CodingKeys: String, CodingKey {
        case resetSequence = "reset_seq"
    }

    public init(from decoder: Decoder) throws {
        // consumer info fields are inlined in the response
        info = try ConsumerInfo(from: decoder)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        resetSequence = try container.decode(UInt64.self, forKey: .resetSequence)
    }

    public func encode(to encoder: Encoder) throws {
        try info.encode(to: encoder)
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(resetSequence, forKey: .resetSequence)
    }
}

internal struct ConsumerPauseRequest: Codable {
    let pauseUntil: String?

    enum CodingKeys: String, CodingKey {
        case pauseUntil = "pause_until"
    }
}

internal struct ConsumerResetRequest: Codable {
    let seq: UInt64?
}

internal struct EmptyResponse: Codable {}

/// Used to iterate over consumer names when listing them with ``JetStreamContext/consumerNames(stream:)``.
public struct ConsumerNames: AsyncSequence, Sendable {
    public typealias Element = String
    public typealias AsyncIterator = ConsumerNamesIterator

    private let ctx: JetStreamContext
    private let stream: String
    private var buffer: [String]
    private var offset: Int
    private var total: Int?

    private struct ConsumerNamesPage: Codable {
        let total: Int
        let consumers: [String]?
    }

    init(ctx: JetStreamContext, stream: String) {
        self.stream = stream
        self.ctx = ctx
        self.buffer = []
        self.offset = 0
    }

    public func makeAsyncIterator() -> ConsumerNamesIterator {
        return ConsumerNamesIterator(seq: self)
    }

    public mutating func next() async throws -> Element? {
        if let consumer = buffer.first {
            buffer.removeFirst()
            return consumer
        }

        if let total = self.total, self.offset >= total {
            return nil
        }

        // poll consumers
        let request = ConsumersPagedRequest(offset: offset)

        let res: Response<ConsumerNamesPage> = try await ctx.request(
            "CONSUMER.NAMES.\(self.stream)", message: JSONEncoder().encode(request))
        switch res {
        case .success(let names):
            guard let consumers = names.consumers else {
                return nil
            }
            self.offset += consumers.count
            self.total = names.total
            buffer.append(contentsOf: consumers)
            return try await self.next()
        case .error(let err):
            if let streamErr = JetStreamError.StreamError(from: err.error) {
                throw streamErr
            }
            throw err.error
        }

    }

    /// Iterator of consumer names.
    public struct ConsumerNamesIterator: AsyncIteratorProtocol {
        var seq: ConsumerNames

        public mutating func next() async throws -> Element? {
            try await seq.next()
        }
    }
}

/// Used to iterate over consumers when listing consumer infos using ``JetStreamContext/consumers(stream:)``
public struct Consumers: AsyncSequence, Sendable {
    public typealias Element = ConsumerInfo
    public typealias AsyncIterator = ConsumersIterator

    private let ctx: JetStreamContext
    private let stream: String
    private var buffer: [ConsumerInfo]
    private var offset: Int
    private var total: Int?

    private struct ConsumersPage: Codable {
        let total: Int
        let consumers: [ConsumerInfo]?
    }

    init(ctx: JetStreamContext, stream: String) {
        self.stream = stream
        self.ctx = ctx
        self.buffer = []
        self.offset = 0
    }

    public func makeAsyncIterator() -> ConsumersIterator {
        return ConsumersIterator(seq: self)
    }

    public mutating func next() async throws -> Element? {
        if let consumer = buffer.first {
            buffer.removeFirst()
            return consumer
        }

        if let total = self.total, self.offset >= total {
            return nil
        }

        // poll consumers
        let request = ConsumersPagedRequest(offset: offset)

        let res: Response<ConsumersPage> = try await ctx.request(
            "CONSUMER.LIST.\(self.stream)", message: JSONEncoder().encode(request))
        switch res {
        case .success(let infos):
            guard let consumers = infos.consumers else {
                return nil
            }
            self.offset += consumers.count
            self.total = infos.total
            buffer.append(contentsOf: consumers)
            return try await self.next()
        case .error(let err):
            if let streamErr = JetStreamError.StreamError(from: err.error) {
                throw streamErr
            }
            throw err.error
        }

    }

    /// Iterator of consumer infos.
    public struct ConsumersIterator: AsyncIteratorProtocol {
        var seq: Consumers

        public mutating func next() async throws -> Element? {
            try await seq.next()
        }
    }
}
