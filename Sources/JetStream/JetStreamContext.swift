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

import Combine
import Foundation
import Nats
import Nuid

/// A context which can perform jetstream scoped requests.
public class JetStreamContext {
    internal var client: NatsClient
    private var prefix: String = "$JS.API"
    private var timeout: TimeInterval = 5.0

    /// Creates a JetStreamContext from ``NatsClient`` with optional custom prefix and timeout.
    ///
    /// - Parameters:
    ///  - client: NATS client connection.
    ///  - prefix: Used to comfigure a prefix for JetStream API requests.
    ///  - timeout: Used to configure a timeout for JetStream API operations.
    public init(client: NatsClient, prefix: String = "$JS.API", timeout: TimeInterval = 5.0) {
        self.client = client
        self.prefix = prefix
        self.timeout = timeout
    }

    /// Creates a JetStreamContext from ``NatsClient`` with custom domain and timeout.
    ///
    /// - Parameters:
    ///  - client: NATS client connection.
    ///  - domain: Used to comfigure a domain for JetStream API requests.
    ///  - timeout: Used to configure a timeout for JetStream API operations.
    public init(client: NatsClient, domain: String, timeout: TimeInterval = 5.0) {
        self.client = client
        self.prefix = "$JS.\(domain).API"
        self.timeout = timeout
    }

    /// Creates a JetStreamContext from ``NatsClient``
    ///
    /// - Parameters:
    ///  - client: NATS client connection.
    public init(client: NatsClient) {
        self.client = client
    }

    /// Sets a custom timeout for JetStream API requests.
    public func setTimeout(_ timeout: TimeInterval) {
        self.timeout = timeout
    }
}

extension JetStreamContext {

    /// Publishes a message on a stream subjec without waiting  for acknowledgment from the server that the message has been successfully delivered.
    ///
    /// - Parameters:
    ///   - subject: Subject on which the message will be published.
    ///   - message: NATS message payload.
    ///   - headers:Optional set of message headers.
    ///   - options: Optional message deduplication and optimistic concurrency expectations.
    ///
    /// - Returns: ``AckFuture`` allowing to await for the ack from the server.
    public func publish(
        _ subject: String, message: Data, headers: NatsHeaderMap? = nil,
        options: PublishOptions? = nil
    ) async throws -> AckFuture {
        var headers = headers
        if let options {
            headers = options.apply(to: headers ?? NatsHeaderMap())
        }
        let inbox = client.newInbox()
        let sub = try await self.client.subscribe(subject: inbox)
        try await self.client.publish(message, subject: subject, reply: inbox, headers: headers)
        return AckFuture(sub: sub, timeout: self.timeout)
    }

    /// Retrieves information about the JetStream usage of the current account.
    ///
    /// - Returns: ``AccountInfo`` with memory, storage, streams and consumers usage.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/RequestError`` if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError`` if there was an API error returned from JetStream.
    public func accountInfo() async throws -> AccountInfo {
        let info: Response<AccountInfo> = try await request("INFO")
        switch info {
        case .success(let info):
            return info
        case .error(let apiResponse):
            throw apiResponse.error
        }
    }

    internal func request<T: Codable>(
        _ subject: String, message: Data? = nil
    ) async throws -> Response<T> {
        let data = message ?? Data()
        do {
            let response = try await self.client.request(
                data, subject: apiSubject(subject), timeout: self.timeout)
            let decoder = JSONDecoder()
            guard let payload = response.payload else {
                throw JetStreamError.RequestError.emptyResponsePayload
            }
            return try decoder.decode(Response<T>.self, from: payload)
        } catch let err as NatsError.RequestError {
            switch err {
            case .noResponders:
                throw JetStreamError.RequestError.noResponders
            case .timeout:
                throw JetStreamError.RequestError.timeout
            case .permissionDenied:
                throw JetStreamError.RequestError.permissionDenied(subject)
            }
        }
    }

    internal func request(_ subject: String, message: Data? = nil) async throws -> NatsMessage {
        let data = message ?? Data()
        do {
            return try await self.client.request(
                data, subject: apiSubject(subject), timeout: self.timeout)
        } catch let err as NatsError.RequestError {
            switch err {
            case .noResponders:
                throw JetStreamError.RequestError.noResponders
            case .timeout:
                throw JetStreamError.RequestError.timeout
            case .permissionDenied:
                throw JetStreamError.RequestError.permissionDenied(subject)
            }
        }
    }

    internal func apiSubject(_ subject: String) -> String {
        return "\(self.prefix).\(subject)"
    }
}

/// Options used to deduplicate a published message and to set optimistic concurrency expectations.
/// If an expectation is not met, ``AckFuture/wait()`` throws ``JetStreamError/PublishError``.
public struct PublishOptions {
    /// Unique ID used by the stream to discard duplicates within its duplicate window.
    public var messageId: String?
    /// Name of the stream the message is expected to be stored in.
    public var expectedStream: String?
    /// Expected sequence of the last message in the stream.
    public var expectedLastSequence: UInt64?
    /// Expected sequence of the last message on the published subject.
    public var expectedLastSubjectSequence: UInt64?
    /// Expected message ID of the last message in the stream.
    public var expectedLastMessageId: String?

    public init(
        messageId: String? = nil, expectedStream: String? = nil,
        expectedLastSequence: UInt64? = nil, expectedLastSubjectSequence: UInt64? = nil,
        expectedLastMessageId: String? = nil
    ) {
        self.messageId = messageId
        self.expectedStream = expectedStream
        self.expectedLastSequence = expectedLastSequence
        self.expectedLastSubjectSequence = expectedLastSubjectSequence
        self.expectedLastMessageId = expectedLastMessageId
    }

    internal func apply(to headers: NatsHeaderMap) -> NatsHeaderMap {
        var headers = headers
        if let messageId {
            headers.insert(.natsMsgId, NatsHeaderValue(messageId))
        }
        if let expectedStream {
            headers.insert(.natsExpectedStream, NatsHeaderValue(expectedStream))
        }
        if let expectedLastSequence {
            headers.insert(.natsExpectedLastSequence, NatsHeaderValue(String(expectedLastSequence)))
        }
        if let expectedLastSubjectSequence {
            headers.insert(
                .natsExpectedLastSubjectSequence,
                NatsHeaderValue(String(expectedLastSubjectSequence)))
        }
        if let expectedLastMessageId {
            headers.insert(.natsExpectedLastMsgId, NatsHeaderValue(expectedLastMessageId))
        }
        return headers
    }
}

public struct JetStreamAPIResponse: Codable {
    public let type: String
    public let error: JetStreamError.APIError

    enum CodingKeys: String, CodingKey {
        case type
        case error
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        // publish acks do not contain `type`
        type = try container.decodeIfPresent(String.self, forKey: .type) ?? ""
        error = try container.decode(JetStreamError.APIError.self, forKey: .error)
    }
}

/// Used to await for response from ``JetStreamContext/publish(_:message:headers:)``
public struct AckFuture {
    let sub: NatsSubscription
    let timeout: TimeInterval

    /// Waits for an ACK from JetStream server.
    ///
    /// - Returns: Acknowledgement object returned by the server.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/RequestError`` if the request timed out (client did not receive the ack in time) or
    public func wait() async throws -> Ack {
        let response = try await withThrowingTaskGroup(
            of: NatsMessage?.self,
            body: { group in
                group.addTask {
                    return try await sub.makeAsyncIterator().next()
                }

                // task for the timeout
                group.addTask {
                    try await Task.sleep(nanoseconds: UInt64(self.timeout * 1_000_000_000))
                    return nil
                }

                for try await result in group {
                    // if the result is not empty, return it (or throw status error)
                    if let msg = result {
                        group.cancelAll()
                        return msg
                    } else {
                        group.cancelAll()
                        try await sub.unsubscribe()
                        // if result is empty, time out
                        throw JetStreamError.RequestError.timeout
                    }
                }

                // this should not be reachable
                throw NatsError.ClientError.internalError("error waiting for response")
            })
        if response.status == StatusCode.noResponders {
            throw JetStreamError.PublishError.streamNotFound
        }

        let decoder = JSONDecoder()
        guard let payload = response.payload else {
            throw JetStreamError.RequestError.emptyResponsePayload
        }

        let ack = try decoder.decode(Response<Ack>.self, from: payload)
        switch ack {
        case .success(let ack):
            return ack
        case .error(let err):
            if let publishErr = JetStreamError.PublishError(from: err.error) {
                throw publishErr
            } else {
                throw err.error
            }
        }

    }
}

public struct Ack: Codable {
    public let stream: String
    public let seq: UInt64
    public let domain: String?
    public let duplicate: Bool

    // Custom CodingKeys to map JSON keys to Swift property names
    enum CodingKeys: String, CodingKey {
        case stream
        case seq
        case domain
        case duplicate
    }

    // Custom initializer from Decoder
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        // Decode `stream` and `seq` as they are required
        stream = try container.decode(String.self, forKey: .stream)
        seq = try container.decode(UInt64.self, forKey: .seq)

        // Decode `domain` as optional since it may not be present
        domain = try container.decodeIfPresent(String.self, forKey: .domain)

        // Decode `duplicate` and provide a default value of `false` if not present
        duplicate = try container.decodeIfPresent(Bool.self, forKey: .duplicate) ?? false
    }
}

/// contains info about the `JetStream` usage from the current account.
public struct AccountInfo: Codable {
    public let memory: Int64
    public let storage: Int64
    public let streams: Int64
    public let consumers: Int64
}
