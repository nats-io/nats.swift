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

/// Representation of NATS message in the context of JetStream.
/// It exposes message properties (payload, headers etc.) and various methods for acknowledging delivery.
/// It also allows for checking message metadata.
public struct JetStreamMessage {
    private let message: NatsMessage

    /// Message payload.
    public var payload: Data? { message.payload }

    /// Message headers.
    public var headers: NatsHeaderMap? { message.headers }

    /// The subject the message was published on.
    public var subject: String { message.subject }

    /// Reply subject used for acking a message.
    public var reply: String? { message.replySubject }

    internal let client: NatsClient

    private let emptyPayload = "".data(using: .utf8)!

    internal init(message: NatsMessage, client: NatsClient) {
        self.message = message
        self.client = client
    }

    /// Sends and acknowledhement of given kind to the server.
    ///
    /// - Parameter ackType: the type of acknowledgement being sent (defaults to ``AckKind/ack``. For details, see ``AckKind``.
    /// - Throws:
    ///  - ``JetStreamError/AckError`` if there was an error sending the acknowledgement.
    public func ack(ackType: AckKind = .ack) async throws {
        guard let subject = message.replySubject else {
            throw JetStreamError.AckError.noReplyInMessage
        }
        try await client.publish(ackType.payload(), subject: subject)
    }

    /// Parses the reply subject of the message, exposing JetStream message metadata.
    ///
    /// - Returns ``MessageMetadata``
    ///
    /// - Throws:
    ///  - ``JetStreamError/MessageMetadataError`` when there is an error parsing metadata.
    public func metadata() throws -> MessageMetadata {
        let prefix = "$JS.ACK."
        guard let subject = message.replySubject else {
            throw JetStreamError.MessageMetadataError.noReplyInMessage
        }
        if !subject.starts(with: prefix) {
            throw JetStreamError.MessageMetadataError.invalidPrefix
        }

        let startIndex = subject.index(subject.startIndex, offsetBy: prefix.count)
        let parts = subject[startIndex...].split(separator: ".")

        return try MessageMetadata(tokens: parts)
    }
}

/// Represents various types of JetStream message acknowledgement.
public enum AckKind {
    /// Normal acknowledgemnt
    case ack
    /// Negative ack, message will be redelivered (immediately or after given delay)
    case nak(delay: TimeInterval? = nil)
    /// Marks the message as being processed, resets ack wait timer delaying evential redelivery.
    case inProgress
    /// Marks the message as terminated, it will never be redelivered.
    case term(reason: String? = nil)

    func payload() -> Data {
        switch self {
        case .ack:
            return "+ACK".data(using: .utf8)!
        case .nak(let delay):
            if let delay {
                let delayStr = String(Int64(delay * 1_000_000_000))
                return "-NAK {\"delay\":\(delayStr)}".data(using: .utf8)!
            } else {
                return "-NAK".data(using: .utf8)!
            }
        case .inProgress:
            return "+WPI".data(using: .utf8)!
        case .term(let reason):
            if let reason {
                return "+TERM \(reason)".data(using: .utf8)!
            } else {
                return "+TERM".data(using: .utf8)!
            }
        }
    }
}

/// Metadata of a JetStream message.
public struct MessageMetadata {
    /// The domain this message was received on.
    public let domain: String?

    /// Optional account hash, present in servers post-ADR-15.
    public let accountHash: String?

    /// Name of the stream the message is delivered from.
    public let stream: String

    /// Name of the consumer the mesasge is delivered from.
    public let consumer: String

    /// Number of delivery attempts of this message.
    public let delivered: UInt64

    /// Stream sequence associated with this message.
    public let streamSequence: UInt64

    /// Consumer sequence associated with this message.
    public let consumerSequence: UInt64

    /// The time this message was received by the server from the publisher.
    public let timestamp: String

    /// The number of messages known by the server to be pending to this consumer.
    public let pending: UInt64

    private let v1TokenCount = 7
    private let v2TokenCount = 9

    init(tokens: [Substring]) throws {
        if tokens.count >= v2TokenCount {
            self.domain = String(tokens[0])
            self.accountHash = String(tokens[1])
            self.stream = String(tokens[2])
            self.consumer = String(tokens[3])
            guard let delivered = UInt64(tokens[4]) else {
                throw JetStreamError.MessageMetadataError.invalidTokenValue
            }
            self.delivered = delivered
            guard let sseq = UInt64(tokens[5]) else {
                throw JetStreamError.MessageMetadataError.invalidTokenValue
            }
            self.streamSequence = sseq
            guard let cseq = UInt64(tokens[6]) else {
                throw JetStreamError.MessageMetadataError.invalidTokenValue
            }
            self.consumerSequence = cseq
            self.timestamp = String(tokens[7])
            guard let pending = UInt64(tokens[8]) else {
                throw JetStreamError.MessageMetadataError.invalidTokenValue
            }
            self.pending = pending
        } else if tokens.count == v1TokenCount {
            self.domain = nil
            self.accountHash = nil
            self.stream = String(tokens[0])
            self.consumer = String(tokens[1])
            guard let delivered = UInt64(tokens[2]) else {
                throw JetStreamError.MessageMetadataError.invalidTokenValue
            }
            self.delivered = delivered
            guard let sseq = UInt64(tokens[3]) else {
                throw JetStreamError.MessageMetadataError.invalidTokenValue
            }
            self.streamSequence = sseq
            guard let cseq = UInt64(tokens[4]) else {
                throw JetStreamError.MessageMetadataError.invalidTokenValue
            }
            self.consumerSequence = cseq
            self.timestamp = String(tokens[5])
            guard let pending = UInt64(tokens[6]) else {
                throw JetStreamError.MessageMetadataError.invalidTokenValue
            }
            self.pending = pending
        } else {
            throw JetStreamError.MessageMetadataError.invalidTokenNum
        }
    }
}
