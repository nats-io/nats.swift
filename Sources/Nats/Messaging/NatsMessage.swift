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

/// A message received from the server.
public struct NatsMessage: Sendable {
    /// The content of the message, nil if it is empty.
    public let payload: Data?
    /// The subject the message was published on.
    public let subject: String
    /// The subject to send a reply to, set for requests.
    public let replySubject: String?
    /// The size of the payload in bytes.
    public let length: Int
    /// The headers of the message.
    public let headers: NatsHeaderMap?
    /// The status of the message, set for status messages sent by the server (e.g. no responders).
    public let status: StatusCode?
    /// The description of the status, set for status messages.
    public let description: String?
}

/// Status of a message sent by the server, similar to HTTP status codes.
public struct StatusCode: Equatable, Sendable {
    /// A heartbeat or flow control message.
    public static let idleHeartbeat = StatusCode(value: 100)
    /// A regular message.
    public static let ok = StatusCode(value: 200)
    /// The request was invalid.
    public static let badRequest = StatusCode(value: 400)
    /// No messages were found.
    public static let notFound = StatusCode(value: 404)
    /// The request timed out.
    public static let timeout = StatusCode(value: 408)
    /// No subscriber is listening on the request subject.
    public static let noResponders = StatusCode(value: 503)
    /// The request was terminated, see the description of the message.
    public static let requestTerminated = StatusCode(value: 409)
    /// The client is not pinned to the priority group of a consumer.
    public static let pinIdMismatch = StatusCode(value: 423)

    let value: UInt16

    // non-optional initializer for static status codes
    private init(value: UInt16) {
        self.value = value
    }

    init?(_ value: UInt16) {
        if !(100..<1000 ~= value) {
            return nil
        }

        self.value = value
    }

    init?(_ value: any StringProtocol) {
        guard let status = UInt16(value) else {
            return nil
        }
        if !(100..<1000 ~= status) {
            return nil
        }

        self.value = status
    }
}
