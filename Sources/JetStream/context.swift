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
class JetStreamContext {
    private var client: NatsClient
    private var prefix: String = "$JS.API"
    private var timeout: TimeInterval = 5.0

    public init(client: NatsClient, prefix: String = "$JS.API", timeout: TimeInterval = 5.0) {
        self.client = client
        self.prefix = prefix
        self.timeout = timeout
    }

    public init(client: NatsClient, domain: String, timeout: TimeInterval = 5.0) {
        self.client = client
        self.prefix = "$JS.\(domain).API"
        self.timeout = timeout
    }

    public init(client: NatsClient) {
        self.client = client
    }

    public func setTimeout(_ timeout: TimeInterval) {
        self.timeout = timeout
    }
}

extension JetStreamContext {
    // fix the error type. Add AckError
    public func publish(_ subject: String, message: Data) async throws -> AckFuture {

        let inbox = nextNuid()
        let sub = try await self.client.subscribe(subject: inbox)
        try await self.client.publish(message, subject: subject, reply: inbox)
        return AckFuture(sub: sub, timeout: self.timeout)
    }

    internal func request<T: Codable>(_ subject: String, message: Data) async throws -> Response<T> {
        let response = try await self.client.request(message, subject: "\(self.prefix).\(subject)", timeout: self.timeout)

        let decoder = JSONDecoder()
       // maybe empty is ok if the response type is nil and we can skip this check?
        guard let payload = response.payload else {
            throw JetStreamRequestError("empty response payload")
        }

        return try decoder.decode(Response<T>.self, from: payload)
    }
}

struct AckFuture {
    let sub: NatsSubscription
    let timeout: TimeInterval
    func wait() async throws -> Ack {
        let response = try await withThrowingTaskGroup(
            of: NatsMessage?.self,
            body: { group in
                group.addTask {
                    return await sub.makeAsyncIterator().next()
                }

                // task for the timeout
                group.addTask {
                    try await Task.sleep(nanoseconds: UInt64(self.timeout * 1_000_000_000))
                    return nil
                }

                for try await result in group {
                    // if the result is not empty, return it (or throw status error)
                    if let msg = result {
                        if let status = msg.status, status == StatusCode.noResponders {
                            throw NatsRequestError.noResponders
                        }
                        return msg
                    } else {
                        // if result is empty, time out
                        throw NatsRequestError.timeout
                    }
                }

                // this should not be reachable
                throw JetStreamPublishError("internal error; error waiting for response")
            })
        if response.status == StatusCode.noResponders {
            throw JetStreamPublishError("Stream not found")
        }

        let decoder = JSONDecoder()
        guard let payload = response.payload else {
            throw JetStreamPublishError("empty ack payload")
        }
        let ack: Response<Ack>
        do {
            ack = try decoder.decode(Response<Ack>.self, from: payload)
        } catch {
            throw JetStreamPublishError("failed to send request: \(error)")
        }
        switch ack {
        case .success(let ack):
            return ack
        case .error(let err):
            throw JetStreamPublishError("ack failed: \(err)")
        }

    }
}
struct JetStreamPublishError: NatsError {
    var description: String
    init(_ description: String) {
        self.description = description
    }
}

struct JetStreamRequestError: NatsError {
    var description: String
    init(_ description: String) {
        self.description = description
    }
}

struct Ack: Codable {
    var stream: String
    var seq: UInt64
    var domain: String?
    var duplicate: Bool

    // Custom CodingKeys to map JSON keys to Swift property names
    enum CodingKeys: String, CodingKey {
        case stream
        case seq
        case domain
        case duplicate
    }

    // Custom initializer from Decoder
    init(from decoder: Decoder) throws {
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
