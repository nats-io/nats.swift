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
public final class JetStreamContext: Sendable {
    internal let client: NatsClient
    package let prefix: String
    private let requestTimeout: Locked<TimeInterval>
    internal let publisher: AsyncPublisher

    /// Timeout of JetStream API requests.
    internal var timeout: TimeInterval { requestTimeout.value }

    /// Creates a JetStreamContext from `NatsClient` with optional custom prefix and timeout.
    ///
    /// - Parameters:
    ///  - client: NATS client connection.
    ///  - prefix: Used to comfigure a prefix for JetStream API requests.
    ///  - timeout: Used to configure a timeout for JetStream API operations.
    public init(client: NatsClient, prefix: String = "$JS.API", timeout: TimeInterval = 5.0) {
        self.client = client
        self.prefix = prefix
        self.requestTimeout = Locked(timeout)
        self.publisher = AsyncPublisher(client: client)
    }

    /// Creates a JetStreamContext from `NatsClient` with custom domain and timeout.
    ///
    /// - Parameters:
    ///  - client: NATS client connection.
    ///  - domain: Used to comfigure a domain for JetStream API requests.
    ///  - timeout: Used to configure a timeout for JetStream API operations.
    public init(client: NatsClient, domain: String, timeout: TimeInterval = 5.0) {
        self.client = client
        self.prefix = "$JS.\(domain).API"
        self.requestTimeout = Locked(timeout)
        self.publisher = AsyncPublisher(client: client)
    }

    /// Creates a JetStreamContext from `NatsClient`
    ///
    /// - Parameters:
    ///  - client: NATS client connection.
    public convenience init(client: NatsClient) {
        self.init(client: client, prefix: "$JS.API", timeout: 5.0)
    }

    /// Sets a custom timeout for JetStream API requests.
    public func setTimeout(_ timeout: TimeInterval) {
        requestTimeout.value = timeout
    }

    /// Sets the maximum number of published messages awaiting acknowledgements (4000 by default)
    /// and how long a publish waits for room before failing with
    /// ``JetStreamError/PublishError/tooManyStalledMessages`` (200 milliseconds by default).
    public func setPublishAsyncMaxPending(_ maxPending: Int, stallWait: TimeInterval = 0.2) async {
        await publisher.configure(maxPending: maxPending, stallWait: stallWait)
    }

    /// Number of published messages awaiting acknowledgements.
    public func publishAsyncPending() async -> Int {
        await publisher.pendingCount
    }

    /// Waits until all published messages were acknowledged by the server (or failed).
    public func publishAsyncComplete() async {
        await publisher.waitForCompletion()
    }
}

extension JetStreamContext {

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

/// An error response of the JetStream API.
public struct JetStreamAPIResponse: Codable, Sendable {
    /// Type of the response.
    public let type: String
    /// The error.
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
