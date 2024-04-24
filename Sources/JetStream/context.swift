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

/// A context which can perform jetstream scoped requests.
class Context {
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

extension Context {
    public func publish(_ subject: String, message: Data) async throws {
        let subject = "\(self.prefix).PUB \(subject)"
        // add ack
        try await self.client.publish(message, subject: subject)
    }
}
