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

extension Client {
    @discardableResult
    public func on(_ events: [NatsEventKind], _ handler: @escaping (NatsEvent) -> Void) -> String {
        guard let connectionHandler = self.connectionHandler else {
            return ""
        }
        return connectionHandler.addListeners(for: events, using: handler)
    }

    @discardableResult
    public func on(_ event: NatsEventKind, _ handler: @escaping (NatsEvent) -> Void) -> String {
        guard let connectionHandler = self.connectionHandler else {
            return ""
        }
        return connectionHandler.addListeners(for: [event], using: handler)
    }

    func off(_ id: String) {
        guard let connectionHandler = self.connectionHandler else {
            return
        }
        connectionHandler.removeListener(id)
    }
}
