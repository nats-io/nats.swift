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

public struct NatsMessage: Sendable {
    public let payload: Data?
    public let subject: String
    public let replySubject: String?
    public let length: Int
    public let headers: NatsHeaderMap?
    public let status: StatusCode?
    public let description: String?
}

public struct StatusCode: Equatable, Sendable {
    public static let idleHeartbeat = StatusCode(value: 100)
    public static let ok = StatusCode(value: 200)
    public static let badRequest = StatusCode(value: 400)
    public static let notFound = StatusCode(value: 404)
    public static let timeout = StatusCode(value: 408)
    public static let noResponders = StatusCode(value: 503)
    public static let requestTerminated = StatusCode(value: 409)

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
