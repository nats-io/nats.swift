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

/// `NanoTimeInterval` represents a time interval in nanoseconds, facilitating high precision time measurements.
public struct NanoTimeInterval: Codable, Equatable {
    /// The value of the time interval in seconds.
    var value: TimeInterval

    public init(_ timeInterval: TimeInterval) {
        self.value = timeInterval
    }

    /// Initializes a `NanoTimeInterval` from a decoder, assuming the encoded value is in nanoseconds.
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let nanoseconds = try container.decode(Double.self)
        self.value = nanoseconds / 1_000_000_000.0
    }

    /// Encodes this `NanoTimeInterval` into a given encoder, converting the time interval from seconds to nanoseconds.
    /// This method allows `NanoTimeInterval` to be serialized directly into a format that stores time in nanoseconds.
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        let nanoseconds = self.value * 1_000_000_000.0
        try container.encode(nanoseconds)
    }
}
