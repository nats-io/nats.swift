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

extension String {
    private static let charactersToTrim: CharacterSet = .whitespacesAndNewlines.union(
        CharacterSet(charactersIn: "'"))

    static func hash() -> String {
        let uuid = String.uuid()
        return uuid[0...7]
    }

    func trimWhitespacesAndApostrophes() -> String {
        return self.trimmingCharacters(in: String.charactersToTrim)
    }

    static func uuid() -> String {
        return UUID().uuidString.trimmingCharacters(in: .punctuationCharacters)
    }

    subscript(bounds: CountableClosedRange<Int>) -> String {
        let start = index(startIndex, offsetBy: bounds.lowerBound)
        let end = index(startIndex, offsetBy: bounds.upperBound)
        return String(self[start...end])
    }
}
