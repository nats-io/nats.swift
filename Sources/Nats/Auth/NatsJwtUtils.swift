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

class JwtUtils {
    // This regular expression is equivalent to the one used in Rust.
    static let userConfigRE: NSRegularExpression = {
        do {
            return try NSRegularExpression(
                pattern:
                    "\\s*(?:(?:-{3,}.*-{3,}\\r?\\n)([\\w\\-.=]+)(?:\\r?\\n-{3,}.*-{3,}\\r?\\n))",
                options: [])
        } catch {
            fatalError("Invalid regular expression: \(error)")
        }
    }()

    /// Parses a credentials file and returns its user JWT.
    static func parseDecoratedJWT(contents: String) -> String? {
        let matches = userConfigRE.matches(
            in: contents, options: [], range: NSRange(contents.startIndex..., in: contents))
        if let match = matches.first, let range = Range(match.range(at: 1), in: contents) {
            return String(contents[range])
        }
        return nil
    }
    /// Parses a credentials file and returns its user JWT.
    static func parseDecoratedJWT(contents: Data) -> Data? {
        guard let contentsString = String(data: contents, encoding: .utf8) else {
            return nil
        }
        if let match = parseDecoratedJWT(contents: contentsString) {
            return match.data(using: .utf8)
        }
        return nil
    }

    /// Parses a credentials file and returns its nkey.
    static func parseDecoratedNKey(contents: String) -> String? {
        let matches = userConfigRE.matches(
            in: contents, options: [], range: NSRange(contents.startIndex..., in: contents))
        if matches.count > 1, let range = Range(matches[1].range(at: 1), in: contents) {
            return String(contents[range])
        }
        return nil
    }

    /// Parses a credentials file and returns its nkey.
    static func parseDecoratedNKey(contents: Data) -> Data? {
        guard let contentsString = String(data: contents, encoding: .utf8) else {
            return nil
        }
        if let match = parseDecoratedNKey(contents: contentsString) {
            return match.data(using: .utf8)
        }
        return nil
    }
}
