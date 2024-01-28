import Foundation

class JwtUtils {
    // This regular expression is equivalent to the one used in Rust.
    static let userConfigRE: NSRegularExpression = {
        do {
            return try NSRegularExpression(pattern: "\\s*(?:(?:-{3,}.*-{3,}\\r?\\n)([\\w\\-.=]+)(?:\\r?\\n-{3,}.*-{3,}\\r?\\n))", options: [])
        } catch {
            fatalError("Invalid regular expression: \(error)")
        }
    }()

    /// Parses a credentials file and returns its user JWT.
    static func parseDecoratedJWT(contents: String) -> String? {
        let matches = userConfigRE.matches(in: contents, options: [], range: NSRange(contents.startIndex..., in: contents))
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
        let matches = userConfigRE.matches(in: contents, options: [], range: NSRange(contents.startIndex..., in: contents))
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
