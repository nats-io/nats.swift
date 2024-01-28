import Foundation

extension Data {
    /// Swift does not provide a way to encode data to base64 without padding in URL safe way.
    func base64EncodedURLSafeNotPadded() -> String {
        return self.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .trimmingCharacters(in: CharacterSet(charactersIn: "="))
    }
}
