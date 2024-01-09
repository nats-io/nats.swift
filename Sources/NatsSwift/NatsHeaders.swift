import Foundation


// Represents NATS header field value in Swift.
struct HeaderValue: Equatable, CustomStringConvertible {
    private var inner: String

    init(_ value: String) {
        self.inner = value
    }

    var description: String {
        return inner
    }
}

// Errors for parsing HeaderValue and HeaderName
enum ParseHeaderValueError: Error, CustomStringConvertible {
    case invalidCharacter

    var description: String {
        switch self {
        case .invalidCharacter:
            return "Invalid character found in header value (value cannot contain '\\r' or '\\n')"
        }
    }
}

enum ParseHeaderNameError: Error, CustomStringConvertible {
    case invalidCharacter

    var description: String {
        switch self {
        case .invalidCharacter:
            return "Invalid header name (name cannot contain non-ascii alphanumeric characters other than '-')"
        }
    }
}

// Custom header representation in Swift
struct HeaderName: Equatable, Hashable, CustomStringConvertible {
    private var inner: String

    init(_ value: String) throws {
            if value.contains(where: { $0 == ":" || $0.asciiValue! < 33 || $0.asciiValue! > 126 }) {
                throw ParseHeaderNameError.invalidCharacter
        }
        self.inner = value
    }

    var description: String {
        return inner
    }

    // Example of standard headers
    static let natsStream = try! HeaderName("Nats-Stream")
    static let natsSequence = try! HeaderName("Nats-Sequence")
    // Add other standard headers as needed...
}

// Represents a NATS header map in Swift.
struct HeaderMap {
    private var inner: [HeaderName: [HeaderValue]]

    init() {
        self.inner = [:]
    }

    var isEmpty: Bool {
        return inner.isEmpty
    }

    mutating func insert(_ name: HeaderName, _ value: HeaderValue) {
        self.inner[name] = [value]
    }

    mutating func append(_ name: HeaderName, _ value: HeaderValue) {
        if inner[name] != nil {
            inner[name]?.append(value)
        } else {
            insert(name, value)
        }
    }

    func get(_ name: HeaderName) -> HeaderValue? {
        return inner[name]?.first
    }

    func getAll(_ name: HeaderName) -> [HeaderValue] {
        return inner[name] ?? []
    }

    //TODO(jrm): can we use unsafe methods here? Probably yes.
    func toBytes() -> [UInt8] {
        var bytes: [UInt8] = []
        bytes.append(contentsOf: "NATS/1.0\r\n".utf8)
        for (name, values) in inner {
            for value in values {
                bytes.append(contentsOf: name.description.utf8)
                bytes.append(contentsOf: ": ".utf8)
                bytes.append(contentsOf: value.description.utf8)
                bytes.append(contentsOf: "\r\n".utf8)
            }
        }
        bytes.append(contentsOf: "\r\n".utf8)
        return bytes
    }
}

extension HeaderMap {
    subscript(name: HeaderName) -> HeaderValue? {
        get {
            return get(name)
        }
        set {
            if let value = newValue {
                insert(name, value)
            } else {
                inner[name] = nil
            }
        }
    }
}

