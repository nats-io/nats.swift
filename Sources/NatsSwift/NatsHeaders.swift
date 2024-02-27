import Foundation

// Represents NATS header field value in Swift.
public struct HeaderValue: Equatable, CustomStringConvertible {
    private var inner: String

    public init(_ value: String) {
        self.inner = value
    }

    public var description: String {
        return inner
    }
}

public enum ParseHeaderNameError: NatsError {
    case invalidCharacter

    public var description: String {
        switch self {
        case .invalidCharacter:
            return
                "Invalid header name (name cannot contain non-ascii alphanumeric characters other than '-')"
        }
    }
}

// Custom header representation in Swift
public struct HeaderName: Equatable, Hashable, CustomStringConvertible {
    private var inner: String

    public init(_ value: String) throws {
        if value.contains(where: { $0 == ":" || $0.asciiValue! < 33 || $0.asciiValue! > 126 }) {
            throw ParseHeaderNameError.invalidCharacter
        }
        self.inner = value
    }

    public var description: String {
        return inner
    }

    // Example of standard headers
    public static let natsStream = try! HeaderName("Nats-Stream")
    public static let natsSequence = try! HeaderName("Nats-Sequence")
    // Add other standard headers as needed...
}

// Represents a NATS header map in Swift.
public struct HeaderMap: Equatable, Sequence {
    private var inner: [HeaderName: [HeaderValue]]

    public init() {
        self.inner = [:]
    }

    var isEmpty: Bool {
        return inner.isEmpty
    }

    public mutating func insert(_ name: HeaderName, _ value: HeaderValue) {
        self.inner[name] = [value]
    }

    public mutating func append(_ name: HeaderName, _ value: HeaderValue) {
        if inner[name] != nil {
            inner[name]?.append(value)
        } else {
            insert(name, value)
        }
    }

    public func get(_ name: HeaderName) -> HeaderValue? {
        return inner[name]?.first
    }

    public func getAll(_ name: HeaderName) -> [HeaderValue] {
        return inner[name] ?? []
    }

    public func makeIterator() -> HeaderMapIterator {
        return HeaderMapIterator(dictionary: inner)
    }
    
    //TODO(jrm): can we use unsafe methods here? Probably yes.
    func toBytes() -> [UInt8] {
        var bytes: [UInt8] = []
        bytes.append(contentsOf: "NATS/1.0\r\n".utf8)
        for (name, values) in inner {
            for value in values {
                bytes.append(contentsOf: name.description.utf8)
                bytes.append(contentsOf: ":".utf8)
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
    
    public subscript(name: String) -> String? {
        get {
            return get(safeEncoded(name: name))?.description
        }
        set {
            if let value = newValue {
                insert(safeEncoded(name: name), HeaderValue(value))
            } else {
                inner[safeEncoded(name: name)] = nil
            }
        }
    }
    
    func safeEncoded(name: String) -> HeaderName {
        let safeName = name.map { character -> String in
            if character == ":" || character.asciiValue.map({ $0 < 33 || $0 > 126 }) ?? true {
                // Encode the character manually, since it meets the criteria or is non-ASCII
                let utf8 = String(character).utf8
                return utf8.reduce("") { partialResult, byte in
                    partialResult + String(format: "%%%02X", byte)
                }
            } else {
                // Return the character as is, since it doesn't need encoding
                return String(character)
            }
        }.joined()

        return try! HeaderName(safeName)
    }
}

public struct HeaderMapIterator: IteratorProtocol {
    private let dictionary: [HeaderName: [HeaderValue]]
    private var keyIterator: Dictionary<HeaderName, [HeaderValue]>.Iterator
    private var currentKey: HeaderName?
    private var currentValueIterator: Array<HeaderValue>.Iterator?
    
    init(dictionary: [HeaderName: [HeaderValue]]) {
        self.dictionary = dictionary
        self.keyIterator = dictionary.makeIterator()
    }
    
    public mutating func next() -> (name: HeaderName, value: HeaderValue)? {
        // If there is a current array iterator and it has a next value, return the current key and value
        if let value = currentValueIterator?.next() {
            return (currentKey!, value)
        }
        
        // If the current array iterator is done or nil, move to the next key in the dictionary
        if let nextKeyValPair = keyIterator.next() {
            currentKey = nextKeyValPair.key
            currentValueIterator = nextKeyValPair.value.makeIterator()
            
            // Try to get the next value for the new key
            if let value = currentValueIterator?.next() {
                return (currentKey!, value)
            }
        }
        
        // If no more keys or values, iteration is complete
        return nil
    }
}

