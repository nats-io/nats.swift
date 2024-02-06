//
//  NatsErorr.swift
//  NatsSwift
//

// TODO(pp): For now we're using error implementation from old codebase, consider changing
public protocol NatsError: Error {
    var description: String { get }
}

struct NatsServerError: NatsError {
    var description: String
    var normalizedError: String {
        return description.trimWhitespacesAndApostrophes().lowercased()
    }
    init(_ description: String) {
        self.description = description
    }
}

struct NatsParserError: NatsError {
    var description: String
    init(_ description: String) {
        self.description = description
    }
}

struct NatsClientError: NatsError {
    var description: String
    init(_ description: String) {
        self.description = description
    }
}
