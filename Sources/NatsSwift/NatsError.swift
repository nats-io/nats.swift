//
//  NatsErorr.swift
//  NatsSwift
//

// TODO(pp): For now we're using error implementation from old codebase, consider changing
protocol NatsError: Error {
    var description: String { get set }
}

struct NatsConnectionError: NatsError {
    var description: String
    var normalizedError: String {
        return description.trimWhitespacesAndApostrophes().lowercased()
    }
    init(_ description: String) {
        self.description = description
    }
}

struct NatsSubscribeError: NatsError {
    var description: String
    init(_ description: String) {
        self.description = description
    }
}

struct NatsPublishError: NatsError {
    var description: String
    init(_ description: String) {
        self.description = description
    }
}

struct NatsTimeoutError: NatsError {
    var description: String
    init(_ description: String) {
        self.description = description
    }
}


