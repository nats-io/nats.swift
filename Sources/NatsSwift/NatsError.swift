//
//  NatsErorr.swift
//  SwiftyNats
//

protocol NatsError: Error {
    var description: String { get set }
}

struct NatsConnectionError: NatsError {
    var description: String
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


