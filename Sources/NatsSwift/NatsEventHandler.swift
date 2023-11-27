//
//  NatsEventHandler.swift
//  NatsSwift
//

internal struct NatsEventHandler {
    let listenerId: String
    let handler: (NatsEvent) -> Void
    let autoOff: Bool
    init(lid: String, handler: @escaping (NatsEvent) -> Void, autoOff: Bool = false) {
        self.listenerId = lid
        self.handler = handler
        self.autoOff = autoOff
    }
}
