//
//  NatsClient+Events.swift
//
// NatsSwift
//

import Foundation

extension Client {
    @discardableResult
    public func on(_ events: [NatsEventKind], _ handler: @escaping (NatsEvent) -> Void) -> String {
        guard let connectionHandler = self.connectionHandler else {
            return ""
        }
        return connectionHandler.addListeners(for: events, using: handler)
    }

    @discardableResult
    public func on(_ event: NatsEventKind, _ handler: @escaping (NatsEvent) -> Void) -> String {
        guard let connectionHandler = self.connectionHandler else {
            return ""
        }
        return connectionHandler.addListeners(for: [event], using: handler)
    }

    func off(_ id: String) {
        guard let connectionHandler = self.connectionHandler else {
            return
        }
        connectionHandler.removeListener(id)
    }
}
