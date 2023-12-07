//
//  NatsMessage.swift
//  NatsSwift
//

import Foundation

public struct NatsMessage {

    public let payload: String?
    public let byteCount: UInt32?
    public let subject: NatsSubject
    public let replySubject: NatsSubject?
    public let mid: String

    init(payload: String?, byteCount: UInt32?, subject: NatsSubject, replySubject: NatsSubject? = nil) {
        self.payload = payload
        self.byteCount = byteCount
        self.subject = subject
        self.replySubject = replySubject
        self.mid = String.hash()
    }

}

extension NatsMessage {

    internal static func publish(payload: String, subject: String) -> String {
        guard let data = payload.data(using: String.Encoding.utf8) else { return "" }
        return "\(NatsOperation.publish.rawValue) \(subject) \(data.count)\r\n\(payload)\r\n"
    }
    internal static func subscribe(subject: String, sid: String, queue: String = "") -> String {
        return "\(NatsOperation.subscribe.rawValue) \(subject) \(queue) \(sid)\r\n"
    }
    internal static func unsubscribe(sid: String) -> String {
        return "\(NatsOperation.unsubscribe.rawValue) \(sid)\r\n"
    }
    internal static func pong() -> String {
        return "\(NatsOperation.pong.rawValue)\r\n"
    }
    internal static func ping() -> String {
        return "\(NatsOperation.ping.rawValue)\r\n"
    }
    internal static func connect(config: [String:Any]) -> String {
        guard let data = try? JSONSerialization.data(withJSONObject: config, options: []) else { return "" }
        guard let payload = data.toString() else { return "" }
        return "\(NatsOperation.connect.rawValue) \(payload)\r\n"
    }
    
    internal static func connect(config: ConnectInfo) -> String {
        guard let data = try? JSONEncoder().encode(config) else { return "" }
        guard let payload = data.toString() else { return "" }
        return "\(NatsOperation.connect.rawValue) \(payload)\r\n"
    }

    internal static func parse(_ message: String) -> NatsMessage? {

        logger.debug("Parsing message: \(message)")

        let components = message.components(separatedBy: CharacterSet.newlines).filter { !$0.isEmpty }

        if components.count <= 0 { return nil }

        let payload = components[1]
        let header = components[0]
            .removePrefix(NatsOperation.message.rawValue)
            .components(separatedBy: CharacterSet.whitespaces)
            .filter { !$0.isEmpty }

        let subject: String
        let sid: String
        let byteCount: UInt32?
        let replySubject: String?

        switch (header.count) {
        case 3:
            subject = header[0]
            sid = header[1]
            byteCount = UInt32(header[2])
            replySubject = nil
            break
        case 4:
            subject = header[0]
            sid = header[1]
            replySubject = nil
            byteCount = UInt32(header[3])
            break
        default:
            return nil
        }

        return NatsMessage(
            payload: payload,
            byteCount: byteCount,
            subject: NatsSubject(subject: subject, id: sid),
            replySubject: replySubject == nil ? nil : NatsSubject(subject: replySubject!)
        )

    }

}
