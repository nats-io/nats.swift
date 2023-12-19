//
//  NatsMessage.swift
//  NatsSwift
//

import Foundation
import NIO

// TODO(pp): rework to have seperate structs for sending NatsMsg and for server operations (info, message with subscription ID, error etc)
public struct OldNatsMessage {

    public let payload: String?
    public let byteCount: UInt32?
    public let subject: NatsSubject
    public let replySubject: NatsSubject?
    public let sid: UInt64?
    public let mid: String

    init(payload: String?, byteCount: UInt32?, subject: NatsSubject, replySubject: NatsSubject? = nil, sid: UInt64?) {
        self.payload = payload
        self.byteCount = byteCount
        self.subject = subject
        self.replySubject = replySubject
        self.sid = sid
        self.mid = String.hash()
    }
}

extension OldNatsMessage {
    internal static func publish(payload: String, subject: String, using allocator: ByteBufferAllocator) -> ByteBuffer {
        var buffer = allocator.buffer(capacity: payload.utf8.count + subject.utf8.count + NatsOperation.publish.rawValue.count + 10) // Estimated capacity
        buffer.writeString("\(NatsOperation.publish.rawValue) \(subject) \(payload.utf8.count)\r\n")
        buffer.writeString(payload)
        buffer.writeString("\r\n")
        return buffer
    }
    internal static func publish(payload: Data?, subject: String, using allocator: ByteBufferAllocator) -> ByteBuffer {
        var buffer: ByteBuffer
        if let payload {
            buffer = allocator.buffer(capacity: payload.count + subject.utf8.count + NatsOperation.publish.rawValue.count + 10) // Estimated capacity
            buffer.writeString("\(NatsOperation.publish.rawValue) \(subject) \(payload.count)\r\n")
            buffer.writeData(payload)
            buffer.writeString("\r\n")
        } else {
            buffer = allocator.buffer(capacity: subject.utf8.count + NatsOperation.publish.rawValue.count + 10) // Estimated capacity
            buffer.writeString("\(NatsOperation.publish.rawValue) \(subject) 0\r\n")
            buffer.writeString("\r\n")
        }
       
        return buffer
    }
    internal static func subscribe(subject: String, sid: String, queue: String = "") -> String {
        return "\(NatsOperation.subscribe.rawValue) \(subject) \(queue) \(sid)\r\n"
    }
    internal static func subscribe(subject: String, sid: UInt64, queue: String? = nil) -> String {
        if let queue {
            return "\(NatsOperation.subscribe.rawValue) \(subject) \(queue) \(sid)\r\n"
        } else {
            return "\(NatsOperation.subscribe.rawValue) \(subject) \(sid)\r\n"
        }
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

    internal static func parse(_ message: String) -> OldNatsMessage? {

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

        return OldNatsMessage(
            payload: payload,
            byteCount: byteCount,
            subject: NatsSubject(subject: subject, id: sid),
            replySubject: replySubject == nil ? nil : NatsSubject(subject: replySubject!),
            sid: UInt64(sid)
        )

    }
}

// TODO(pp) Add headers, status, description etc
public struct NatsMessage {
    public let payload: Data?
    public let subject: String
    public let replySubject: String?
    public let length: UInt64
}
