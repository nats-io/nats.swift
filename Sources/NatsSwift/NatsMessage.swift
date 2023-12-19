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

enum ServerOp {
    case Ok
    case Info(ServerInfo)
    case Ping
    case Pong
    case Error(NatsError)
    case Message(MessageInbound)

    static func parse(from message: Data) throws -> ServerOp {
        guard message.count > 2 else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse inbound message: \(message)"])
        }
        let msgType = message.getMessageType()
        switch msgType {
        case .message:
            return try Message(MessageInbound.parse(data: message))
        case .info:
            return try Info(ServerInfo.parse(data: message))
        case .ok:
            return Ok
        case .error:
            if let errMsg = message.toString() {
                return Error(NatsConnectionError(errMsg))
            }
            return Error(NatsConnectionError("unexpected error"))
        case .ping:
            return Ping
        case .pong:
            return Pong
        default:
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "Unknown server op: \(message)"])
        }
    }
}

// TODO(pp): add headers and HMSG parsing
internal struct MessageInbound {
    var subject: String
    var sid: UInt64
    var reply: String?
    var payload: Data?
    var length: UInt64

    // Parse the operation syntax: MSG <subject> <sid> [reply-to] <#bytes>
    internal static func parse(data: Data) throws -> MessageInbound {
        let newline = UInt8(ascii: "\n")
        let space = UInt8(ascii: " ")
        let components = data.split(separator: newline).filter { !$0.isEmpty }

        guard let headerData = components.first else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse inbound message"])
        }

        let payloadData = components.dropFirst().reduce(Data(), +)
        let headerComponents = headerData
            .dropFirst(NatsOperation.message.rawValue.count)  // Assuming the message starts with "MSG "
            .split(separator: space)
            .filter { !$0.isEmpty }

        guard let subjectData = headerComponents.first,
              let sidData = headerComponents.dropFirst().first,
              let lengthData = headerComponents.dropFirst(2).first else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse inbound message header"])
        }

        let subject = String(decoding: subjectData, as: UTF8.self)
        guard let sid = UInt64(String(decoding: sidData, as: UTF8.self)) else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse subscription ID as number"])
        }

        let length: UInt64
        let replySubject: String?

        if headerComponents.count == 3 {
            length = UInt64(String(decoding: lengthData, as: UTF8.self)) ?? 0
            replySubject = nil
        } else if headerComponents.count == 4 {
            replySubject = String(decoding: headerComponents[2], as: UTF8.self)
            length = UInt64(String(decoding: headerComponents[3], as: UTF8.self)) ?? 0
        } else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse inbound message header"])
        }

        return MessageInbound(subject: subject, sid: sid, reply: replySubject, payload: payloadData, length: length)
    }
}


/// Struct representing server information in NATS.
struct ServerInfo: Codable {
    /// The unique identifier of the NATS server.
    let serverId: String
    /// Generated Server Name.
    let serverName: String
    /// The host specified in the cluster parameter/options.
    let host: String
    /// The port number specified in the cluster parameter/options.
    let port: UInt16
    /// The version of the NATS server.
    let version: String
    /// If this is set, then the server should try to authenticate upon connect.
    let authRequired: Bool?
    /// If this is set, then the server must authenticate using TLS.
    let tlsRequired: Bool?
    /// Maximum payload size that the server will accept.
    let maxPayload: UInt
    /// The protocol version in use.
    let proto: Int8
    /// The server-assigned client ID. This may change during reconnection.
    let clientId: UInt64?
    /// The version of golang the NATS server was built with.
    let go: String
    /// The nonce used for nkeys.
    let nonce: String?
    /// A list of server urls that a client can connect to.
    let connectUrls: [String]?
    /// The client IP as known by the server.
    let clientIp: String
    /// Whether the server supports headers.
    let headers: Bool
    /// Whether server goes into lame duck mode.
    let lameDuckMode: Bool?
    
    private static let prefix = NatsOperation.info.rawValue.data(using: .utf8)!

    private enum CodingKeys: String, CodingKey {
        case serverId = "server_id"
        case serverName = "server_name"
        case host
        case port
        case version
        case authRequired = "auth_required"
        case tlsRequired = "tls_required"
        case maxPayload = "max_payload"
        case proto
        case clientId = "client_id"
        case go
        case nonce
        case connectUrls = "connect_urls"
        case clientIp = "client_ip"
        case headers
        case lameDuckMode = "ldm"
    }

    internal static func parse(data: Data) throws -> ServerInfo {
        let info = data.removePrefix(prefix)
        return try JSONDecoder().decode(self, from: info)
    }
}

enum ClientOp {
    case Publish((subject: String, reply: String?, payload: Data? ))
    case Subscribe((sid: UInt64, subject: String, queue: String?))
    case Unsubscribe((sid: UInt64, max: UInt64?))
    case Connect(ConnectInfo)
    case Ping
    case Pong
    
    internal func asBytes(using allocator: ByteBufferAllocator) throws -> ByteBuffer {
        var buffer: ByteBuffer
        switch self {
        case let .Publish((subject, reply, payload)):
            if let payload = payload {
                buffer = allocator.buffer(capacity: payload.count + subject.utf8.count + NatsOperation.publish.rawValue.count + 10)
                buffer.writeString("\(NatsOperation.publish.rawValue) \(subject) \(payload.count)\r\n")
                buffer.writeData(payload)
                buffer.writeString("\r\n")
            } else {
                buffer = allocator.buffer(capacity: subject.utf8.count + NatsOperation.publish.rawValue.count + 10)
                buffer.writeString("\(NatsOperation.publish.rawValue) \(subject) 0\r\n")
            }
            
        case let .Subscribe((sid, subject, queue)):
            buffer = allocator.buffer(capacity: 0)
            if let queue {
                buffer.writeString("\(NatsOperation.subscribe.rawValue) \(subject) \(queue) \(sid)\r\n")
            } else {
                buffer.writeString("\(NatsOperation.subscribe.rawValue) \(subject) \(sid)\r\n")
            }
            
        case let .Unsubscribe((sid, max)):
            buffer = allocator.buffer(capacity: 0)
            if let max {
                buffer.writeString("\(NatsOperation.unsubscribe.rawValue) \(sid) \(max)\r\n")
            } else {
                buffer.writeString("\(NatsOperation.unsubscribe.rawValue) \(sid)\r\n")
            }
        case let .Connect(info):
            let json = try JSONEncoder().encode(info)
            buffer = allocator.buffer(capacity: json.count+5)
            buffer.writeString("\(NatsOperation.connect.rawValue) ")
            buffer.writeData(json)
            buffer.writeString("\r\n")
            return buffer
        case .Ping:
            buffer = allocator.buffer(capacity: 8)
            buffer.writeString("\(NatsOperation.ping.rawValue)\r\n")
        case .Pong:
            buffer = allocator.buffer(capacity: 8)
            buffer.writeString("\(NatsOperation.pong.rawValue)\r\n")
        }
        return buffer
    }
}
