//
//  NatsProto.swift
//  NatsSwift
//

import Foundation
import NIO

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
    private static let newline = UInt8(ascii: "\n")
    private static let space = UInt8(ascii: " ")
    var subject: String
    var sid: UInt64
    var reply: String?
    var payload: Data?
    var length: Int
    
    // Parse the operation syntax: MSG <subject> <sid> [reply-to]
    internal static func parse(data: Data) throws -> MessageInbound {
        let protoComponents = data
            .dropFirst(NatsOperation.message.rawValue.count)  // Assuming the message starts with "MSG "
            .split(separator: space)
            .filter { !$0.isEmpty }
        
        
        let parseArgs: ((Data, Data, Data?, Data) throws -> MessageInbound) = { subjectData, sidData, replyData, lengthData in
            let subject = String(decoding: subjectData, as: UTF8.self)
            guard let sid = UInt64(String(decoding: sidData, as: UTF8.self)) else {
                throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse subscription ID as number"])
            }
            var replySubject: String? = nil
            if let replyData = replyData {
                replySubject = String(decoding: replyData, as: UTF8.self)
            }
            let length = Int(String(decoding: lengthData, as: UTF8.self)) ?? 0
            return MessageInbound(subject: subject, sid: sid, reply: replySubject, payload: nil, length: length)
        }
        
        var msg: MessageInbound
        switch protoComponents.count {
        case 3:
            msg = try parseArgs(protoComponents[0], protoComponents[1], nil, protoComponents[2])
        case 4:
            msg = try parseArgs(protoComponents[0], protoComponents[1], protoComponents[2], protoComponents[3])
        default:
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse inbound message header"])
        }
        return msg
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
