//
//  NatsMessage.swift
//  NatsSwift
//

import Foundation

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

    internal static func publish(payload: String, subject: String) -> String {
        guard let data = payload.data(using: String.Encoding.utf8) else { return "" }
        return "\(NatsOperation.publish.rawValue) \(subject) \(data.count)\r\n\(payload)\r\n"
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
    public let payload: String?
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

    static func parse(from message: String) throws -> ServerOp {
        guard message.count > 2 else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse inbound message: \(message)"])
        }

        let isOperation: ((NatsOperation) -> Bool) = { no in
            let l = no.rawValue.count - 1
            guard message.count > l else { return false }
            let operation = String(message[0...l]).uppercased()
            guard operation == no.rawValue else { return false }
            return true
        }

        let firstCharacter = String(message[0...0]).uppercased()

        switch firstCharacter {
        case "M":
            guard isOperation(.message) else {
                throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "Unknown server op: \(message)"])
            }
            return try Message(MessageInbound.parse(message: message))
        case "I":
            guard isOperation(.info) else {
                throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "Unknown server op: \(message)"])
            }

            return try Info(ServerInfo.parse(message: message))
        case "+":
            guard isOperation(.ok) else {
                throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "Unknown server op: \(message)"])
            }
            return Ok
        case "-":
            guard isOperation(.error) else {
                throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "Unknown server op: \(message)"])
            }
            return Error(NatsConnectionError(message))
        case "P":
            if isOperation(.ping) { return Ping }
            if isOperation(.pong) { return Pong }
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "Unknown server op: \(message)"])
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
    var payload: String?
    var length: UInt64

    // Parse the operation syntax: MSG <subject> <sid> [reply-to] <#bytes>
    internal static func parse(message: String) throws -> MessageInbound {
        let components = message.components(separatedBy: CharacterSet.newlines).filter { !$0.isEmpty }

        if components.count <= 0 {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse inbound message: \(message)"])
        }

        let payload = components[1]
        let header = components[0]
            .removePrefix(NatsOperation.message.rawValue)
            .components(separatedBy: CharacterSet.whitespaces)
            .filter { !$0.isEmpty }

        let subject: String
        let sid: UInt64
        let length: UInt64
        let replySubject: String?

        switch (header.count) {
        case 3:
            subject = header[0]
            guard let parsedID = UInt64(header[1]) else {
                throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse subscription ID as number: \(header[1])"])
            }
            sid = parsedID
            guard let parsedLen = UInt64(header[2]) else {
                throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse message length as number: \(header[2])"])
            }
            length = parsedLen
            replySubject = nil
            break
        case 4:
            subject = header[0]
            guard let parsedID = UInt64(header[1]) else {
                throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse subscription ID as number: \(header[1])"])
            }
            sid = parsedID
            replySubject = nil
            guard let parsedLen = UInt64(header[2]) else {
                throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse message length as number: \(header[3])"])
            }
            length = parsedLen
            break
        default:
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unable to parse inbound message: \(message)"])
        }

        return MessageInbound(subject: subject, sid: sid, reply: replySubject, payload: payload ,length: length)
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

    internal static func parse(message: String) throws -> ServerInfo {
        let infoJSON = message.removeNewlines().removePrefix(NatsOperation.info.rawValue).data(using: .utf8)!
        return try JSONDecoder().decode(self, from: infoJSON)
    }
}

enum ClientOp {
    internal struct MessageOp {
        var subject: String
        var reply: String?
        var payload: String?
    }

    internal struct SubscribeOp {
        var sid: UInt64
        var subject: String
        var queueGroup: String?
    }

    internal struct UnsubscribeOp {
        var sid: UInt64
        var max: UInt64
    }

    case Publish(MessageOp)
    case Subscribe(SubscribeOp)
    case Unsubscribe(UnsubscribeOp)
    case Connect(ConnectInfo)

//    internal func toCommand() throws -> String {
//        let cmd: String
//        switch self {
//        case let .Publish(msg):
//            if let payload = msg.payload {
//                print(payload)
//            }
//
//        case let .Subscribe(sub):
//            print(sub.subject)
//        default:
//            print("")
//        }
//    }
}

// TODO(pp) add headers
internal struct MessageOutbound {
    var subject: String
    var reply: String?
    var payload: String?
}
