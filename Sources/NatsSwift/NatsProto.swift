//
//  NatsProto.swift
//  NatsSwift
//

import Foundation
import NIO

internal enum NatsOperation: String {
    case connect = "CONNECT"
    case subscribe = "SUB"
    case unsubscribe = "UNSUB"
    case publish = "PUB"
    case hpublish = "HPUB"
    case message = "MSG"
    case hmessage = "HMSG"
    case info = "INFO"
    case ok = "+OK"
    case error = "-ERR"
    case ping = "PING"
    case pong = "PONG"

    var rawBytes: [UInt8] {
        return Array(self.rawValue.utf8)
    }

    static func allOperations() -> [NatsOperation] {
        return [
            .connect, .subscribe, .unsubscribe, .publish, .message, .hmessage, .info, .ok, .error,
            .ping, .pong,
        ]
    }
}

enum ServerOp {
    case ok
    case info(ServerInfo)
    case ping
    case pong
    case error(NatsServerError)
    case message(MessageInbound)
    case hMessage(HMessageInbound)

    static func parse(from msg: Data) throws -> ServerOp {
        guard msg.count > 2 else {
            throw NatsParserError(
                "unable to parse inbound message: \(String(data: msg, encoding: .utf8)!)")
        }
        let msgType = msg.getMessageType()
        switch msgType {
        case .message:
            return try message(MessageInbound.parse(data: msg))
        case .hmessage:
            return try hMessage(HMessageInbound.parse(data: msg))
        case .info:
            return try info(ServerInfo.parse(data: msg))
        case .ok:
            return ok
        case .error:
            if let errMsg = msg.removePrefix(Data(NatsOperation.error.rawBytes)).toString() {
                return error(NatsServerError(errMsg))
            }
            return error(NatsServerError("unexpected error"))
        case .ping:
            return ping
        case .pong:
            return pong
        default:
            throw NatsParserError("unknown server op: \(String(data: msg, encoding: .utf8)!)")
        }
    }
}

// TODO(pp): add headers and HMSG parsing
internal struct HMessageInbound: Equatable {
    private static let newline = UInt8(ascii: "\n")
    private static let space = UInt8(ascii: " ")
    var subject: String
    var sid: UInt64
    var reply: String?
    var payload: Data?
    var headers: HeaderMap
    var headersLength: Int
    var length: Int

    // Parse the operation syntax: HMSG <subject> <sid> [reply-to]
    internal static func parse(data: Data) throws -> HMessageInbound {
        let protoComponents =
            data
            .dropFirst(NatsOperation.hmessage.rawValue.count)  // Assuming msg starts with "HMSG "
            .split(separator: space)
            .filter { !$0.isEmpty }

        let parseArgs: ((Data, Data, Data?, Data, Data) throws -> HMessageInbound) = {
            subjectData, sidData, replyData, lengthHeaders, lengthData in
            let subject = String(decoding: subjectData, as: UTF8.self)
            guard let sid = UInt64(String(decoding: sidData, as: UTF8.self)) else {
                throw NatsParserError("unable to parse subscription ID as number")
            }
            var replySubject: String? = nil
            if let replyData = replyData {
                replySubject = String(decoding: replyData, as: UTF8.self)
            }
            let headersLength = Int(String(decoding: lengthHeaders, as: UTF8.self)) ?? 0
            let length = Int(String(decoding: lengthData, as: UTF8.self)) ?? 0
            return HMessageInbound(
                subject: subject, sid: sid, reply: replySubject, payload: nil, headers: HeaderMap(),
                headersLength: headersLength, length: length)
        }

        var msg: HMessageInbound
        switch protoComponents.count {
        case 4:
            msg = try parseArgs(
                protoComponents[0], protoComponents[1], nil, protoComponents[2],
                protoComponents[3])
        case 5:
            msg = try parseArgs(
                protoComponents[0], protoComponents[1], protoComponents[2], protoComponents[3],
                protoComponents[4])
        default:
            throw NatsParserError("unable to parse inbound message header")
        }
        return msg
    }
}

// TODO(pp): add headers and HMSG parsing
internal struct MessageInbound: Equatable {
    private static let newline = UInt8(ascii: "\n")
    private static let space = UInt8(ascii: " ")
    var subject: String
    var sid: UInt64
    var reply: String?
    var payload: Data?
    var length: Int

    // Parse the operation syntax: MSG <subject> <sid> [reply-to]
    internal static func parse(data: Data) throws -> MessageInbound {
        let protoComponents =
            data
            .dropFirst(NatsOperation.message.rawValue.count)  // Assuming msg starts with "MSG "
            .split(separator: space)
            .filter { !$0.isEmpty }

        let parseArgs: ((Data, Data, Data?, Data) throws -> MessageInbound) = {
            subjectData, sidData, replyData, lengthData in
            let subject = String(decoding: subjectData, as: UTF8.self)
            guard let sid = UInt64(String(decoding: sidData, as: UTF8.self)) else {
                throw NatsParserError("unable to parse subscription ID as number")
            }
            var replySubject: String? = nil
            if let replyData = replyData {
                replySubject = String(decoding: replyData, as: UTF8.self)
            }
            let length = Int(String(decoding: lengthData, as: UTF8.self)) ?? 0
            return MessageInbound(
                subject: subject, sid: sid, reply: replySubject, payload: nil, length: length)
        }

        var msg: MessageInbound
        switch protoComponents.count {
        case 3:
            msg = try parseArgs(protoComponents[0], protoComponents[1], nil, protoComponents[2])
        case 4:
            msg = try parseArgs(
                protoComponents[0], protoComponents[1], protoComponents[2], protoComponents[3])
        default:
            throw NatsParserError("unable to parse inbound message header")
        }
        return msg
    }
}

/// Struct representing server information in NATS.
struct ServerInfo: Codable, Equatable {
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
    /// Whether server goes into lame duck
    private let _lameDuckMode: Bool?
    var lameDuckMode: Bool {
        get{
            return _lameDuckMode ?? false
        }
    }

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
        case _lameDuckMode = "ldm"
    }

    internal static func parse(data: Data) throws -> ServerInfo {
        let info = data.removePrefix(prefix)
        return try JSONDecoder().decode(self, from: info)
    }
}

enum ClientOp {
    case publish((subject: String, reply: String?, payload: Data?, headers: HeaderMap?))
    case subscribe((sid: UInt64, subject: String, queue: String?))
    case unsubscribe((sid: UInt64, max: UInt64?))
    case connect(ConnectInfo)
    case ping
    case pong

    internal func asBytes(using allocator: ByteBufferAllocator) throws -> ByteBuffer {
        var buffer: ByteBuffer
        switch self {
        case .publish((let subject, let reply, let payload, let headers)):
            if let payload = payload {
                buffer = allocator.buffer(
                    capacity: payload.count + subject.utf8.count
                        + NatsOperation.publish.rawValue.count + 12)
                if headers != nil {
                    buffer.writeData(NatsOperation.hpublish.rawBytes)
                } else {
                    buffer.writeData(NatsOperation.publish.rawBytes)
                }
                buffer.writeString(" ")
                buffer.writeString(subject)
                buffer.writeString(" ")
                if let reply = reply {
                    buffer.writeString("\(reply) ")
                }
                if let headers = headers {
                    let headers = headers.toBytes()
                    let totalLen = headers.count + payload.count
                    let headersLen = headers.count
                    buffer.writeString("\(headersLen) \(totalLen)\r\n")
                    buffer.writeData(headers)
                } else {
                    buffer.writeString("\(payload.count)\r\n")
                }
                buffer.writeData(payload)
                buffer.writeString("\r\n")
            } else {
                buffer = allocator.buffer(
                    capacity: subject.utf8.count + NatsOperation.publish.rawValue.count + 12)
                buffer.writeData(NatsOperation.publish.rawBytes)
                buffer.writeString(" ")
                buffer.writeString(subject)
                if let reply = reply {
                    buffer.writeString("\(reply) ")
                }
                buffer.writeString("0\r\n")
            }

        case .subscribe((let sid, let subject, let queue)):
            buffer = allocator.buffer(capacity: 0)
            if let queue {
                buffer.writeString(
                    "\(NatsOperation.subscribe.rawValue) \(subject) \(queue) \(sid)\r\n")
            } else {
                buffer.writeString("\(NatsOperation.subscribe.rawValue) \(subject) \(sid)\r\n")
            }

        case .unsubscribe((let sid, let max)):
            buffer = allocator.buffer(capacity: 0)
            if let max {
                buffer.writeString("\(NatsOperation.unsubscribe.rawValue) \(sid) \(max)\r\n")
            } else {
                buffer.writeString("\(NatsOperation.unsubscribe.rawValue) \(sid)\r\n")
            }
        case .connect(let info):
            let json = try JSONEncoder().encode(info)
            buffer = allocator.buffer(capacity: json.count + 5)
            buffer.writeString("\(NatsOperation.connect.rawValue) ")
            buffer.writeData(json)
            buffer.writeString("\r\n")
            return buffer
        case .ping:
            buffer = allocator.buffer(capacity: 8)
            buffer.writeString("\(NatsOperation.ping.rawValue)\r\n")
        case .pong:
            buffer = allocator.buffer(capacity: 8)
            buffer.writeString("\(NatsOperation.pong.rawValue)\r\n")
        }
        return buffer
    }
}

/// Info to construct a CONNECT message.
struct ConnectInfo: Encodable {
    /// Turns on +OK protocol acknowledgments.
    var verbose: Bool
    /// Turns on additional strict format checking, e.g. for properly formed
    /// subjects.
    var pedantic: Bool
    /// User's JWT.
    var userJwt: String?
    /// Public nkey.
    var nkey: String
    /// Signed nonce, encoded to Base64URL.
    var signature: String?
    /// Optional client name.
    var name: String
    /// If set to `true`, the server (version 1.2.0+) will not send originating
    /// messages from this connection to its own subscriptions. Clients should
    /// set this to `true` only for server supporting this feature, which is
    /// when proto in the INFO protocol is set to at least 1.
    var echo: Bool
    /// The implementation language of the client.
    var lang: String
    /// The version of the client.
    var version: String
    /// Sending 0 (or absent) indicates client supports original protocol.
    /// Sending 1 indicates that the client supports dynamic reconfiguration
    /// of cluster topology changes by asynchronously receiving INFO messages
    /// with known servers it can reconnect to.
    var natsProtocol: NatsProtocol
    /// Indicates whether the client requires an SSL connection.
    var tlsRequired: Bool
    /// Connection username (if `auth_required` is set)
    var user: String
    /// Connection password (if auth_required is set)
    var pass: String
    /// Client authorization token (if auth_required is set)
    var authToken: String
    /// Whether the client supports the usage of headers.
    var headers: Bool
    /// Whether the client supports no_responders.
    var noResponders: Bool
    enum CodingKeys: String, CodingKey {
        case verbose
        case pedantic
        case userJwt = "jwt"
        case nkey
        case signature = "sig"  // Custom key name for JSON
        case name
        case echo
        case lang
        case version
        case natsProtocol = "protocol"
        case tlsRequired = "tls_required"
        case user
        case pass
        case authToken = "auth_token"
        case headers
        case noResponders = "no_responders"
    }
}

enum NatsProtocol: Encodable {
    case original
    case dynamic

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()

        switch self {
        case .original:
            try container.encode(0)
        case .dynamic:
            try container.encode(1)
        }
    }
}
