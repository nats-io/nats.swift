//
//  NatsClient.swift
//  NatsSwift
//

import Foundation
import NIO
import NIOFoundationCompat
import Dispatch

/// Client connection states
public enum NatsState {
    case connected
    case disconnected
}

/// Nats events
public enum NatsEvent: String {
    case connected      = "connected"
    case disconnected   = "disconnected"
    case response       = "response"
    case error          = "error"
    case dropped        = "dropped"
    case reconnecting   = "reconnecting"
    case informed       = "informed"
    static let all      = [ connected, disconnected, response, error, dropped, reconnecting ]
}

internal enum NatsOperation: String {
    case connect        = "CONNECT"
    case subscribe      = "SUB"
    case unsubscribe    = "UNSUB"
    case publish        = "PUB"
    case message        = "MSG"
    case info           = "INFO"
    case ok             = "+OK"
    case error          = "-ERR"
    case ping           = "PING"
    case pong           = "PONG"

    var rawBytes: [UInt8] {
        return Array(self.rawValue.utf8)
    }

    static func allOperations() -> [NatsOperation] {
        return [.connect, .subscribe, .unsubscribe, .publish, .message, .info, .ok, .error, .ping, .pong]
    }
}


public class Client {
    var urls: [URL] = []
    var pingInteval: TimeInterval = 1.0

    internal let allocator = ByteBufferAllocator()
    internal var buffer: ByteBuffer
    internal var connectionHandler: ConnectionHandler

    public init(urls: [URL]) {
        self.urls = urls
        self.buffer = allocator.buffer(capacity: 1024)
        self.connectionHandler = ConnectionHandler(inputBuffer: buffer, urls: urls)

    }
    public init(url: URL) {
        self.urls = [url]
        self.buffer = allocator.buffer(capacity: 1024)
        self.connectionHandler = ConnectionHandler(inputBuffer: buffer, urls: urls)
    }
}

extension Client {
    public func connect() async throws  {
        //TODO(jrm): reafactor for reconnection and review error handling.
        //TODO(jrm): handle response
        logger.debug("connect")
        try await self.connectionHandler.connect()
    }

    public func publish(_ payload: Data, subject: String, reply: String? = nil) throws {
        logger.debug("publish")
        try self.connectionHandler.write(operation: ClientOp.Publish((subject, reply, payload)))
    }

    public func flush() async throws {
        logger.debug("flush")
        self.connectionHandler.channel?.flush()
    }

    public func subscribe(to subject: String) async throws -> Subscription {
        logger.info("subscribe to subject \(subject)")
        return try await self.connectionHandler.subscribe(subject)
    }
}

// TODO(pp): Implement slow consumer
public class Subscription: AsyncIteratorProtocol {
    public typealias Element = NatsMessage

    private var buffer: [Element]
    private let maxPending: UInt64
    private var closed = false
    private var continuation: CheckedContinuation<Element?, Never>?
    private let lock = NSLock()

    private static let defaultMaxPending: UInt64 = 512*1024

    convenience init() {
        self.init(maxPending: Subscription.defaultMaxPending)
    }

    init(maxPending: uint64) {
        self.maxPending = maxPending
        self.buffer = []
    }

    public func next() async -> Element? {
        let msg: NatsMessage? = lock.withLock {
            if closed {
                print("closed")
                return nil
            }

            if let message = buffer.first {
                buffer.removeFirst()
                return message
            }
            return nil
        }
        if let msg {
            return msg
        }
        return await withCheckedContinuation { continuation in
            lock.withLock{
                self.continuation = continuation
            }
        }
    }

    func receiveMessage(_ message: Element) {
        lock.withLock {
            if let continuation = self.continuation {
                continuation.resume(returning: message)
                self.continuation = nil
            } else if buffer.count < maxPending {
                buffer.append(message)
            }
        }
     }

     func complete() async {
         lock.withLock {
             closed = true
             continuation?.resume(returning: nil)
         }
     }

}

internal class SubscriptionCounter {
    private var counter: UInt64 = 0
    private let queue = DispatchQueue(label: "io.nats.swift.subscriptionCounter")

    func next() -> UInt64 {
        queue.sync {
            counter+=1
            return counter
        }
    }
}

class ConnectionHandler: ChannelInboundHandler {
    let lang = "Swift"
    let version = "0.0.1"
    typealias InboundIn = ByteBuffer
    internal let allocator = ByteBufferAllocator()
    internal var inputBuffer: ByteBuffer
    internal var urls: [URL]
    internal var state: NatsState = .disconnected
    internal var subscriptions: [ UInt64: Subscription ]
    internal var subscriptionCounter = SubscriptionCounter()
    internal var serverInfo: ServerInfo?
    private var parseRemainder: Data?
    
    private var previousChunk: Data?

    func channelActive(context: ChannelHandlerContext) {
        logger.debug("TCP channel active")

        self.state = .connected
        inputBuffer = context.channel.allocator.buffer(capacity: 1024 * 1024 * 8)
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.debug("TCP channel inactive")

        self.state = .disconnected
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        logger.debug("channel read")
        var byteBuffer = self.unwrapInboundIn(data)
        inputBuffer.writeBuffer(&byteBuffer)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        var inputChunk = Data(buffer: inputBuffer)
        
        if let remainder = self.parseRemainder {
            inputChunk.prepend(remainder)
        }

        self.parseRemainder = nil
        let parseResult = inputChunk.parseOutMessages()
        if let remainder = parseResult.remainder {
            self.parseRemainder = remainder
        }
        for op in parseResult.ops {
            if let continuation = self.serverInfoContinuation {
                logger.debug("server info")
                switch op {
                case let .Error(err):
                    continuation.resume(throwing: err)
                case let .Info(info):
                    continuation.resume(returning: info)
                default:
                    continuation.resume(throwing: NSError(domain: "nats_swift", code: 1, userInfo: ["message": "unexpected operation; expected server info: \(op)"]))
                }
                self.serverInfoContinuation = nil
                continue
            }

            if let continuation = self.connectionEstablishedContinuation {
                logger.debug("conn established")
                switch op {
                case let .Error(err):
                    continuation.resume(throwing: err)
                default:
                    continuation.resume()
                }
                self.connectionEstablishedContinuation = nil
                continue
            }

            switch op {
            case .Ping:
                logger.debug("ping")
                do {
                    try self.write(operation: .Pong)
                } catch {
                    // TODO(pp): handle async error
                    logger.error("error sending pong: \(error)")
                    continue
                }
            case let .Error(err):
                logger.debug("error \(err)")
            case let .Message(msg):
                self.handleIncomingMessage(msg)
            case let .Info(serverInfo):
                logger.debug("info \(op)")
                self.serverInfo = serverInfo
            default:
                logger.debug("unknown operation type")
            }
        }
        self.previousChunk = inputChunk
        inputBuffer.clear()
    }
    init(inputBuffer: ByteBuffer, urls: [URL]) {
        self.inputBuffer = self.allocator.buffer(capacity: 1024)
        self.urls = urls
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        self.inputBuffer = allocator.buffer(capacity: 1024)
        self.subscriptions = [UInt64:Subscription]()
    }
    internal var group: MultiThreadedEventLoopGroup
    internal var channel: Channel?

    private var serverInfoContinuation: CheckedContinuation<ServerInfo, Error>?
    private var connectionEstablishedContinuation: CheckedContinuation<Void, Error>?

    func connect() async throws {
        let info = try await withCheckedThrowingContinuation { continuation in
            self.serverInfoContinuation = continuation
            Task.detached {
                do {
                    let bootstrap = ClientBootstrap(group: self.group)
                        .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
                        .channelInitializer { channel in
                            //Fixme(jrm): do not ignore error from addHandler future.
                            channel.pipeline.addHandler(self).whenComplete { result in
                                switch result {
                                case .success():
                                    print("success")
                                case .failure(let error):
                                    print("error: \(error)")
                                }
                            }
                            return channel.eventLoop.makeSucceededFuture(())
                        }.connectTimeout(.seconds(5))
                    guard let url = self.urls.first, let host = url.host, let port = url.port else {
                        throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "no url"])
                    }
                    self.channel = try await bootstrap.connect(host: host, port: port).get()
                } catch {
                    continuation.resume(throwing: error)
                }
            }
            // Wait for the first message after sending the connect request
        }
        self.serverInfo = info
        let connect = ConnectInfo(verbose: false, pedantic: false, userJwt: nil, nkey: "", signature: nil, name: "", echo: true, lang: self.lang, version: self.version, natsProtocol: .dynamic, tlsRequired: false, user: "", pass: "", authToken: "", headers: true, noResponders: true)

        try await withCheckedThrowingContinuation { continuation in
            self.connectionEstablishedContinuation = continuation
            Task.detached {
                do {
                    try self.write(operation: ClientOp.Connect(connect))
                    try self.write(operation: ClientOp.Ping)
                    self.channel?.flush()
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
        logger.debug("connection established")
    }

    func handleIncomingMessage(_ message: MessageInbound) {
        let natsMsg = NatsMessage(payload: message.payload, subject: message.subject, replySubject: message.reply, length: message.length)
        if let sub = self.subscriptions[message.sid] {
            sub.receiveMessage(natsMsg)
        }
    }

    func write(operation: ClientOp) throws {
        guard let allocator = self.channel?.allocator else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "no allocator"])
        }
        let payload = try operation.asBytes(using: allocator)
        try self.writeMessage(payload)
    }

    func writeMessage(_ message: ByteBuffer)  throws {
        channel?.write(message)
        if channel?.isWritable ?? true {
            channel?.flush()
        }
    }

    func subscribe(_ subject: String) async throws -> Subscription {
        let sid = self.subscriptionCounter.next()
        try write(operation: ClientOp.Subscribe((sid, subject, nil)))
        let sub = Subscription()
        self.subscriptions[sid] = sub
        return sub
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
        case userJwt = "user_jwt"
        case nkey
        case signature = "sig" // Custom key name for JSON
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



/// a Nats client
open class NatsClient: NSObject {
    var urls = [String]()
    var connectedUrl: URL?
    public var config: NatsClientConfig

    internal var server: NatsServer?
    internal var writeQueue = OperationQueue()
    internal var eventHandlerStore: [ NatsEvent: [ NatsEventHandler ] ] = [:]
    internal var subjectHandlerStore: [ NatsSubject: (OldNatsMessage) -> Void] = [:]
    internal var messageQueue = OperationQueue()

    public var connectionState: NatsState {
        get { return state }
    }
    internal var state: NatsState = .disconnected {
        didSet {
            // fire event when state is changed only
            if oldValue != state {
                switch state {
                case .connected:
                    self.fire(.connected)
                case .disconnected:
                    self.fire(.disconnected)
                }
            }
        }
    }
    internal var connectionError: NatsError?
    internal var group: MultiThreadedEventLoopGroup?
    internal var thread: Thread?
    internal var channel: Channel?
    internal let dispatchGroup = DispatchGroup()

    // Buffer where incoming messages will be stroed
    internal var inputBuffer: ByteBuffer?

    public init(_ aUrls: [String], _ config: NatsClientConfig) {
        for u in aUrls { self.urls.append(u) }
        self.config = config

        writeQueue.maxConcurrentOperationCount = 1
        logger.debug("Init NatsClient with config: \(config)")
    }

    public convenience init(_ url: String, _ config: NatsClientConfig? = nil) {
        let config = config ?? NatsClientConfig()
        self.init([ url ], config)
    }
}

// MARK: - Protocols

protocol NatsConnection {
    func connect() throws
    func disconnect()
}

protocol NatsSubscribe {
    func subscribe(to subject: String, _ handler: @escaping (OldNatsMessage) -> Void) -> NatsSubject
    func subscribe(to subject: String, asPartOf queue: String, _ handler: @escaping (OldNatsMessage) -> Void) -> NatsSubject
    func unsubscribe(from subject: NatsSubject)

    func subscribeSync(to subject: String, _ handler: @escaping (OldNatsMessage) -> Void) throws -> NatsSubject
    func subscribeSync(to subject: String, asPartOf queue: String, _ handler: @escaping (OldNatsMessage) -> Void) throws -> NatsSubject
    func unsubscribeSync(from subject: NatsSubject) throws
}

protocol NatsPublish {
    func publish(_ payload: String, to subject: String)
    func publish(_ payload: String, to subject: NatsSubject)
    func reply(to message: OldNatsMessage, withPayload payload: String)

    func publishSync(_ payload: String, to subject: String) throws
    func publishSync(_ payload: String, to subject: NatsSubject) throws
    func replySync(to message: OldNatsMessage, withPayload payload: String) throws
}

protocol NatsEventBus {
    func on(_ events: [NatsEvent], _ handler: @escaping (NatsEvent) -> Void) -> String
    func on(_ event: NatsEvent, _ handler: @escaping (NatsEvent) -> Void) -> String
    func on(_ event: NatsEvent, autoOff: Bool, _ handler: @escaping (NatsEvent) -> Void) -> String
    func on(_ events: [NatsEvent], autoOff: Bool, _ handler: @escaping (NatsEvent) -> Void) -> String
    func off(_ id: String)
}

protocol NatsQueue {
    var queueCount: Int { get }
    func flushQueue(maxWait: TimeInterval?) throws
}
