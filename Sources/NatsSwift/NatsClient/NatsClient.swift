//
//  NatsClient.swift
//  NatsSwift
//

import Foundation
import NIO
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
}


actor Client {
    var urls: [URL] = []
    var pingInteval: TimeInterval = 1.0
    
    internal let allocator = ByteBufferAllocator()
    internal var buffer: ByteBuffer
    internal var connectionHandler: ConnectionHandler
    
    init(urls: [URL]) {
        self.urls = urls
        self.buffer = allocator.buffer(capacity: 1024)
        self.connectionHandler = ConnectionHandler(inputBuffer: buffer, urls: urls)
        
    }
    init(url: URL) {
        self.urls = [url]
        self.buffer = allocator.buffer(capacity: 1024)
        self.connectionHandler = ConnectionHandler(inputBuffer: buffer, urls: urls)
    }
}

extension Client {
    func connect() async throws  {
        //TODO(jrm): reafactor for reconnection and review error handling.
        //TODO(jrm): handle response
        logger.debug("connect")
        try await self.connectionHandler.connect()
    }
    
    func publish(_ payload: String, subject: String) async throws {
        logger.debug("publish")
        try await self.connectionHandler.writeMessage(NatsMessage.publish(payload: payload, subject: subject))
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
        logger.debug("Channel read complete")
        
        let chunkLength = inputBuffer.readableBytes
        
        logger.debug("reading string from buffer")
        guard let inputChunk = inputBuffer.readString(length: chunkLength) else {
            logger.warning("Input buffer can not read into string")
            return
        }
        
        logger.debug("parsing message")
        let messages = inputChunk.parseOutMessages()
        for message in messages {
            logger.debug("getting message type")
            guard let type = message.getMessageType() else { return }
            
            if let continuation = self.serverInfoContinuation {
                logger.debug("first message callback")
                continuation.resume(returning: message)
                self.serverInfoContinuation = nil
                continue
            }
            
            switch type {
            case .ping:
                logger.debug("ping")
                let pong = NatsMessage.pong()
                _ = self.channel?.write(pong)
                // self.sendMessage(NatsMessage.pong())
            case .error:
                logger.debug("error \(message)")
            case .message:
                logger.debug("message \(message)")
                //   self.handleIncomingMessage(message)
            case .info:
                logger.debug("info \(message)")
                //    self.updateServerInfo(with: message)
            default:
                print("default")
            }
        }
        inputBuffer.clear()
    }
    init(inputBuffer: ByteBuffer, urls: [URL]) {
        self.inputBuffer = self.allocator.buffer(capacity: 1024)
        self.urls = urls
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        self.inputBuffer = allocator.buffer(capacity: 1024)
    }
    internal var group: MultiThreadedEventLoopGroup
    internal var channel: Channel?
    
    private var serverInfoContinuation: CheckedContinuation<String, Error>?
    
    func connect() async throws {
        let firstMessage = try await withCheckedThrowingContinuation { continuation in
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
                    let connectFuture: EventLoopFuture<Channel> = bootstrap.connect(host: host, port: port)
                    
                    connectFuture.whenSuccess { channel in
                        self.channel = channel
                    }
                    
                    connectFuture.whenFailure { error in
                        print(error)
                    }
                    self.channel = try await bootstrap.connect(host: host, port: port).get()
                } catch {
                    continuation.resume(throwing: error)
                }
            }
            // Wait for the first message after sending the connect request
        }
        print("Received first message: \(firstMessage)")
        let connect = ConnectInfo(verbose: false, pedantic: false, userJwt: nil, nkey: "", signature: nil, name: "", echo: false, lang: self.lang, version: self.version, natsProtocol: .original, tlsRequired: false, user: "", pass: "", authToken: "", headers: false, noResponders: false)

        let connectStr = NatsMessage.connect(config: connect)
        print(connectStr)
        try await self.writeMessage(connectStr)
    }
    
    func writeMessage(_ message: String) async throws {
        print("msg: \(message)")
        var buffer = self.channel?.allocator.buffer(capacity: message.utf8.count)
        buffer?.writeString(message)
        try await self.channel?.writeAndFlush(buffer)
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
    internal var subjectHandlerStore: [ NatsSubject: (NatsMessage) -> Void] = [:]
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
    func subscribe(to subject: String, _ handler: @escaping (NatsMessage) -> Void) -> NatsSubject
    func subscribe(to subject: String, asPartOf queue: String, _ handler: @escaping (NatsMessage) -> Void) -> NatsSubject
    func unsubscribe(from subject: NatsSubject)
    
    func subscribeSync(to subject: String, _ handler: @escaping (NatsMessage) -> Void) throws -> NatsSubject
    func subscribeSync(to subject: String, asPartOf queue: String, _ handler: @escaping (NatsMessage) -> Void) throws -> NatsSubject
    func unsubscribeSync(from subject: NatsSubject) throws
}

protocol NatsPublish {
    func publish(_ payload: String, to subject: String)
    func publish(_ payload: String, to subject: NatsSubject)
    func reply(to message: NatsMessage, withPayload payload: String)
    
    func publishSync(_ payload: String, to subject: String) throws
    func publishSync(_ payload: String, to subject: NatsSubject) throws
    func replySync(to message: NatsMessage, withPayload payload: String) throws
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
