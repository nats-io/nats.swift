//
//  NatsClient.swift
//  NatsSwift
//

import Foundation
import NIO
import NIOFoundationCompat
import Dispatch

import Logging

var logger = Logger(label: "NatsSwift")

/// Client connection states
public enum NatsState {
    case connected
    case disconnected
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

    public func publish(_ payload: Data, subject: String, reply: String? = nil, headers: HeaderMap? = nil) throws {
        logger.debug("publish")
        try self.connectionHandler.write(operation: ClientOp.Publish((subject, reply, payload, headers)))
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
            case let .HMessage(msg):
                self.handleIncomingHMessage(msg)
            case let .Info(serverInfo):
                logger.debug("info \(op)")
                self.serverInfo = serverInfo
            default:
                logger.debug("unknown operation type")
            }
        }
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
        let natsMsg = NatsMessage(payload: message.payload, subject: message.subject, replySubject: message.reply, length: message.length, headers: nil, status: nil)
        if let sub = self.subscriptions[message.sid] {
            sub.receiveMessage(natsMsg)
        }
    }

    func handleIncomingHMessage(_ message: HMessageInbound) {
        let natsMsg = NatsMessage(payload: message.payload, subject: message.subject, replySubject: message.reply, length: message.length, headers: message.headers, status: nil)
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
