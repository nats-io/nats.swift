//
//  NatsConnection.swift
//  NatsSwoft
//

import Foundation
import NIO
import NIOFoundationCompat
import Dispatch

class ConnectionHandler: ChannelInboundHandler {
    let lang = "Swift"
    let version = "0.0.1"
    
    internal let allocator = ByteBufferAllocator()
    internal var inputBuffer: ByteBuffer
    internal var urls: [URL]
    // nanoseconds representation of TimeInterval
    internal let reconnectWait: UInt64
    internal let maxReconnects: Int?
    
    typealias InboundIn = ByteBuffer
    internal var state: NatsState = .Pending
    internal var subscriptions: [ UInt64: Subscription ]
    internal var subscriptionCounter = SubscriptionCounter()
    internal var serverInfo: ServerInfo?
    private var parseRemainder: Data?


    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        logger.debug("channel read")
        var byteBuffer = self.unwrapInboundIn(data)
        inputBuffer.writeBuffer(&byteBuffer)
    }

    // TODO(pp): errors in parser should trigger context.fireErrorCaught() which invokes errorCaught() and invokes reconnect
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
    init(inputBuffer: ByteBuffer, urls: [URL], reconnectWait: TimeInterval, maxReconnects: Int?) {
        self.inputBuffer = self.allocator.buffer(capacity: 1024)
        self.urls = urls
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        self.inputBuffer = allocator.buffer(capacity: 1024)
        self.subscriptions = [UInt64:Subscription]()
        self.reconnectWait = UInt64(reconnectWait * 1_000_000_000)
        self.maxReconnects = maxReconnects
    }
    internal var group: MultiThreadedEventLoopGroup
    internal var channel: Channel?

    private var serverInfoContinuation: CheckedContinuation<ServerInfo, Error>?
    private var connectionEstablishedContinuation: CheckedContinuation<Void, Error>?

    // TODO(pp): add retryOnFailedConnect option
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
        self.state = .Connected
        logger.debug("connection established")
    }
    
    func channelActive(context: ChannelHandlerContext) {
        logger.debug("TCP channel active")

        inputBuffer = context.channel.allocator.buffer(capacity: 1024 * 1024 * 8)
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.debug("TCP channel inactive")

        handleDisconnect()
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        // TODO(pp): implement Close() on the connection and call it here
        logger.debug("Encountered error on the channel: \(error)")
        self.state = .Disconnected
        handleReconnect()
    }
    
    func handleDisconnect() {
        self.state = .Disconnected
        handleReconnect()
    }
    
    func handleReconnect() {
        Task {
            var attempts = 0
            while maxReconnects == nil || attempts < maxReconnects! {
                do {
                    try await self.connect()
                } catch {
                    // TODO(pp): add option to set this to exponential backoff (with jitter)
                    try await Task.sleep(nanoseconds: self.reconnectWait)
                    attempts += 1
                    continue
                }
                break
            }
            for (sid, sub) in self.subscriptions {
                try write(operation: ClientOp.Subscribe((sid, sub.subject, nil)))
            }
        }
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
        let sub = Subscription(subject: subject)
        self.subscriptions[sid] = sub
        return sub
    }
}
