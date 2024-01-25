//
//  NatsConnection.swift
//  NatsSwoft
//

import Foundation
import NIO
import NIOFoundationCompat
import Dispatch
import Atomics
import NKeys

class ConnectionHandler: ChannelInboundHandler {
    let lang = "Swift"
    let version = "0.0.1"

    internal let allocator = ByteBufferAllocator()
    internal var inputBuffer: ByteBuffer

    // Connection options
    internal var urls: [URL]
    // nanoseconds representation of TimeInterval
    internal let reconnectWait: UInt64
    internal let maxReconnects: Int?
    internal let pingInterval: TimeInterval

    typealias InboundIn = ByteBuffer
    internal var state: NatsState = .Pending
    internal var subscriptions: [ UInt64: Subscription ]
    internal var subscriptionCounter = ManagedAtomic<UInt64>(0)
    internal var serverInfo: ServerInfo?
    internal var auth: Auth?
    private var parseRemainder: Data?
    private var pingTask: RepeatedTask?
    private var outstandingPings = ManagedAtomic<UInt8>(0)


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
        let parseResult: (ops: [ServerOp], remainder: Data?)
        do {
            parseResult = try inputChunk.parseOutMessages()
        } catch {
            // if parsing throws an error, return and reconnect
            inputBuffer.clear()
            context.fireErrorCaught(error)
            return
        }
        if let remainder = parseResult.remainder {
            self.parseRemainder = remainder
        }
        for op in parseResult.ops {
            if let continuation = self.serverInfoContinuation {
                self.serverInfoContinuation = nil
                logger.debug("server info")
                switch op {
                case let .Error(err):
                    continuation.resume(throwing: err)
                case let .Info(info):
                    continuation.resume(returning: info)
                default:
                    // ignore until we get either error or server info
                    continue
                }
                continue
            }

            if let continuation = self.connectionEstablishedContinuation {
                self.connectionEstablishedContinuation = nil
                logger.debug("conn established")
                switch op {
                case let .Error(err):
                    continuation.resume(throwing: err)
                default:
                    continuation.resume()
                }
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
            case .Pong:
                logger.debug("pong")
                self.outstandingPings.store(0, ordering: AtomicStoreOrdering.relaxed)
            case let .Error(err):
                logger.debug("error \(err)")

                let normalizedError = err.normalizedError
                // on some errors, force reconnect
                if normalizedError == "stale connection" || normalizedError == "maximum connections exceeded" {
                    inputBuffer.clear()
                    context.fireErrorCaught(err)
                }
                // TODO(pp): handle auth errors here
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
    init(inputBuffer: ByteBuffer, urls: [URL], reconnectWait: TimeInterval, maxReconnects: Int?, pingInterval: TimeInterval, auth: Auth?) {
        self.inputBuffer = self.allocator.buffer(capacity: 1024)
        self.urls = urls
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        self.inputBuffer = allocator.buffer(capacity: 1024)
        self.subscriptions = [UInt64:Subscription]()
        self.reconnectWait = UInt64(reconnectWait * 1_000_000_000)
        self.maxReconnects = maxReconnects
        self.auth = auth
        self.pingInterval = pingInterval
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
        // TODO(jrm): Add rest of auth here.

        var initialConnect = ConnectInfo(verbose: false, pedantic: false, userJwt: nil, nkey: "",  name: "", echo: true, lang: self.lang, version: self.version, natsProtocol: .dynamic, tlsRequired: false, user: self.auth?.user ?? "", pass: self.auth?.password ?? "", authToken: self.auth?.token ?? "", headers: true, noResponders: true)

        if let auth = self.auth {
            print("in auth path")
            if  let credentialsPath = auth.credentialsPath {
                let credentials = try await URLSession.shared.data(from: credentialsPath).0
                guard let jwt = JwtUtils.parseDecoratedJWT(contents: credentials) else {
                    throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "failed to extract jwt from credentials file"])
                }
                print("JWT: \(String(data: jwt, encoding: .utf8)!)")
                guard let nkey = JwtUtils.parseDecoratedNKey(contents:  credentials) else {
                    throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "failed to extract NKEY from credentials file"])
                }
                print("NKEY: \(nkey)")
                guard let nonce = self.serverInfo?.nonce else  {
                    throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "missing nonce"])
                }
                print("NONCE: \(nonce)")
                print("SEED: \(String(data: nkey, encoding: .utf8)!)")
                let keypair = try KeyPair(seed: String(data: nkey, encoding: .utf8)!)
                let nonceData = nonce.data(using: .utf8)!
                print("NONCE DATA: \(nonceData)")
                let sig  = try keypair.sign(input: nonceData)
                print("SIG: \(sig)")
                var base64sig = sig.base64EncodedString()
                print("SIG STRING: \(base64sig)")
                 base64sig = base64sig
                     .replacingOccurrences(of: "+", with: "-")
                     .replacingOccurrences(of: "/", with: "_")
                base64sig = base64sig.trimmingCharacters(in: CharacterSet(charactersIn: "="))
                initialConnect.signature = base64sig
                initialConnect.userJwt = String(data: jwt, encoding: .utf8)!
                print("SIG: \(initialConnect.signature)")
            }
        }
        let connect = initialConnect
        print("WHOLE CONNECT: \(connect)")
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
        guard let channel = self.channel else {
            throw NSError(domain: "nats_swift", code: 1, userInfo: ["message": "empty channel"])
        }
        // Schedule the task to send a PING periodically
        let pingInterval = TimeAmount.nanoseconds(Int64(self.pingInterval*1_000_000_000))
        self.pingTask = channel.eventLoop.scheduleRepeatedTask(initialDelay: pingInterval, delay: pingInterval) { [weak self] task in
             self?.sendPing()
         }
        logger.debug("connection established")
    }

    func close() async throws {
        self.state = .Closed
        try await disconnect()
        try await self.group.shutdownGracefully()
    }

    func disconnect() async throws {
        self.pingTask?.cancel()
        try await self.channel?.close().get()
    }

    private func sendPing() {
        let pingsOut = self.outstandingPings.wrappingIncrementThenLoad(ordering: AtomicUpdateOrdering.relaxed)
        if pingsOut > 2 {
            handleDisconnect()
            return
        }
        let ping = ClientOp.Ping
        do {
            try self.write(operation: ping)
            logger.debug("sent ping: \(pingsOut)")
        } catch {
            logger.error("Unable to send ping: \(error)")
        }

    }

    func channelActive(context: ChannelHandlerContext) {
        logger.debug("TCP channel active")

        inputBuffer = context.channel.allocator.buffer(capacity: 1024 * 1024 * 8)
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.debug("TCP channel inactive")

        if self.state == .Connected {
            handleDisconnect()
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        // TODO(pp): implement Close() on the connection and call it here
        logger.debug("Encountered error on the channel: \(error)")
        context.close(promise: nil)
        if self.state == .Connected {
            handleDisconnect()
        } else if self.state == .Disconnected {
            handleReconnect()
        }
    }

    func handleDisconnect() {
        self.state = .Disconnected
        if let channel = self.channel {
            let promise = channel.eventLoop.makePromise(of: Void.self)
            Task {
                do {
                    try await self.disconnect()
                    promise.succeed()
                } catch {
                    promise.fail(error)
                }
            }
            promise.futureResult.whenComplete { result in
                do {
                  try result.get()
                } catch {
                    logger.error("Error closing connection: \(error)")
                }
            }
        }

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
                    logger.debug("could not reconnect: \(error)")
                    try await Task.sleep(nanoseconds: self.reconnectWait)
                    attempts += 1
                    continue
                }
                logger.debug("reconnected")
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
        _ = channel?.write(message)
        if channel?.isWritable ?? true {
            channel?.flush()
        }
    }

    func subscribe(_ subject: String) async throws -> Subscription {
        let sid = self.subscriptionCounter.wrappingIncrementThenLoad(ordering: AtomicUpdateOrdering.relaxed)
        try write(operation: ClientOp.Subscribe((sid, subject, nil)))
        let sub = Subscription(subject: subject)
        self.subscriptions[sid] = sub
        return sub
    }
}
