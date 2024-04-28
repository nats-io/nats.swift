// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Atomics
import Dispatch
import Foundation
import NIO
import NIOFoundationCompat
import NIOHTTP1
import NIOSSL
import NIOWebSocket
import NKeys

class ConnectionHandler: ChannelInboundHandler {
    let lang = "Swift"
    let version = "0.0.1"

    internal var connectedUrl: URL?
    internal let allocator = ByteBufferAllocator()
    internal var inputBuffer: ByteBuffer
    internal var channel: Channel?

    private var eventHandlerStore: [NatsEventKind: [NatsEventHandler]] = [:]

    // Connection options
    internal var retryOnFailedConnect = false
    private var urls: [URL]
    // nanoseconds representation of TimeInterval
    private let reconnectWait: UInt64
    private let maxReconnects: Int?
    private let retainServersOrder: Bool
    private let pingInterval: TimeInterval
    private let requireTls: Bool
    private let tlsFirst: Bool
    private var rootCertificate: URL?
    private var clientCertificate: URL?
    private var clientKey: URL?

    typealias InboundIn = ByteBuffer
    private let stateLock = NSLock()
    internal var state: NatsState = .pending

    private var subscriptions: [UInt64: NatsSubscription]
    private var subscriptionCounter = ManagedAtomic<UInt64>(0)
    private var serverInfo: ServerInfo?
    private var auth: Auth?
    private var parseRemainder: Data?
    private var pingTask: RepeatedTask?
    private var outstandingPings = ManagedAtomic<UInt8>(0)
    private var reconnectAttempts = 0
    private var reconnectTask: Task<(), Never>? = nil

    private var group: MultiThreadedEventLoopGroup

    private var serverInfoContinuation: CheckedContinuation<ServerInfo, Error>?
    private var connectionEstablishedContinuation: CheckedContinuation<Void, Error>?

    private let pingQueue = ConcurrentQueue<RttCommand>()
    private(set) var batchBuffer: BatchBuffer?

    init(
        inputBuffer: ByteBuffer, urls: [URL], reconnectWait: TimeInterval, maxReconnects: Int?,
        retainServersOrder: Bool,
        pingInterval: TimeInterval, auth: Auth?, requireTls: Bool, tlsFirst: Bool,
        clientCertificate: URL?, clientKey: URL?,
        rootCertificate: URL?, retryOnFailedConnect: Bool
    ) {
        self.inputBuffer = self.allocator.buffer(capacity: 1024)
        self.urls = urls
        self.group = .singleton
        self.inputBuffer = allocator.buffer(capacity: 1024)
        self.subscriptions = [UInt64: NatsSubscription]()
        self.reconnectWait = UInt64(reconnectWait * 1_000_000_000)
        self.maxReconnects = maxReconnects
        self.retainServersOrder = retainServersOrder
        self.auth = auth
        self.pingInterval = pingInterval
        self.requireTls = requireTls
        self.tlsFirst = tlsFirst
        self.clientCertificate = clientCertificate
        self.clientKey = clientKey
        self.rootCertificate = rootCertificate
        self.retryOnFailedConnect = retryOnFailedConnect
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var byteBuffer = self.unwrapInboundIn(data)
        inputBuffer.writeBuffer(&byteBuffer)
    }

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
                case .error(let err):
                    continuation.resume(throwing: err)
                case .info(let info):
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
                case .error(let err):
                    continuation.resume(throwing: err)
                default:
                    continuation.resume()
                }
                continue
            }

            switch op {
            case .ping:
                logger.debug("ping")
                Task {
                    do {
                        try await self.write(operation: .pong)
                    } catch {
                        logger.error("error sending pong: \(error)")
                        self.fire(.error(NatsClientError("error sending pong: \(error)")))
                    }
                }
            case .pong:
                logger.debug("pong")
                self.outstandingPings.store(0, ordering: AtomicStoreOrdering.relaxed)
                self.pingQueue.dequeue()?.setRoundTripTime()
            case .error(let err):
                logger.debug("error \(err)")

                let normalizedError = err.normalizedError
                // on some errors, force reconnect
                if normalizedError == "stale connection"
                    || normalizedError == "maximum connections exceeded"
                {
                    inputBuffer.clear()
                    context.fireErrorCaught(err)
                } else {
                    self.fire(.error(err))
                }
            case .message(let msg):
                self.handleIncomingMessage(msg)
            case .hMessage(let msg):
                self.handleIncomingHMessage(msg)
            case .info(let serverInfo):
                logger.debug("info \(op)")
                self.serverInfo = serverInfo
                if serverInfo.lameDuckMode {
                    self.fire(.lameDuckMode)
                }
                self.serverInfo = serverInfo
                updateServersList(info: serverInfo)
            default:
                logger.debug("unknown operation type: \(op)")
            }
        }
        inputBuffer.clear()
    }

    private func handleIncomingMessage(_ message: MessageInbound) {
        let natsMsg = NatsMessage(
            payload: message.payload, subject: message.subject, replySubject: message.reply,
            length: message.length, headers: nil, status: nil, description: nil)
        if let sub = self.subscriptions[message.sid] {
            sub.receiveMessage(natsMsg)
        }
    }

    private func handleIncomingHMessage(_ message: HMessageInbound) {
        let natsMsg = NatsMessage(
            payload: message.payload, subject: message.subject, replySubject: message.reply,
            length: message.length, headers: message.headers, status: message.status,
            description: message.description)
        if let sub = self.subscriptions[message.sid] {
            sub.receiveMessage(natsMsg)
        }
    }

    func connect() async throws {
        var servers = self.urls
        if !self.retainServersOrder {
            servers = self.urls.shuffled()
        }
        var lastErr: Error?

        // if there are more reconnect attempts than the number of servers,
        // we are after the initial connect, so sleep between servers
        let shouldSleep = self.reconnectAttempts >= self.urls.count
        for s in servers {
            if let maxReconnects {
                if reconnectAttempts >= maxReconnects {
                    throw NatsClientError("could not reconnect; maxReconnects exceeded")
                }

            }
            self.reconnectAttempts += 1
            if shouldSleep {
                try await Task.sleep(nanoseconds: self.reconnectWait)
            }

            do {
                try await connectToServer(s: s)
            } catch let error as NatsConfigError {
                throw error
            } catch {
                logger.error("error connecting to server: \(error)")
                lastErr = error
                continue
            }
            lastErr = nil
            break
        }
        if let lastErr {
            self.state = .disconnected
            throw lastErr
        }
        self.reconnectAttempts = 0
        guard let channel = self.channel else {
            throw NatsClientError("internal error: empty channel")
        }
        // Schedule the task to send a PING periodically
        let pingInterval = TimeAmount.nanoseconds(Int64(self.pingInterval * 1_000_000_000))
        self.pingTask = channel.eventLoop.scheduleRepeatedTask(
            initialDelay: pingInterval, delay: pingInterval
        ) { _ in
            Task { await self.sendPing() }
        }
        logger.debug("connection established")
        return
    }

    private func connectToServer(s: URL) async throws {
        let info = try await withCheckedThrowingContinuation { continuation in
            self.serverInfoContinuation = continuation
            Task.detached {
                do {
                    let (bootstrap, upgradePromise) = try self.bootstrapConnection(to: s)
                    guard let host = s.host, let port = s.port else {
                        upgradePromise.succeed() // avoid promise leaks
                        throw NatsConfigError("no url")
                    }
                    let connect = bootstrap.connect(host: host, port: port)
                    connect.cascadeFailure(to: upgradePromise)
                    self.channel = try await connect.get()
                    guard let channel = self.channel else {
                        upgradePromise.succeed() // avoid promise leaks
                        throw NatsClientError("internal error: empty channel")
                    }

                    try await upgradePromise.futureResult.get()

                    self.batchBuffer = BatchBuffer(channel: channel)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
            // Wait for the first message after sending the connect request
        }
        self.serverInfo = info
        if (info.tlsRequired ?? false || self.requireTls) && !self.tlsFirst && s.scheme != "wss" {
            let tlsConfig = try makeTLSConfig()
            let sslContext = try NIOSSLContext(configuration: tlsConfig)
            let sslHandler = try NIOSSLClientHandler(
                context: sslContext, serverHostname: s.host)
            try await self.channel?.pipeline.addHandler(sslHandler, position: .first)
        }

        try await sendClientConnectInit()
        self.connectedUrl = s
    }

    private func makeTLSConfig() throws -> TLSConfiguration {
        var tlsConfiguration =
            TLSConfiguration.makeClientConfiguration()
        if let rootCertificate = self.rootCertificate {
            tlsConfiguration.trustRoots = .file(
                rootCertificate.path)
        }
        if let clientCertificate = self.clientCertificate,
            let clientKey = self.clientKey
        {
            // Load the client certificate from the PEM file
            let certificate = try NIOSSLCertificate.fromPEMFile(
                clientCertificate.path
            ).map { NIOSSLCertificateSource.certificate($0) }
            tlsConfiguration.certificateChain = certificate

            // Load the private key from the file
            let privateKey = try NIOSSLPrivateKey(
                file: clientKey.path, format: .pem)
            tlsConfiguration.privateKey = .privateKey(
                privateKey)
        }
        return tlsConfiguration
    }

    private func sendClientConnectInit() async throws {
        var initialConnect = ConnectInfo(
            verbose: false, pedantic: false, userJwt: nil, nkey: "", name: "", echo: true,
            lang: self.lang, version: self.version, natsProtocol: .dynamic, tlsRequired: false,
            user: self.auth?.user ?? "", pass: self.auth?.password ?? "",
            authToken: self.auth?.token ?? "", headers: true, noResponders: true)

        if self.auth?.nkey != nil && self.auth?.nkeyPath != nil {
            throw NatsConfigError("cannot use both nkey and nkeyPath")
        }
        if let auth = self.auth, let credentialsPath = auth.credentialsPath {
            let credentials = try await URLSession.shared.data(from: credentialsPath).0
            guard let jwt = JwtUtils.parseDecoratedJWT(contents: credentials) else {
                throw NatsConfigError("failed to extract JWT from credentials file")
            }
            guard let nkey = JwtUtils.parseDecoratedNKey(contents: credentials) else {
                throw NatsConfigError("failed to extract NKEY from credentials file")
            }
            guard let nonce = self.serverInfo?.nonce else {
                throw NatsConfigError("missing nonce")
            }
            let keypair = try KeyPair(seed: String(data: nkey, encoding: .utf8)!)
            let nonceData = nonce.data(using: .utf8)!
            let sig = try keypair.sign(input: nonceData)
            let base64sig = sig.base64EncodedURLSafeNotPadded()
            initialConnect.signature = base64sig
            initialConnect.userJwt = String(data: jwt, encoding: .utf8)!
        }
        if let nkey = self.auth?.nkeyPath {
            let nkeyData = try await URLSession.shared.data(from: nkey).0

            guard let nkeyContent = String(data: nkeyData, encoding: .utf8) else {
                throw NatsConfigError("failed to read NKEY file")
            }
            let keypair = try KeyPair(
                seed: nkeyContent.trimmingCharacters(in: .whitespacesAndNewlines)
            )

            guard let nonce = self.serverInfo?.nonce else {
                throw NatsConfigError("missing nonce")
            }
            let sig = try keypair.sign(input: nonce.data(using: .utf8)!)
            let base64sig = sig.base64EncodedURLSafeNotPadded()
            initialConnect.signature = base64sig
            initialConnect.nkey = keypair.publicKeyEncoded
        }
        if let nkey = self.auth?.nkey {
            let keypair = try KeyPair(seed: nkey)
            guard let nonce = self.serverInfo?.nonce else {
                throw NatsConfigError("missing nonce")
            }
            let nonceData = nonce.data(using: .utf8)!
            let sig = try keypair.sign(input: nonceData)
            let base64sig = sig.base64EncodedURLSafeNotPadded()
            initialConnect.signature = base64sig
            initialConnect.nkey = keypair.publicKeyEncoded
        }
        let connect = initialConnect
        try await withCheckedThrowingContinuation { continuation in
            self.connectionEstablishedContinuation = continuation
            Task.detached {
                do {
                    try await self.write(operation: ClientOp.connect(connect))
                    try await self.write(operation: ClientOp.ping)
                    self.channel?.flush()
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    private func bootstrapConnection(
        to server: URL
    ) throws -> (ClientBootstrap, EventLoopPromise<Void>) {
        let upgradePromise: EventLoopPromise<Void> = self.group.any().makePromise(of: Void.self)
        let bootstrap = ClientBootstrap(group: self.group)
            .channelOption(
                ChannelOptions.socket(
                    SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR),
                value: 1
            )
            .channelInitializer { channel in
                if self.requireTls && self.tlsFirst {
                    upgradePromise.succeed(())
                    do {
                        let tlsConfig = try self.makeTLSConfig()
                        let sslContext = try NIOSSLContext(
                            configuration: tlsConfig)
                        let sslHandler = try NIOSSLClientHandler(
                            context: sslContext, serverHostname: server.host!)
                        //Fixme(jrm): do not ignore error from addHandler future.
                        channel.pipeline.addHandler(sslHandler).flatMap { _ in
                            channel.pipeline.addHandler(self)
                        }.whenComplete { result in
                            switch result {
                            case .success():
                                print("success")
                            case .failure(let error):
                                print("error: \(error)")
                            }
                        }
                        return channel.eventLoop.makeSucceededFuture(())
                    } catch {
                        return channel.eventLoop.makeFailedFuture(error)
                    }
                } else {
                    if server.scheme == "ws" || server.scheme == "wss" {
                        let host = server.host ?? "localhost"
                        let port = server.port ?? 80
                        let httpUpgradeRequestHandler = HTTPUpgradeRequestHandler(
                            host: "\(host):\(port)", upgradePromise: upgradePromise)
                        let httpUpgradeRequestHandlerBox = NIOLoopBound(
                            httpUpgradeRequestHandler, eventLoop: channel.eventLoop)

                        let websocketUpgrader = NIOWebSocketClientUpgrader(
                            maxFrameSize: 8 * 1024 * 1024,
                            automaticErrorHandling: true,
                            upgradePipelineHandler: { channel, _ in
                                let wsh = NIOWebSocketFrameAggregator(
                                    minNonFinalFragmentSize: 0,
                                    maxAccumulatedFrameCount: Int.max,
                                    maxAccumulatedFrameSize: Int.max
                                )
                                return channel.pipeline.addHandler(wsh).flatMap {
                                    channel.pipeline.addHandler(WebSocketByteBufferCodec()).flatMap
                                    {
                                        channel.pipeline.addHandler(self)
                                    }
                                }
                            }
                        )

                        let config: NIOHTTPClientUpgradeConfiguration = (
                            upgraders: [websocketUpgrader],
                            completionHandler: { context in
                                upgradePromise.succeed(())
                                channel.pipeline.removeHandler(
                                    httpUpgradeRequestHandlerBox.value, promise: nil)
                            }
                        )

                        if server.scheme == "wss" {
                            do {
                                let tlsConfig = try self.makeTLSConfig()
                                let sslContext = try NIOSSLContext(
                                    configuration: tlsConfig)
                                let sslHandler = try NIOSSLClientHandler(
                                    context: sslContext, serverHostname: server.host!)
                                // The sync methods here are safe because we're on the channel event loop
                                // due to the promise originating on the event loop of the channel.
                                try channel.pipeline.syncOperations.addHandler(sslHandler)
                            } catch {
                                upgradePromise.fail(error)
                                return channel.eventLoop.makeFailedFuture(error)
                            }
                        }

                        //Fixme(jrm): do not ignore error from addHandler future.
                        channel.pipeline.addHTTPClientHandlers(
                            leftOverBytesStrategy: .forwardBytes,
                            withClientUpgrade: config
                        ).flatMap {
                            channel.pipeline.addHandler(httpUpgradeRequestHandlerBox.value)
                        }.whenComplete { result in
                            switch result {
                            case .success():
                                logger.debug("success")
                            case .failure(let error):
                                logger.debug("error: \(error)")
                            }
                        }
                    } else {
                        upgradePromise.succeed(())
                        //Fixme(jrm): do not ignore error from addHandler future.
                        channel.pipeline.addHandler(self).whenComplete { result in
                            switch result {
                            case .success():
                                logger.debug("success")
                            case .failure(let error):
                                logger.debug("error: \(error)")
                            }
                        }
                    }
                    return channel.eventLoop.makeSucceededFuture(())
                }
            }.connectTimeout(.seconds(5))
        return (bootstrap, upgradePromise)
    }

    private func updateServersList(info: ServerInfo) {
        if let connectUrls = info.connectUrls {
            for connectUrl in connectUrls {
                guard let url = URL(string: connectUrl) else {
                    continue
                }
                if !self.urls.contains(url) {
                    urls.append(url)
                }
            }
        }
    }

    func close() async throws {
        self.reconnectTask?.cancel()
        await self.reconnectTask?.value

        guard let eventLoop = self.channel?.eventLoop else {
            throw NatsClientError("internal error: channel should not be nil")
        }
        let promise = eventLoop.makePromise(of: Void.self)

        eventLoop.execute {  // This ensures the code block runs on the event loop
            self.state = .closed
            self.pingTask?.cancel()
            self.channel?.close(mode: .all, promise: promise)
        }

        try await promise.futureResult.get()
        self.fire(.closed)
    }

    private func disconnect() async throws {
        self.pingTask?.cancel()
        try await self.channel?.close().get()
    }

    func suspend() async throws {
        self.reconnectTask?.cancel()
        _ = await self.reconnectTask?.value

        guard let eventLoop = self.channel?.eventLoop else {
            throw NatsClientError("internal error: channel should not be nil")
        }
        let promise = eventLoop.makePromise(of: Void.self)

        eventLoop.execute {  // This ensures the code block runs on the event loop
            if self.state == .connected {
                self.state = .suspended
                self.pingTask?.cancel()
                self.channel?.close(mode: .all, promise: promise)
            } else {
                self.state = .suspended
                promise.succeed()
            }
        }

        try await promise.futureResult.get()
        self.fire(.suspended)
    }

    func resume() async throws {
        guard let eventLoop = self.channel?.eventLoop else {
            throw NatsClientError("internal error: channel should not be nil")
        }
        try await eventLoop.submit {
            guard self.state == .suspended else {
                throw NatsClientError(
                    "unable to resume connection - connection is not in suspended state")
            }
            self.handleReconnect()
        }.get()
    }

    func reconnect() async throws {
        try await suspend()
        try await resume()
    }

    internal func sendPing(_ rttCommand: RttCommand? = nil) async {
        let pingsOut = self.outstandingPings.wrappingIncrementThenLoad(
            ordering: AtomicUpdateOrdering.relaxed)
        if pingsOut > 2 {
            handleDisconnect()
            return
        }
        let ping = ClientOp.ping
        do {
            self.pingQueue.enqueue(rttCommand ?? RttCommand.makeFrom(channel: self.channel))
            try await self.write(operation: ping)
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

        if self.state == .connected {
            handleDisconnect()
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        logger.debug("Encountered error on the channel: \(error)")
        context.close(promise: nil)
        if let natsErr = error as? NatsError {
            self.fire(.error(natsErr))
        } else {
            logger.error("unexpected error: \(error)")
        }
        if let continuation = self.serverInfoContinuation {
            self.serverInfoContinuation = nil
            continuation.resume(throwing: error)
            return
        }

        if let continuation = self.connectionEstablishedContinuation {
            self.connectionEstablishedContinuation = nil
            continuation.resume(throwing: error)
            return
        }
        if self.state == .pending {
            handleDisconnect()
        } else if self.state == .disconnected {
            handleReconnect()
        }
    }

    func handleDisconnect() {
        self.state = .disconnected
        if let channel = self.channel {
            let promise = channel.eventLoop.makePromise(of: Void.self)
            Task {
                do {
                    try await self.disconnect()
                    promise.succeed()
                } catch ChannelError.alreadyClosed {
                    // if the channel was already closed, no need to return error
                    promise.succeed()
                } catch {
                    promise.fail(error)
                }
            }
            promise.futureResult.whenComplete { result in
                do {
                    try result.get()
                    self.fire(.disconnected)
                } catch {
                    logger.error("Error closing connection: \(error)")
                }
            }
        }

        handleReconnect()
    }

    func handleReconnect() {
        reconnectTask = Task {
            var reconnected = false
            while !Task.isCancelled
                && (maxReconnects == nil || self.reconnectAttempts < maxReconnects!)
            {
                do {
                    try await self.connect()
                } catch _ as CancellationError {
                    // task cancelled
                    return
                } catch {
                    // TODO(pp): add option to set this to exponential backoff (with jitter)
                    logger.debug("could not reconnect: \(error)")
                    continue
                }
                logger.debug("reconnected")
                reconnected = true
                break
            }
            // if task was cancelled when establishing connection, do not attempt to recreate subscriptions
            if Task.isCancelled {
                return
            }
            if !reconnected && !Task.isCancelled {
                logger.error("could not reconnect; maxReconnects exceeded")
                logger.debug("closing connection")
                do {
                    try await self.close()
                } catch {
                    logger.error("error closing connection: \(error)")
                    return
                }
                return
            }
            for (sid, sub) in self.subscriptions {
                do {
                    try await write(operation: ClientOp.subscribe((sid, sub.subject, nil)))
                } catch {
                    logger.error("error recreating subscription \(sid): \(error)")
                }
            }
            self.channel?.eventLoop.execute {
                self.state = .connected
                self.fire(.connected)
            }
        }
    }

    func write(operation: ClientOp) async throws {
        guard let buffer = self.batchBuffer else {
            throw NatsClientError("not connected")
        }
        try await buffer.writeMessage(operation)
    }

    internal func subscribe(_ subject: String) async throws -> NatsSubscription {
        let sid = self.subscriptionCounter.wrappingIncrementThenLoad(
            ordering: AtomicUpdateOrdering.relaxed)
        try await write(operation: ClientOp.subscribe((sid, subject, nil)))
        let sub = NatsSubscription(sid: sid, subject: subject, conn: self)
        self.subscriptions[sid] = sub
        return sub
    }

    internal func unsubscribe(sub: NatsSubscription, max: UInt64?) async throws {
        if let max, sub.delivered < max {
            // if max is set and the sub has not yet reached it, send unsub with max set
            // and do not remove the sub from connection
            try await write(operation: ClientOp.unsubscribe((sid: sub.sid, max: max)))
            sub.max = max
        } else {
            // if max is not set or the subscription received at least as meny
            // messages as max, send unsub command without max and remove sub from connection
            try await write(operation: ClientOp.unsubscribe((sid: sub.sid, max: nil)))
            self.removeSub(sub: sub)
        }
    }

    internal func removeSub(sub: NatsSubscription) {
        self.subscriptions.removeValue(forKey: sub.sid)
        sub.complete()
    }
}

extension ConnectionHandler {

    internal func fire(_ event: NatsEvent) {
        let eventKind = event.kind()
        guard let handlerStore = self.eventHandlerStore[eventKind] else { return }

        for handler in handlerStore {
            handler.handler(event)
        }
    }

    internal func addListeners(
        for events: [NatsEventKind], using handler: @escaping (NatsEvent) -> Void
    ) -> String {

        let id = String.hash()

        for event in events {
            if self.eventHandlerStore[event] == nil {
                self.eventHandlerStore[event] = []
            }
            self.eventHandlerStore[event]?.append(
                NatsEventHandler(lid: id, handler: handler))
        }

        return id

    }

    internal func removeListener(_ id: String) {

        for event in NatsEventKind.all {

            let handlerStore = self.eventHandlerStore[event]
            if let store = handlerStore {
                self.eventHandlerStore[event] = store.filter { $0.listenerId != id }
            }

        }

    }

}

/// Nats events
public enum NatsEventKind: String {
    case connected = "connected"
    case disconnected = "disconnected"
    case closed = "closed"
    case suspended = "suspended"
    case lameDuckMode = "lameDuckMode"
    case error = "error"
    static let all = [connected, disconnected, closed, lameDuckMode, error]
}

public enum NatsEvent {
    case connected
    case disconnected
    case suspended
    case closed
    case lameDuckMode
    case error(NatsError)

    func kind() -> NatsEventKind {
        switch self {
        case .connected:
            return .connected
        case .disconnected:
            return .disconnected
        case .suspended:
            return .suspended
        case .closed:
            return .closed
        case .lameDuckMode:
            return .lameDuckMode
        case .error(_):
            return .error
        }
    }
}

internal struct NatsEventHandler {
    let listenerId: String
    let handler: (NatsEvent) -> Void
    init(lid: String, handler: @escaping (NatsEvent) -> Void) {
        self.listenerId = lid
        self.handler = handler
    }
}
