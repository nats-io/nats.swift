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
import NIOConcurrencyHelpers
import NIOFoundationCompat
import NIOHTTP1
import NIOSSL
import NIOWebSocket
import NKeys

final class ConnectionHandler: ChannelInboundHandler, Sendable {
    let lang = "Swift"
    let version = "0.0.1"

    private let _connectedUrl = NIOLockedValueBox<URL?>(nil)
    internal var connectedUrl: URL? {
        get { _connectedUrl.withLockedValue { $0 } }
        set { _connectedUrl.withLockedValue { $0 = newValue } }
    }
    internal let allocator = ByteBufferAllocator()
    private let _inputBuffer: NIOLockedValueBox<ByteBuffer>
    private let _channel = NIOLockedValueBox<Channel?>(nil)
    internal var channel: Channel? {
        get { _channel.withLockedValue { $0 } }
        set { _channel.withLockedValue { $0 = newValue } }
    }

    private let eventHandlerStore = NIOLockedValueBox<[NatsEventKind: [NatsEventHandler]]>([:])

    // Connection options
    private let retryOnFailedConnect: Bool
    private let _urls: NIOLockedValueBox<[URL]>
    private var urls: [URL] {
        get { _urls.withLockedValue { $0 } }
        set { _urls.withLockedValue { $0 = newValue } }
    }
    // nanoseconds representation of TimeInterval
    private let reconnectWait: UInt64
    private let maxReconnects: Int?
    private let retainServersOrder: Bool
    private let pingInterval: TimeInterval
    private let requireTls: Bool
    private let tlsFirst: Bool
    private let rootCertificate: URL?
    private let clientCertificate: URL?
    private let clientKey: URL?

    typealias InboundIn = ByteBuffer
    private struct Lifecycle: Sendable {
        var state = NatsState.pending
        var connectionTask: Task<Void, Error>?
        var stopping = false
    }

    // Admit connection work and change its state under the same lock.
    private let lifecycle = NIOLockedValueBox(Lifecycle())
    private let subscriptions = NIOLockedValueBox([UInt64: NatsSubscription]())

    internal var currentState: NatsState {
        lifecycle.withLockedValue { $0.state }
    }

    private let subscriptionCounter = ManagedAtomic<UInt64>(0)
    private let _serverInfo = NIOLockedValueBox<ServerInfo?>(nil)
    private var serverInfo: ServerInfo? {
        get { _serverInfo.withLockedValue { $0 } }
        set { _serverInfo.withLockedValue { $0 = newValue } }
    }

    private let auth: Auth?
    private let parseRemainder = NIOLockedValueBox<Data?>(nil)
    private let _pingTask = NIOLockedValueBox<RepeatedTask?>(nil)
    private var pingTask: RepeatedTask? {
        get { _pingTask.withLockedValue { $0 } }
        set { _pingTask.withLockedValue { $0 = newValue } }
    }
    private let outstandingPings = ManagedAtomic<UInt8>(0)
    private let _reconnectAttempts = ManagedAtomic<Int>(0)
    private var reconnectAttempts: Int {
        get { _reconnectAttempts.load(ordering: .relaxed) }
        set { _reconnectAttempts.store(newValue, ordering: .relaxed) }
    }

    private let group: MultiThreadedEventLoopGroup

    // Promises retain early results across the asynchronous phases of one attempt.
    private struct Handshake: Sendable {
        let info: EventLoopPromise<ServerInfo>
        let upgraded: EventLoopPromise<Void>
        let established: EventLoopPromise<Channel>
        let channel = NIOLockedValueBox<Channel?>(nil)

        init(eventLoop: EventLoop) {
            info = eventLoop.makePromise(of: ServerInfo.self)
            upgraded = eventLoop.makePromise(of: Void.self)
            established = eventLoop.makePromise(of: Channel.self)
            upgraded.futureResult.cascadeFailure(to: established)
        }

        func fail(_ error: Error) {
            // Preserve the original failure before cancelled setup work reports its own error.
            established.fail(error)
            info.fail(error)
            upgraded.fail(error)
        }
    }

    private let handshake = NIOLockedValueBox<Handshake?>(nil)

    private let pingQueue = ConcurrentQueue<RttCommand>()
    private let _batchBuffer = NIOLockedValueBox<BatchBuffer?>(nil)
    private(set) var batchBuffer: BatchBuffer? {
        get { _batchBuffer.withLockedValue { $0 } }
        set { _batchBuffer.withLockedValue { $0 = newValue } }
    }

    init(
        urls: [URL], reconnectWait: TimeInterval, maxReconnects: Int?,
        retainServersOrder: Bool,
        pingInterval: TimeInterval, auth: Auth?, requireTls: Bool, tlsFirst: Bool,
        clientCertificate: URL?, clientKey: URL?,
        rootCertificate: URL?, retryOnFailedConnect: Bool
    ) {
        self._urls = NIOLockedValueBox(urls)
        self.group = .singleton
        self._inputBuffer = NIOLockedValueBox(allocator.buffer(capacity: 1024))
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
        guard context.channel === self.channel else { return }

        let state: NatsState = self.currentState

        guard state == .connected || state == .pending || state == .connecting else {
            logger.debug("Ignoring channelRead. Current state (\(state)) does not allow reading.")
            return
        }

        var byteBuffer = self.unwrapInboundIn(data)
        _ = _inputBuffer.withLockedValue { $0.writeBuffer(&byteBuffer) }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        guard context.channel === self.channel else { return }

        let state: NatsState = self.currentState
        guard state == .connected || state == .pending || state == .connecting else {
            _inputBuffer.withLockedValue { $0.clear() }
            parseRemainder.withLockedValue { $0 = nil }
            return
        }

        let inputChunkOrNil: Data? = _inputBuffer.withLockedValue { buffer in
            guard buffer.readableBytes > 0 else { return nil }
            return Data(buffer: buffer)
        }
        guard var inputChunk = inputChunkOrNil else { return }

        let remainder = parseRemainder.withLockedValue { value in
            let current = value
            value = nil
            return current
        }

        if let remainder = remainder, !remainder.isEmpty {
            inputChunk.prepend(remainder)
        }

        let parseResult: (ops: [ServerOp], remainder: Data?)
        do {
            parseResult = try inputChunk.parseOutMessages()
        } catch {
            // if parsing throws an error, clear buffer and remainder, then reconnect
            _inputBuffer.withLockedValue { $0.clear() }
            parseRemainder.withLockedValue { $0 = nil }

            if self.currentState != .closed && self.currentState != .suspended {
                self.errorCaught(context: context, error: error)
            }

            return
        }
        if let remainder = parseResult.remainder {
            parseRemainder.withLockedValue { $0 = remainder }
        }
        for op in parseResult.ops {
            if let handshake = self.handshake.withLockedValue({ $0 }) {
                switch op {
                case .info(let info):
                    handshake.info.succeed(info)
                    continue
                case .error(let error):
                    handshake.fail(error)
                    continue
                case .pong:
                    self.outstandingPings.store(0, ordering: .relaxed)
                    handshake.established.succeed(context.channel)
                    continue
                case .ping:
                    context.channel.writeAndFlush(
                        context.channel.allocator.buffer(string: "PONG\r\n")
                    ).whenFailure { handshake.fail($0) }
                    continue
                default:
                    break
                }
            }

            switch op {
            case .ping:
                logger.debug("ping")
                Task {
                    do {
                        try await self.write(operation: .pong)
                    } catch let err as NatsError.ClientError {
                        logger.error("error sending pong: \(err)")
                        self.fire(
                            .error(err))
                    } catch {
                        logger.error("unexpected error sending pong: \(error)")
                    }
                }
            case .pong:
                logger.debug("pong")
                self.outstandingPings.store(0, ordering: AtomicStoreOrdering.relaxed)
                self.pingQueue.dequeue()?.setRoundTripTime()
            case .error(let err):
                logger.debug("error \(err)")

                switch err {
                case .staleConnection, .maxConnectionsExceeded:
                    _inputBuffer.withLockedValue { $0.clear() }
                    parseRemainder.withLockedValue { $0 = nil }
                    context.fireErrorCaught(err)
                case .permissionsViolation(let operation, let subject, _):
                    switch operation {
                    case .subscribe:
                        subscriptions.withLockedValue { subs in
                            for (_, s) in subs {
                                if s.subject == subject {
                                    s.receiveError(NatsError.SubscriptionError.permissionDenied)
                                }
                            }
                        }
                    case .publish:
                        self.fire(.error(err))
                    }
                default:
                    self.fire(.error(err))
                }

                let normalizedError = err.normalizedError
                // on some errors, force reconnect
                if normalizedError == "stale connection"
                    || normalizedError == "maximum connections exceeded"
                {
                    _inputBuffer.withLockedValue { $0.clear() }
                    parseRemainder.withLockedValue { $0 = nil }
                    context.fireErrorCaught(err)
                } else {
                    self.fire(.error(err))
                }
            case .message(let msg):
                self.handleIncomingMessage(msg)
            case .hMessage(let msg):
                self.handleIncomingMessage(msg)
            case .info(let serverInfo):
                logger.debug("info \(op)")
                self.serverInfo = serverInfo
                if serverInfo.lameDuckMode {
                    self.fire(.lameDuckMode)
                }
                updateServersList(info: serverInfo)
            default:
                logger.debug("unknown operation type: \(op)")
            }
        }
        _inputBuffer.withLockedValue { $0.clear() }
    }

    private func handleIncomingMessage(_ message: MessageInbound) {
        let natsMsg = NatsMessage(
            payload: message.payload, subject: message.subject, replySubject: message.reply,
            length: message.length, headers: nil, status: nil, description: nil)
        deliverOutsideLock(natsMsg, toSid: message.sid)
    }

    private func handleIncomingMessage(_ message: HMessageInbound) {
        let natsMsg = NatsMessage(
            payload: message.payload, subject: message.subject, replySubject: message.reply,
            length: message.length, headers: message.headers, status: message.status,
            description: message.description)
        deliverOutsideLock(natsMsg, toSid: message.sid)
    }

    private func deliverOutsideLock(_ natsMsg: NatsMessage, toSid sid: UInt64) {
        let sub = subscriptions.withLockedValue { $0[sid] }
        sub?.receiveMessage(natsMsg)
    }

    func connect() async throws {
        try Task.checkCancellation()
        let task = try lifecycle.withLockedValue { lifecycle in
            switch lifecycle.state {
            case .connected, .connecting:
                throw NatsError.ClientError.alreadyConnected
            case .closed:
                throw NatsError.ClientError.connectionClosed
            case .suspended:
                throw NatsError.ClientError.invalidConnection(
                    "connection is suspended, use resume() instead")
            case .pending, .disconnected:
                break
            }
            guard lifecycle.connectionTask == nil else {
                throw NatsError.ClientError.alreadyConnected
            }
            lifecycle.state = .connecting
            let task = makeConnectionTask(retrying: retryOnFailedConnect)
            lifecycle.connectionTask = task
            return task
        }

        if retryOnFailedConnect { return }
        try await withTaskCancellationHandler {
            try await task.value
        } onCancel: {
            task.cancel()
        }
    }

    private func connectToServers() async throws {
        try Task.checkCancellation()
        var servers = self.urls
        if !self.retainServersOrder {
            servers = self.urls.shuffled()
        }
        var lastErr: Error?

        // After the first pass, wait once before each server attempt.
        let shouldSleep = self.reconnectAttempts >= self.urls.count
        for s in servers {
            if let maxReconnects {
                if reconnectAttempts > 0 && reconnectAttempts >= maxReconnects {
                    if lastErr != nil { break }
                    throw NatsError.ClientError.maxReconnects
                }
            }
            self.reconnectAttempts += 1
            if shouldSleep {
                try await Task.sleep(nanoseconds: self.reconnectWait)
            }

            do {
                try await connectToServer(s: s)
            } catch is CancellationError {
                throw CancellationError()
            } catch let error as NatsError.ConnectError {
                if case .invalidConfig(_) = error {
                    throw error
                }
                logger.debug("error connecting to server: \(error)")
                lastErr = error
                continue
            } catch {
                logger.debug("error connecting to server: \(error)")
                lastErr = error
                continue
            }
            lastErr = nil
            break
        }
        if let lastErr {
            switch lastErr {
            case let error as ChannelError:
                var err: NatsError.ConnectError
                switch error.self {
                case .connectTimeout(_):
                    err = .timeout
                default:
                    err = .io(error)
                }
                throw err
            case let error as NIOConnectionError:
                if let dnsAAAAError = error.dnsAAAAError {
                    throw NatsError.ConnectError.dns(dnsAAAAError)
                } else if let dnsAError = error.dnsAError {
                    throw NatsError.ConnectError.dns(dnsAError)
                } else {
                    throw NatsError.ConnectError.io(error)
                }
            case let err as NIOSSLError:
                throw NatsError.ConnectError.tlsFailure(err)
            case let err as BoringSSLError:
                throw NatsError.ConnectError.tlsFailure(err)
            case let err as NatsError.ServerError:
                throw err
            case let err as NatsError.ConnectError:
                throw err
            default:
                throw NatsError.ConnectError.io(lastErr)
            }
        }
        guard let channel = self.channel else {
            throw NatsError.ClientError.internalError("empty channel")
        }
        // A cancelled restore must release writes waiting for socket backpressure.
        try await withTaskCancellationHandler {
            try Task.checkCancellation()
            try await restoreSubscriptions()
            try Task.checkCancellation()
        } onCancel: {
            channel.close(promise: nil)
        }
        // Schedule the task to send a PING periodically
        let pingInterval = TimeAmount.nanoseconds(Int64(self.pingInterval * 1_000_000_000))
        self.pingTask = channel.eventLoop.scheduleRepeatedTask(
            initialDelay: pingInterval, delay: pingInterval
        ) { _ in
            Task { await self.sendPing() }
        }
    }

    private func connectToServer(s: URL) async throws {
        try Task.checkCancellation()
        guard let host = s.host, let port = s.port else {
            throw NatsError.ConnectError.invalidConfig("no url")
        }

        let handshake = Handshake(eventLoop: group.any())
        self.handshake.withLockedValue { $0 = handshake }
        defer { self.handshake.withLockedValue { $0 = nil } }

        let established = handshake.established.futureResult.map { channel in
            // Hand off on the event loop before processing any messages following PONG.
            self.batchBuffer = BatchBuffer(channel: channel)
            self.handshake.withLockedValue { $0 = nil }
        }

        let setup = Task {
            do {
                try Task.checkCancellation()
                // DNS can race several TCP candidates; only configure the winning connection.
                let channel = try await ClientBootstrap(
                    group: handshake.established.futureResult.eventLoop
                )
                .channelOption(ChannelOptions.autoRead, value: false)
                .channelOption(
                    ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1
                )
                .connectTimeout(.seconds(5))
                .connect(host: host, port: port).get()
                handshake.channel.withLockedValue { $0 = channel }
                self.channel = channel
                self.batchBuffer = nil
                handshake.established.futureResult.whenFailure { _ in
                    channel.close(promise: nil)
                }
                // WebSocket negotiation can close before this handler is added to the pipeline.
                channel.closeFuture.whenComplete { _ in
                    handshake.fail(NatsError.ClientError.connectionClosed)
                }

                // TCP has its own timeout; all remaining handshake phases share this deadline.
                let deadline = channel.eventLoop.scheduleTask(in: .seconds(5)) {
                    handshake.fail(NatsError.ConnectError.timeout)
                }
                handshake.established.futureResult.whenComplete { _ in deadline.cancel() }

                parseRemainder.withLockedValue { $0 = nil }
                _inputBuffer.withLockedValue { $0.clear() }
                try await channel.eventLoop.flatSubmit {
                    self.configureTransport(channel: channel, server: s, handshake: handshake)
                }.get()
                try await channel.setOption(ChannelOptions.autoRead, value: true).get()
                try await handshake.upgraded.futureResult.get()
                let info = try await handshake.info.futureResult.get()
                try Task.checkCancellation()
                self.serverInfo = info

                if (info.tlsRequired ?? false || self.requireTls) && !self.tlsFirst
                    && s.scheme != "wss"
                {
                    let tlsConfig = try makeTLSConfig()
                    let sslContext = try NIOSSLContext(configuration: tlsConfig)
                    let sslHandler = try NIOSSLClientHandler(
                        context: sslContext, serverHostname: s.host)
                    let sslHandlerBox = NIOLoopBoundBox.makeBoxSendingValue(
                        sslHandler, eventLoop: channel.eventLoop)
                    try await channel.eventLoop.submit {
                        try channel.pipeline.syncOperations.addHandler(
                            sslHandlerBox.value, position: .first)
                    }.get()
                }

                try await sendClientConnectInit(channel: channel, info: info)
            } catch {
                handshake.fail(error)
            }
        }
        handshake.established.futureResult.whenFailure { _ in setup.cancel() }

        do {
            try await withTaskCancellationHandler {
                try await established.get()
                await setup.value
                try Task.checkCancellation()
            } onCancel: {
                // Waiting on a NIO future does not observe Swift task cancellation on its own.
                handshake.fail(CancellationError())
            }

            guard let channel = handshake.channel.withLockedValue({ $0 }), channel.isActive else {
                throw NatsError.ClientError.connectionClosed
            }
            self.connectedUrl = s
        } catch {
            handshake.fail(error)
            await setup.value
            if let channel = handshake.channel.withLockedValue({ $0 }) {
                channel.close(promise: nil)
                try await channel.closeFuture.get()
            }
            throw error
        }
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

    private func sendClientConnectInit(channel: Channel, info: ServerInfo) async throws {
        var initialConnect = ConnectInfo(
            verbose: false, pedantic: false, userJwt: nil, nkey: "", name: "", echo: true,
            lang: self.lang, version: self.version, natsProtocol: .dynamic, tlsRequired: false,
            user: self.auth?.user ?? "", pass: self.auth?.password ?? "",
            authToken: self.auth?.token ?? "", headers: true, noResponders: true)

        if self.auth?.nkey != nil && self.auth?.nkeyPath != nil {
            throw NatsError.ConnectError.invalidConfig("cannot use both nkey and nkeyPath")
        }
        if let auth = self.auth, let credentialsPath = auth.credentialsPath {
            let credentials = try await URLSession.shared.data(from: credentialsPath).0
            guard let jwt = JwtUtils.parseDecoratedJWT(contents: credentials) else {
                throw NatsError.ConnectError.invalidConfig(
                    "failed to extract JWT from credentials file")
            }
            guard let nkey = JwtUtils.parseDecoratedNKey(contents: credentials) else {
                throw NatsError.ConnectError.invalidConfig(
                    "failed to extract NKEY from credentials file")
            }
            guard let nonce = info.nonce else {
                throw NatsError.ConnectError.invalidConfig("missing nonce")
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
                throw NatsError.ConnectError.invalidConfig("failed to read NKEY file")
            }
            let keypair = try KeyPair(
                seed: nkeyContent.trimmingCharacters(in: .whitespacesAndNewlines)
            )

            guard let nonce = info.nonce else {
                throw NatsError.ConnectError.invalidConfig("missing nonce")
            }
            let sig = try keypair.sign(input: nonce.data(using: .utf8)!)
            let base64sig = sig.base64EncodedURLSafeNotPadded()
            initialConnect.signature = base64sig
            initialConnect.nkey = keypair.publicKeyEncoded
        }
        if let nkey = self.auth?.nkey {
            let keypair = try KeyPair(seed: nkey)
            guard let nonce = info.nonce else {
                throw NatsError.ConnectError.invalidConfig("missing nonce")
            }
            let nonceData = nonce.data(using: .utf8)!
            let sig = try keypair.sign(input: nonceData)
            let base64sig = sig.base64EncodedURLSafeNotPadded()
            initialConnect.signature = base64sig
            initialConnect.nkey = keypair.publicKeyEncoded
        }
        try Task.checkCancellation()
        var buffer = channel.allocator.buffer(capacity: 512)
        try buffer.writeClientOp(.connect(initialConnect))
        try buffer.writeClientOp(.ping)
        // Handshake writes must observe socket errors, not merely acceptance by the batch buffer.
        try await channel.writeAndFlush(buffer)
    }

    private func configureTransport(
        channel: Channel, server: URL, handshake: Handshake
    ) -> EventLoopFuture<Void> {
        let upgradePromise = handshake.upgraded
        if self.requireTls && self.tlsFirst {
            upgradePromise.succeed(())
            do {
                let tlsConfig = try self.makeTLSConfig()
                let sslContext = try NIOSSLContext(
                    configuration: tlsConfig)
                let sslHandler = try NIOSSLClientHandler(
                    context: sslContext, serverHostname: server.host!)
                try channel.pipeline.syncOperations.addHandler(sslHandler)
                return channel.pipeline.addHandler(self)
            } catch {
                let tlsError = NatsError.ConnectError.tlsFailure(error)
                return channel.eventLoop.makeFailedFuture(tlsError)
            }
        }

        guard server.scheme == "ws" || server.scheme == "wss" else {
            upgradePromise.succeed(())
            return channel.pipeline.addHandler(self)
        }

        let httpUpgradeRequestHandler = HTTPUpgradeRequestHandler(
            host: server.host ?? "localhost",
            path: server.path,
            query: server.query,
            headers: HTTPHeaders(),  // TODO (mtmk): pass in from client options
            upgradePromise: upgradePromise)
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
                do {
                    try channel.pipeline.syncOperations.addHandler(wsh)
                    try channel.pipeline.syncOperations.addHandler(
                        WebSocketByteBufferCodec())
                } catch {
                    return channel.eventLoop.makeFailedFuture(error)
                }
                return channel.pipeline.addHandler(self)
            }
        )

        let config: NIOHTTPClientUpgradeConfiguration = (
            upgraders: [websocketUpgrader],
            completionHandler: { context in
                upgradePromise.succeed(())
                channel.pipeline.syncOperations.removeHandler(
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
                let tlsError = NatsError.ConnectError.tlsFailure(error)
                upgradePromise.fail(tlsError)
                return channel.eventLoop.makeFailedFuture(tlsError)
            }
        }

        return channel.pipeline.addHTTPClientHandlers(
            leftOverBytesStrategy: .forwardBytes,
            withClientUpgrade: config
        ).flatMap {
            do {
                try channel.pipeline.syncOperations.addHandler(
                    httpUpgradeRequestHandlerBox.value)
                return channel.eventLoop.makeSucceededFuture(())
            } catch {
                return channel.eventLoop.makeFailedFuture(error)
            }
        }
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
        try await stop(state: .closed)
    }

    func suspend() async throws {
        try await stop(state: .suspended)
    }

    private func stop(state: NatsState) async throws {
        let task = try lifecycle.withLockedValue { lifecycle in
            guard lifecycle.state != .closed else {
                throw NatsError.ClientError.connectionClosed
            }
            guard !lifecycle.stopping else {
                throw NatsError.ClientError.invalidConnection("connection is stopping")
            }
            // Stop admitting success or new work before cancelling the active attempt.
            lifecycle.state = state
            lifecycle.stopping = true
            return lifecycle.connectionTask
        }
        var stopped = false
        defer {
            lifecycle.withLockedValue {
                $0.connectionTask = nil
                $0.stopping = false
            }
            if stopped { fire(state == .closed ? .closed : .suspended) }
        }

        task?.cancel()
        _ = await task?.result
        try await closeChannel()
        stopped = true
    }

    private func closeChannel() async throws {
        pingTask?.cancel()
        guard let channel = self.channel else { return }

        let closing: EventLoopFuture<Void> = channel.close()
        try await channel.closeFuture.get()
        do {
            try await closing.get()
        } catch ChannelError.alreadyClosed {
            // The handshake or peer may have closed the socket first.
        }
    }

    func resume() async throws {
        try lifecycle.withLockedValue { lifecycle in
            guard lifecycle.state != .closed else {
                throw NatsError.ClientError.connectionClosed
            }
            guard lifecycle.state == .suspended, !lifecycle.stopping else {
                throw NatsError.ClientError.invalidConnection(
                    "unable to resume connection - connection is not in suspended state")
            }
            lifecycle.state = .connecting
            lifecycle.connectionTask = makeConnectionTask(retrying: true)
        }
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

    func channelInactive(context: ChannelHandlerContext) {
        guard context.channel === self.channel else { return }

        logger.debug("TCP channel inactive")

        handshake.withLockedValue { $0 }?.fail(NatsError.ClientError.connectionClosed)

        handleDisconnect()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        guard context.channel === self.channel else { return }

        logger.debug("Encountered error on the channel: \(error)")

        if let handshake = handshake.withLockedValue({ $0 }) {
            // The connection task reports handshake failures once, after cleanup.
            handshake.fail(error)
            context.close(promise: nil)
            return
        }

        context.close(promise: nil)

        if let natsErr = error as? NatsErrorProtocol {
            self.fire(.error(natsErr))
        } else {
            logger.error("unexpected error: \(error)")
        }
    }

    private func handleDisconnect() {
        lifecycle.withLockedValue { lifecycle in
            guard lifecycle.state == .connected else { return }
            lifecycle.state = .disconnected

            // A task still finishing its connected notification performs this handoff in defer.
            guard lifecycle.connectionTask == nil else { return }
            lifecycle.state = .connecting
            lifecycle.connectionTask = makeConnectionTask(retrying: true, notifyDisconnect: true)
        }
    }

    // Called while holding the lifecycle lock, so shutdown always sees the registered task.
    private func makeConnectionTask(
        retrying: Bool, notifyDisconnect: Bool = false
    ) -> Task<Void, Error> {
        Task {
            var connected = false
            defer {
                lifecycle.withLockedValue { lifecycle in
                    // stop() keeps ownership until both the task and socket have drained.
                    guard !lifecycle.stopping else { return }
                    if connected && lifecycle.state == .disconnected {
                        lifecycle.state = .connecting
                        lifecycle.connectionTask = makeConnectionTask(
                            retrying: true, notifyDisconnect: true)
                    } else {
                        lifecycle.connectionTask = nil
                        if lifecycle.state == .connecting { lifecycle.state = .disconnected }
                    }
                }
            }

            // Drain any previous socket before reusing the handler for a new attempt.
            pingTask?.cancel()
            if let channel = self.channel {
                channel.close(promise: nil)
                try await channel.closeFuture.get()
            }
            if notifyDisconnect { fire(.disconnected) }

            while !retrying || maxReconnects == nil || reconnectAttempts < maxReconnects! {
                do {
                    try Task.checkCancellation()
                    try await connectToServers()
                    guard let channel = self.channel else {
                        throw NatsError.ClientError.connectionClosed
                    }
                    try await channel.eventLoop.submit {
                        try self.lifecycle.withLockedValue { lifecycle in
                            guard lifecycle.state == .connecting,
                                lifecycle.connectionTask?.isCancelled == false
                            else { throw CancellationError() }
                            guard channel.isActive else {
                                throw NatsError.ClientError.connectionClosed
                            }
                            lifecycle.state = .connected
                        }
                        logger.debug("connection established")
                        self.fire(.connected)
                    }.get()
                    self.reconnectAttempts = 0
                    connected = true
                    return
                } catch {
                    do {
                        try await closeChannel()
                    } catch {
                        logger.warning("Error closing failed connection: \(error)")
                    }
                    if error is CancellationError || Task.isCancelled {
                        throw CancellationError()
                    }
                    guard retrying else { throw error }

                    fire(.error((error as? NatsErrorProtocol) ?? NatsError.ClientError.io(error)))
                    logger.debug("Could not reconnect: \(error)")
                }
            }

            let exhausted = lifecycle.withLockedValue { lifecycle in
                guard lifecycle.state == .connecting else { return false }
                lifecycle.state = .closed
                return true
            }
            if exhausted {
                logger.error("Could not reconnect; maxReconnects exceeded")
                fire(.closed)
            }
        }
    }

    private func restoreSubscriptions() async throws {
        let subsToRestore = subscriptions.withLockedValue { Array($0) }
        for (sid, sub) in subsToRestore {
            try await resubscribe(sid: sid, sub)
        }
    }

    private func resubscribe(sid: UInt64, _ sub: NatsSubscription) async throws {
        guard let max = sub.max else {
            try await write(operation: ClientOp.subscribe((sid, sub.subject, sub.queue)))
            return
        }
        let receivedSoFar = sub.received
        guard receivedSoFar < max else { return }
        try await write(operation: ClientOp.subscribe((sid, sub.subject, sub.queue)))
        try await write(operation: ClientOp.unsubscribe((sid: sid, max: max - receivedSoFar)))
    }

    func write(operation: ClientOp) async throws {
        guard let buffer = self.batchBuffer else {
            throw NatsError.ClientError.invalidConnection("not connected")
        }
        do {
            try await buffer.writeMessage(operation)
        } catch {
            throw NatsError.ClientError.io(error)
        }
    }

    internal var subscriptionCount: Int {
        subscriptions.withLockedValue { $0.count }
    }

    internal func subscribe(
        _ subject: String, queue: String? = nil, capacity: UInt64? = nil
    ) async throws -> NatsSubscription {
        let sid = self.subscriptionCounter.wrappingIncrementThenLoad(
            ordering: AtomicUpdateOrdering.relaxed)
        let sub: NatsSubscription
        if let capacity {
            sub = try NatsSubscription(
                sid: sid, subject: subject, queue: queue, capacity: max(1, capacity), conn: self)
        } else {
            sub = try NatsSubscription(sid: sid, subject: subject, queue: queue, conn: self)
        }

        // Add subscription BEFORE sending command to avoid race condition
        subscriptions.withLockedValue { $0[sid] = sub }

        do {
            try await write(operation: ClientOp.subscribe((sid, subject, queue)))
        } catch {
            // Remove subscription if subscribe command fails
            _ = subscriptions.withLockedValue { $0.removeValue(forKey: sid) }
            throw error
        }

        return sub
    }

    internal func unsubscribe(sub: NatsSubscription, max: UInt64?) async throws {
        if let max, sub.delivered < max {
            // if max is set and the sub has not yet reached it, send unsub with max set
            // and do not remove the sub from connection
            try await write(operation: ClientOp.unsubscribe((sid: sub.sid, max: max)))
            sub.max = max
        } else {
            // if max is not set or the subscription received at least as many
            // messages as max, send unsub command without max and remove sub from connection
            try await write(operation: ClientOp.unsubscribe((sid: sub.sid, max: nil)))
            self.removeSub(sub: sub)
        }
    }

    internal func removeSub(sub: NatsSubscription) {
        _ = subscriptions.withLockedValue { $0.removeValue(forKey: sub.sid) }
        sub.complete()
    }
}

extension ConnectionHandler {

    internal func fire(_ event: NatsEvent) {
        let eventKind = event.kind()
        let handlerStore = self.eventHandlerStore.withLockedValue { $0[eventKind] }
        guard let handlerStore = handlerStore else { return }

        for handler in handlerStore {
            handler.handler(event)
        }
    }

    internal func addListeners(
        for events: [NatsEventKind], using handler: @escaping @Sendable (NatsEvent) -> Void
    ) -> String {

        let id = String.hash()

        for event in events {
            self.eventHandlerStore.withLockedValue { store in
                if store[event] == nil {
                    store[event] = []
                }
                store[event]?.append(NatsEventHandler(lid: id, handler: handler))
            }
        }

        return id

    }

    internal func removeListener(_ id: String) {

        for event in NatsEventKind.all {
            self.eventHandlerStore.withLockedValue { store in
                if let handlerStore = store[event] {
                    store[event] = handlerStore.filter { $0.listenerId != id }
                }
            }
        }

    }

}

/// Nats events
public enum NatsEventKind: String, Sendable {
    case connected = "connected"
    case disconnected = "disconnected"
    case closed = "closed"
    case suspended = "suspended"
    case lameDuckMode = "lameDuckMode"
    case error = "error"
    static let all = [connected, disconnected, closed, lameDuckMode, error]
}

public enum NatsEvent: Sendable {
    case connected
    case disconnected
    case suspended
    case closed
    case lameDuckMode
    case error(NatsErrorProtocol)

    public func kind() -> NatsEventKind {
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

internal struct NatsEventHandler: Sendable {
    let listenerId: String
    let handler: @Sendable (NatsEvent) -> Void
    init(lid: String, handler: @escaping @Sendable (NatsEvent) -> Void) {
        self.listenerId = lid
        self.handler = handler
    }
}
