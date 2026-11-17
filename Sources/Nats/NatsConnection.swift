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
    internal let retryOnFailedConnect: Bool
    private let pool: NIOLockedValueBox<ServerPool>
    /// The configured servers, then the discovered ones.
    internal var servers: [Server] {
        pool.withLockedValue { $0.servers }
    }
    /// The server of the current connection attempt or connection, and whether that
    /// connection has TLS. The servers its INFOs report are learned on it.
    private struct CurrentServer: Sendable {
        let server: Server
        var tls: Bool
    }
    private let current = NIOLockedValueBox<CurrentServer?>(nil)
    // nanoseconds representation of TimeInterval
    private let reconnectWait: UInt64
    private let maxReconnects: Int?
    private let retainServersOrder: Bool
    private let pingInterval: TimeInterval
    /// The setting that asks for TLS, as messages name it, or nil when none does. When one
    /// does, every connection to every server uses TLS.
    private let tlsAskedBy: String?
    /// Why no connection attempt can succeed with these settings, or nil.
    private let configurationError: String?
    private let tlsFirst: Bool
    private let rootCertificate: URL?
    private let clientCertificate: URL?
    private let clientKey: URL?

    typealias InboundIn = ByteBuffer
    private let state = NIOLockedValueBox(NatsState.pending)
    private let subscriptions = NIOLockedValueBox([UInt64: NatsSubscription]())

    // Helper methods for state access
    internal var currentState: NatsState {
        state.withLockedValue { $0 }
    }

    internal func setState(_ newState: NatsState) {
        state.withLockedValue { $0 = newState }
    }

    private let subscriptionCounter = ManagedAtomic<UInt64>(0)
    private let _serverInfo = NIOLockedValueBox<ServerInfo?>(nil)
    private var serverInfo: ServerInfo? {
        get { _serverInfo.withLockedValue { $0 } }
        set { _serverInfo.withLockedValue { $0 = newValue } }
    }

    private let auth: Auth?
    private let parseRemainder = NIOLockedValueBox<Data?>(nil)

    /// What the handler does with the bytes of the current connection attempt.
    private enum ReadPhase {
        /// The first line from the server must be INFO.
        case expectingInfo
        /// INFO was read. What follows is kept unparsed until the attempt has decided whether
        /// to upgrade to TLS, and dropped when it does.
        case holdingAfterInfo
        /// Lines are parsed and handled.
        case processing
    }
    private let readPhase = NIOLockedValueBox(ReadPhase.expectingInfo)
    /// The channel of the attempt whose lines are handled.
    private let attemptChannel = NIOLockedValueBox<ObjectIdentifier?>(nil)
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
    private let capturedConnectionError = NIOLockedValueBox<Error?>(nil)

    private let _reconnectTask = NIOLockedValueBox<Task<(), Error>?>(nil)
    private var reconnectTask: Task<(), Error>? {
        get { _reconnectTask.withLockedValue { $0 } }
        set { _reconnectTask.withLockedValue { $0 = newValue } }
    }

    private let group: MultiThreadedEventLoopGroup

    private let serverInfoContinuation = NIOLockedValueBox<CheckedContinuation<ServerInfo, Error>?>(
        nil)
    private let connectionEstablishedContinuation = NIOLockedValueBox<
        CheckedContinuation<Void, Error>?
    >(nil)

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
        tlsOptional: Bool, clientCertificate: URL?, clientKey: URL?,
        rootCertificate: URL?, retryOnFailedConnect: Bool
    ) {
        self.pool = NIOLockedValueBox(ServerPool(configured: urls))
        self.group = .singleton
        self._inputBuffer = NIOLockedValueBox(allocator.buffer(capacity: 1024))
        self.reconnectWait = UInt64(reconnectWait * 1_000_000_000)
        self.maxReconnects = maxReconnects
        self.retainServersOrder = retainServersOrder
        self.auth = auth
        self.pingInterval = pingInterval
        let tlsAskedBy = Self.tlsAskedBy(
            urls: urls, requireTls: requireTls, tlsFirst: tlsFirst, tlsOptional: tlsOptional,
            rootCertificate: rootCertificate, clientCertificate: clientCertificate)
        self.tlsAskedBy = tlsAskedBy
        self.configurationError = Self.configurationError(
            urls: urls, requireTls: requireTls, tlsFirst: tlsFirst, tlsOptional: tlsOptional,
            tlsAskedBy: tlsAskedBy)
        self.tlsFirst = tlsFirst
        self.clientCertificate = clientCertificate
        self.clientKey = clientKey
        self.rootCertificate = rootCertificate
        self.retryOnFailedConnect = retryOnFailedConnect
    }

    /// The setting that asks for TLS for every server, as messages name it: an option by its
    /// name, or the first `tls://` or `wss://` URL of the list. Nil when nothing asks for TLS,
    /// as with TLS optional.
    private static func tlsAskedBy(
        urls: [URL], requireTls: Bool, tlsFirst: Bool, tlsOptional: Bool, rootCertificate: URL?,
        clientCertificate: URL?
    ) -> String? {
        if tlsOptional {
            return nil
        } else if tlsFirst {
            return "withTlsFirst()"
        } else if requireTls {
            return "requireTls()"
        } else if let url = urls.first(where: { ["tls", "wss"].contains($0.lowercasedScheme) }) {
            return "the URL \(url.quoted)"
        } else if rootCertificate != nil {
            return "rootCertificates(_:)"
        } else if clientCertificate != nil {
            return "clientCertificate(_:_:)"
        }
        return nil
    }

    private static func configurationError(
        urls: [URL], requireTls: Bool, tlsFirst: Bool, tlsOptional: Bool, tlsAskedBy: String?
    ) -> String? {
        if tlsOptional && tlsFirst {
            return "tlsOptional() cannot be set together with withTlsFirst()"
        } else if tlsOptional && requireTls {
            return "tlsOptional() cannot be set together with requireTls()"
        } else if let tlsAskedBy, let url = urls.first(where: { $0.lowercasedScheme == "ws" }) {
            // A websocket gets TLS from its scheme, before the first byte: a ws:// URL cannot
            // be upgraded, and dialing it with TLS would change the URL the user wrote.
            return wsUrlRefusal(url, tlsAskedBy: tlsAskedBy)
        }
        return nil
    }

    /// Why the `ws://` URL `url` is refused while `tlsAskedBy` asks for TLS.
    private static func wsUrlRefusal(_ url: URL, tlsAskedBy: String) -> String {
        "\(tlsAskedBy) asks for TLS, but \(url.quoted) is a ws:// URL; wss:// is the TLS form"
    }

    /// Throws the configuration error of the client's settings, if they have one. No connection
    /// attempt can fix it, so ``NatsClient/connect()`` reports it before the first.
    internal func checkConfiguration() throws {
        if let configurationError {
            throw NatsError.ConnectError.invalidConfig(configurationError)
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {

        let state: NatsState = self.currentState

        guard state == .connected || state == .pending || state == .connecting else {
            logger.debug("Ignoring channelRead. Current state (\(state)) does not allow reading.")
            return
        }

        guard !isFromAnotherChannel(context) else { return }
        var byteBuffer = self.unwrapInboundIn(data)
        _ = _inputBuffer.withLockedValue { $0.writeBuffer(&byteBuffer) }
    }

    /// Once the attempt's lines are handled, what another channel reads (e.g. a previous
    /// connection that is still closing) is dropped.
    private func isFromAnotherChannel(_ context: ChannelHandlerContext) -> Bool {
        readPhase.withLockedValue { $0 } == .processing
            && attemptChannel.withLockedValue { $0 } != ObjectIdentifier(context.channel)
    }

    func channelReadComplete(context: ChannelHandlerContext) {

        let state: NatsState = self.currentState
        guard state == .connected || state == .pending || state == .connecting else {
            _inputBuffer.withLockedValue { $0.clear() }
            parseRemainder.withLockedValue { $0 = nil }
            return
        }
        guard !isFromAnotherChannel(context) else { return }

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

        if readPhase.withLockedValue({ $0 }) == .expectingInfo {
            _inputBuffer.withLockedValue { $0.clear() }
            guard let afterInfo = readExpectedInfo(inputChunk, context: context) else {
                return
            }
            inputChunk = afterInfo
        }

        // Checked in one step: the TLS upgrade drops what is kept under the same lock, and a
        // later attempt may have reset the phase since the checks above.
        let handles = readPhase.withLockedValue { phase in
            if phase == .holdingAfterInfo {
                _inputBuffer.withLockedValue { $0.clear() }
                parseRemainder.withLockedValue { $0 = inputChunk }
            }
            return phase == .processing
                && attemptChannel.withLockedValue { $0 } == ObjectIdentifier(context.channel)
        }
        guard handles else { return }

        processInput(inputChunk, context: context)
    }

    /// Parses the bytes read from the server and handles the operations in them.
    private func processInput(_ inputChunk: Data, context: ChannelHandlerContext) {
        let state: NatsState = self.currentState

        // Once connected, an INFO that does not parse is skipped and reported, and the
        // connection stays up. During a connection attempt it fails the parse.
        let reportSkippedInfo: ((Error) -> Void)? =
            state == .connected
            ? { error in
                logger.warning("skipping INFO: \(error)")
                if let error = error as? NatsErrorProtocol {
                    self.fire(.error(error))
                }
            } : nil

        let parseResult: (ops: [ServerOp], remainder: Data?)
        do {
            parseResult = try inputChunk.parseOutMessages(
                onInfoThatDoesNotParse: reportSkippedInfo)
        } catch {
            // if parsing throws an error, clear buffer and remainder, then reconnect
            _inputBuffer.withLockedValue { $0.clear() }
            parseRemainder.withLockedValue { $0 = nil }

            if self.currentState != .closed && self.currentState != .suspended {
                // this handler is the last in the pipeline, so an error fired from here would
                // reach no handler: handle it here, which reports it and closes the connection
                self.errorCaught(context: context, error: error)
            }

            return
        }
        if let remainder = parseResult.remainder {
            parseRemainder.withLockedValue { $0 = remainder }
        }
        for op in parseResult.ops {
            // CONNECT is answered by PONG and refused by -ERR. Any other line, such as an INFO
            // the server sends first, is handled as usual.
            switch op {
            case .pong, .error:
                let connEstablishedCont = connectionEstablishedContinuation.withLockedValue {
                    cont in
                    let toResume = cont
                    cont = nil
                    return toResume
                }
                if let continuation = connEstablishedCont {
                    if case .error(let err) = op {
                        continuation.resume(throwing: err)
                    } else {
                        logger.debug("conn established")
                        continuation.resume()
                    }
                    continue
                }
            default:
                break
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

    /// Reads the first line of a connection attempt, which must be INFO, and hands the INFO to
    /// the attempt. Any other line, or an INFO that does not parse, fails the attempt, is
    /// reported as an error event, and closes its channel. Returns the bytes after the line, or
    /// nil when the line is not complete yet or the attempt failed.
    private func readExpectedInfo(_ chunk: Data, context: ChannelHandlerContext) -> Data? {
        guard let lineEnd = chunk.range(of: Data([UInt8(ascii: "\r"), UInt8(ascii: "\n")])) else {
            parseRemainder.withLockedValue { $0 = chunk }
            return nil
        }
        let line = chunk[chunk.startIndex..<lineEnd.lowerBound]
        let result: Result<ServerInfo, NatsError.ProtocolError>
        if line.getMessageType() != .info {
            result = .failure(
                NatsError.ProtocolError.parserFailure(
                    "expected INFO as the first line from the server"))
        } else {
            do {
                result = .success(try ServerInfo.parse(data: line))
            } catch {
                result = .failure(
                    NatsError.ProtocolError.parserFailure("INFO does not parse: \(error)"))
            }
        }

        let continuation = serverInfoContinuation.withLockedValue { cont in
            let toResume = cont
            cont = nil
            return toResume
        }
        switch result {
        case .success(let info):
            logger.debug("server info")
            readPhase.withLockedValue { $0 = .holdingAfterInfo }
            continuation?.resume(returning: info)
            return Data(chunk[lineEnd.upperBound...])
        case .failure(let error):
            logger.debug("connection attempt failed: \(error)")
            context.close(promise: nil)
            continuation?.resume(throwing: error)
            fire(.error(error))
            return nil
        }
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
        self.setState(.connecting)
        var servers = self.servers
        if !self.retainServersOrder {
            servers.shuffle()
        }
        var lastErr: Error?

        // if there are more reconnect attempts than the number of servers,
        // we are after the initial connect, so sleep between servers
        let shouldSleep = self.reconnectAttempts >= servers.count
        for server in servers {
            // a discovered server as the pool has it now: it may have gained TLS in this round
            guard let s = self.pool.withLockedValue({ $0.latest(server) }) else {
                continue
            }
            if let maxReconnects {
                if reconnectAttempts > 0 && reconnectAttempts >= maxReconnects {
                    throw NatsError.ClientError.maxReconnects
                }
            }
            self.reconnectAttempts += 1
            if shouldSleep {
                try await Task.sleep(nanoseconds: self.reconnectWait)
            }

            do {
                try await connectToServer(s)
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
            self.state.withLockedValue { $0 = .disconnected }
            switch lastErr {
            case let error as ChannelError:
                serverInfoContinuation.withLockedValue { $0 = nil }
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
            case let err as NatsError.CertificateNameMismatch:
                throw NatsError.ConnectError.tlsFailure(err)
            case let err as NatsError.ServerError:
                throw err
            case let err as NatsError.ConnectError:
                throw err
            default:
                throw NatsError.ConnectError.io(lastErr)
            }
        }
        // Restore before clearing the counter so a restore failure stays bounded by
        // maxReconnects (no-op on the initial connect: the set is empty).
        try await restoreSubscriptions()
        self.reconnectAttempts = 0
        guard let channel = self.channel else {
            throw NatsError.ClientError.internalError("empty channel")
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

    private func connectToServer(_ server: Server) async throws {
        let s = server.url
        // the first line of every attempt must be INFO
        readPhase.withLockedValue { $0 = .expectingInfo }
        capturedConnectionError.withLockedValue { $0 = nil }
        // a websocket with TLS and TLS first have TLS from the start
        let tls = s.lowercasedScheme == "wss" || (self.tlsFirst && !s.isWebsocket)
        current.withLockedValue { $0 = CurrentServer(server: server, tls: tls) }
        parseRemainder.withLockedValue { $0 = nil }
        _inputBuffer.withLockedValue { $0.clear() }
        var infoTask: Task<(), Never>? = nil
        // the channel this attempt dialed
        let dialed = NIOLockedValueBox<Channel?>(nil)
        do {
            let info: ServerInfo = try await withCheckedThrowingContinuation { continuation in
                serverInfoContinuation.withLockedValue { $0 = continuation }
                infoTask = Task {
                    await withTaskCancellationHandler {
                        do {
                            let (bootstrap, upgradePromise) = self.bootstrapConnection(to: s)

                            guard let host = s.host, let port = s.port else {
                                upgradePromise.succeed()  // avoid promise leaks
                                throw NatsError.ConnectError.invalidConfig("no url")
                            }

                            let connect = bootstrap.connect(host: host, port: port)
                            connect.cascadeFailure(to: upgradePromise)
                            let dialedChannel = try await connect.get()
                            dialed.withLockedValue { $0 = dialedChannel }
                            self.channel = dialedChannel

                            guard self.channel != nil else {
                                upgradePromise.succeed()  // avoid promise leaks
                                throw NatsError.ClientError.internalError("empty channel")
                            }

                            try await upgradePromise.futureResult.get()
                        } catch {
                            let continuationToResume: CheckedContinuation<ServerInfo, Error>? = self
                                .serverInfoContinuation.withLockedValue { cont in
                                    guard let c = cont else { return nil }
                                    cont = nil
                                    return c
                                }
                            if let continuation = continuationToResume {
                                continuation.resume(throwing: error)
                            }
                        }
                    } onCancel: {
                        logger.debug("Connection task cancelled")
                        // Clean up resources
                        if let channel = self.channel {
                            channel.close(mode: .all, promise: nil)
                            self.channel = nil
                        }
                        self.batchBuffer = nil

                        let continuationToResume: CheckedContinuation<ServerInfo, Error>? = self
                            .serverInfoContinuation.withLockedValue { cont in
                                guard let c = cont else { return nil }
                                cont = nil
                                return c
                            }
                        if let continuation = continuationToResume {
                            continuation.resume(throwing: NatsError.ClientError.cancelled)
                        }
                    }
                }
            }

            await infoTask?.value
            self.serverInfo = info
            guard let channel = dialed.withLockedValue({ $0 }) else {
                throw NatsError.ClientError.internalError("empty channel")
            }
            try await completeConnection(to: server, info: info, on: channel)
        } catch {
            // The wait for INFO also ends when another channel closes or reads a line, and the
            // dial of this attempt may still be running then: wait for it. Then close the channel
            // it dialed and wait until it is closed, so that nothing it receives later reaches the
            // next attempt or the connection that follows.
            await infoTask?.value
            if let channel = dialed.withLockedValue({ $0 }) {
                channel.close(promise: nil)
                try? await channel.closeFuture.get()
            }
            throw error
        }
        self.connectedUrl = s
    }

    /// Upgrades the attempt's channel to TLS when needed, then sends CONNECT and waits for the
    /// server's answer.
    private func completeConnection(
        to server: Server, info: ServerInfo, on channel: Channel
    ) async throws {
        let s = server.url
        if try upgradesToTls(server: server, info: info) {
            let sslHandler = try makeSSLHandler(for: s)
            // NIOLoopBoundBox.makeBoxSendingValue can be created off the event loop
            // (takes ownership via `sending`), unlike NIOLoopBound which requires
            // already being on the event loop at construction time.
            let sslHandlerBox = NIOLoopBoundBox.makeBoxSendingValue(
                sslHandler, eventLoop: channel.eventLoop)
            do {
                try await channel.eventLoop.submit {
                    // a TLS handler added to a closed connection never starts its handshake
                    guard channel.isActive else {
                        throw NatsError.ClientError.connectionClosed
                    }
                    // Bytes that arrived after INFO were read before TLS: drop them in the same
                    // event loop task that adds the TLS handler.
                    self._inputBuffer.withLockedValue { $0.clear() }
                    self.attemptChannel.withLockedValue { $0 = ObjectIdentifier(channel) }
                    self.readPhase.withLockedValue { phase in
                        self.parseRemainder.withLockedValue { $0 = nil }
                        phase = .processing
                    }
                    self.current.withLockedValue { $0?.tls = true }
                    try channel.pipeline.syncOperations.addHandlers(
                        [sslHandlerBox.value, self.makeNameCheck(for: s)], position: .first)
                    // writes reach this connection only from here on, through TLS
                    self.batchBuffer = BatchBuffer(channel: channel)
                }.get()
            } catch {
                // the TLS handshake could not start, e.g. the server closed the connection
                throw NatsError.ConnectError.tlsFailure(error)
            }
        } else {
            try await channel.eventLoop.submit {
                guard channel.isActive else {
                    throw NatsError.ClientError.connectionClosed
                }
                // No TLS upgrade follows: handle the bytes that arrived after INFO now.
                self.attemptChannel.withLockedValue { $0 = ObjectIdentifier(channel) }
                self.readPhase.withLockedValue { $0 = .processing }
                // before the held bytes, so that a reply to them goes to this connection
                self.batchBuffer = BatchBuffer(channel: channel)
                let held = self.parseRemainder.withLockedValue { value in
                    let current = value
                    value = nil
                    return current
                }
                if let held, !held.isEmpty {
                    let context = try channel.pipeline.syncOperations.context(handler: self)
                    self.processInput(held, context: context)
                }
            }.get()
        }

        try await sendClientConnectInit(on: channel)
    }

    /// Whether the attempt upgrades its connection to `server` to TLS after `info`. A `wss://`
    /// connection and a TLS-first connection have TLS from the start, and a `ws://` connection
    /// is never upgraded: its TLS would start below the websocket framing after the upgrade.
    /// While TLS is asked for, ``NatsClient/connect()`` refuses a `ws://` URL of the list, so
    /// no server is dialed over `ws://`: a discovered server gets `ws://` only from a `ws://`
    /// connection. Throws when TLS is asked for and `info` offers none.
    private func upgradesToTls(server: Server, info: ServerInfo) throws -> Bool {
        let url = server.url
        if url.isWebsocket || self.tlsFirst {
            return false
        }
        // With TLS optional nothing asks for TLS, but a tls:// URL, and a server learned on a
        // connection with TLS, still get it.
        let serverAsksForTls: String?
        if server.learnedOverTls {
            serverAsksForTls = "\(url.quoted) was discovered on a connection with TLS"
        } else if url.lowercasedScheme == "tls" {
            serverAsksForTls = "the URL \(url.quoted) asks for TLS"
        } else {
            serverAsksForTls = nil
        }
        guard let askedBy = self.tlsAskedBy.map({ "\($0) asks for TLS" }) ?? serverAsksForTls
        else {
            return info.tlsRequired ?? false
        }
        // An INFO with neither tls_required nor tls_available says that the server offers no
        // TLS: instead of starting a handshake, say which server it is and what asked for TLS.
        guard info.tlsRequired ?? false || info.tlsAvailable ?? false else {
            var message =
                self.tlsAskedBy == nil && server.learnedOverTls
                ? "\(askedBy), but does not offer it"
                : "\(askedBy), but \(url.quoted) does not offer it"
            // tlsOptional() does not take TLS away from a server learned over TLS
            if self.tlsAskedBy != nil && !server.learnedOverTls {
                message += "; tlsOptional() leaves TLS to each server"
            }
            throw NatsError.ConnectError.tlsFailure(NatsError.TlsNotOffered(description: message))
        }
        return true
    }

    private func makeSSLHandler(for server: URL) throws -> NIOSSLClientHandler {
        let sslContext = try NIOSSLContext(configuration: makeTLSConfig())
        return try NIOSSLClientHandler(
            context: sslContext, serverHostname: tlsServerName(for: server))
    }

    /// Checks that the certificate of `server` is issued for its host. It goes right after the
    /// TLS handler.
    private func makeNameCheck(for server: URL) -> TlsNameCheckHandler {
        TlsNameCheckHandler(server: server.quoted, name: server.host ?? "")
    }

    /// The name sent as SNI to `server`: its host, or nil for an IP address, which SNI cannot
    /// carry.
    private func tlsServerName(for server: URL) -> String? {
        guard let host = server.host, (try? SocketAddress(ipAddress: host, port: 0)) == nil else {
            return nil
        }
        return host
    }

    private func makeTLSConfig() throws -> TLSConfiguration {
        var tlsConfiguration =
            TLSConfiguration.makeClientConfiguration()
        // The name is checked by TlsNameCheckHandler.
        tlsConfiguration.certificateVerification = .noHostnameVerification
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

    private func sendClientConnectInit(on channel: Channel) async throws {
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
            guard let nonce = self.serverInfo?.nonce else {
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

            guard let nonce = self.serverInfo?.nonce else {
                throw NatsError.ConnectError.invalidConfig("missing nonce")
            }
            let sig = try keypair.sign(input: nonce.data(using: .utf8)!)
            let base64sig = sig.base64EncodedURLSafeNotPadded()
            initialConnect.signature = base64sig
            initialConnect.nkey = keypair.publicKeyEncoded
        }
        if let nkey = self.auth?.nkey {
            let keypair = try KeyPair(seed: nkey)
            guard let nonce = self.serverInfo?.nonce else {
                throw NatsError.ConnectError.invalidConfig("missing nonce")
            }
            let nonceData = nonce.data(using: .utf8)!
            let sig = try keypair.sign(input: nonceData)
            let base64sig = sig.base64EncodedURLSafeNotPadded()
            initialConnect.signature = base64sig
            initialConnect.nkey = keypair.publicKeyEncoded
        }
        let connect = initialConnect
        // this continuation can throw NatsError.ServerError if server responds with
        // -ERR to client connect (e.g. auth error)
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                connectionEstablishedContinuation.withLockedValue { $0 = continuation }
                // channelInactive fails the continuation only once it is set: a connection that
                // closed before is found here, on its event loop
                channel.eventLoop.execute {
                    guard !channel.isActive else { return }
                    let toResume = self.connectionEstablishedContinuation.withLockedValue { cont in
                        defer { cont = nil }
                        return cont
                    }
                    toResume?.resume(throwing: self.takeConnectionError())
                }
                Task.detached {
                    do {
                        try await self.write(operation: ClientOp.connect(connect))
                        try await self.write(operation: ClientOp.ping)
                        self.channel?.flush()
                    } catch {
                        let continuationToResume: CheckedContinuation<Void, Error>? = self
                            .connectionEstablishedContinuation.withLockedValue { cont in
                                guard let c = cont else { return nil }
                                cont = nil
                                return c
                            }
                        if let continuation = continuationToResume {
                            continuation.resume(throwing: error)
                        }
                    }
                }
            }
        } onCancel: {
            logger.debug("Client connect initialization cancelled")
            // Clean up resources
            if let channel = self.channel {
                channel.close(mode: .all, promise: nil)
                self.channel = nil
            }
            self.batchBuffer = nil

            let continuationToResume: CheckedContinuation<Void, Error>? = self
                .connectionEstablishedContinuation.withLockedValue { cont in
                    guard let c = cont else { return nil }
                    cont = nil
                    return c
                }
            if let continuation = continuationToResume {
                continuation.resume(throwing: NatsError.ClientError.cancelled)
            }
        }
    }

    private func bootstrapConnection(
        to server: URL
    ) -> (ClientBootstrap, EventLoopPromise<Void>) {
        let upgradePromise: EventLoopPromise<Void> = self.group.any().makePromise(of: Void.self)
        let bootstrap = ClientBootstrap(group: self.group)
            .channelOption(
                ChannelOptions.socket(
                    SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR),
                value: 1
            )
            .channelInitializer { channel in
                // A websocket URL gets TLS below the websocket, from its scheme.
                if self.tlsFirst && !server.isWebsocket {
                    upgradePromise.succeed(())
                    do {
                        let sslHandler = try self.makeSSLHandler(for: server)
                        do {
                            try channel.pipeline.syncOperations.addHandlers(
                                sslHandler, self.makeNameCheck(for: server))
                        } catch {
                            let tlsError = NatsError.ConnectError.tlsFailure(error)
                            return channel.eventLoop.makeFailedFuture(tlsError)
                        }
                        return channel.pipeline.addHandler(self)
                    } catch {
                        let tlsError = NatsError.ConnectError.tlsFailure(error)
                        return channel.eventLoop.makeFailedFuture(tlsError)
                    }
                } else {
                    if server.isWebsocket {
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

                        if server.lowercasedScheme == "wss" {
                            do {
                                let sslHandler = try self.makeSSLHandler(for: server)
                                // The sync methods here are safe because we're on the channel event loop
                                // due to the promise originating on the event loop of the channel.
                                try channel.pipeline.syncOperations.addHandlers(
                                    sslHandler, self.makeNameCheck(for: server))
                            } catch {
                                let tlsError = NatsError.ConnectError.tlsFailure(error)
                                upgradePromise.fail(tlsError)
                                return channel.eventLoop.makeFailedFuture(tlsError)
                            }
                        }

                        channel.pipeline.addHTTPClientHandlers(
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
        guard let connectUrls = info.connectUrls, let current = current.withLockedValue({ $0 })
        else {
            return
        }
        pool.withLockedValue {
            $0.add(connectUrls, websocket: current.server.url.isWebsocket, tls: current.tls)
        }
    }

    func close() async throws {
        self.reconnectTask?.cancel()
        try await self.reconnectTask?.value

        guard let eventLoop = self.channel?.eventLoop else {
            self.state.withLockedValue { $0 = .closed }
            self.pingTask?.cancel()
            self.fire(.closed)
            return
        }
        let promise = eventLoop.makePromise(of: Void.self)

        eventLoop.execute {
            self.state.withLockedValue { $0 = .closed }
            self.pingTask?.cancel()
            self.channel?.close(mode: .all, promise: promise)
        }

        do {
            try await promise.futureResult.get()
        } catch ChannelError.alreadyClosed {
            // we don't want to throw an error if channel is already closed
            // as that would mean we would get an error closing client during reconnect
        }

        self.fire(.closed)
    }

    private func disconnect() async throws {
        self.pingTask?.cancel()
        try await self.channel?.close().get()
    }

    func suspend() async throws {
        self.reconnectTask?.cancel()
        _ = try await self.reconnectTask?.value

        // Handle case where channel is already nil (e.g., during rapid reconnections)
        guard let eventLoop = self.channel?.eventLoop else {
            // Set state to suspended even if channel is nil
            self.state.withLockedValue { $0 = .suspended }
            return
        }
        let promise = eventLoop.makePromise(of: Void.self)

        eventLoop.execute {  // This ensures the code block runs on the event loop
            let shouldClose = self.state.withLockedValue { currentState in
                let wasConnected = currentState == .connected
                currentState = .suspended
                return wasConnected
            }

            if shouldClose {
                self.pingTask?.cancel()
                self.channel?.close(mode: .all, promise: promise)
            } else {
                promise.succeed()
            }
        }

        try await promise.futureResult.get()
        self.fire(.suspended)
    }

    func resume() async throws {
        guard let eventLoop = self.channel?.eventLoop else {
            throw NatsError.ClientError.internalError("channel should not be nil")
        }
        try await eventLoop.submit {
            let canResume = self.state.withLockedValue { $0 == .suspended }
            guard canResume else {
                throw NatsError.ClientError.invalidConnection(
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

        parseRemainder.withLockedValue { $0 = nil }

        self._inputBuffer.withLockedValue {
            $0 = context.channel.allocator.buffer(capacity: 1024 * 1024 * 8)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.debug("TCP channel inactive")

        // If we lost the channel before we delivered server INFO or connection
        // establishment, make sure to fail any pending continuations to avoid leaks.
        let serverInfoCont = serverInfoContinuation.withLockedValue { cont in
            defer { cont = nil }
            return cont
        }
        let establishedCont = connectionEstablishedContinuation.withLockedValue { cont in
            defer { cont = nil }
            return cont
        }
        // with no continuation waiting, the error stays for the one sendClientConnectInit sets
        if serverInfoCont != nil || establishedCont != nil {
            let errorToUse = takeConnectionError()
            serverInfoCont?.resume(throwing: errorToUse)
            establishedCont?.resume(throwing: errorToUse)
        }

        let shouldHandleDisconnect = state.withLockedValue { $0 == .connected }
        if shouldHandleDisconnect {
            handleDisconnect()
        }
    }

    /// The error that fails the current attempt once its channel is closed: the error captured on
    /// the channel (e.g., a TLS failure), else connectionClosed.
    private func takeConnectionError() -> Error {
        capturedConnectionError.withLockedValue { err in
            defer { err = nil }
            guard let captured = err else {
                return NatsError.ClientError.connectionClosed
            }
            // a line from the server that does not parse is not a TLS failure
            if captured is NatsError.ProtocolError {
                return NatsError.ConnectError.io(captured)
            }
            return NatsError.ConnectError.tlsFailure(captured)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        logger.debug("Encountered error on the channel: \(error)")

        let isConnecting = state.withLockedValue { $0 == .pending || $0 == .connecting }
        if isConnecting {
            capturedConnectionError.withLockedValue { $0 = error }
        }

        context.close(promise: nil)

        if let natsErr = error as? NatsErrorProtocol {
            self.fire(.error(natsErr))
        } else {
            logger.error("unexpected error: \(error)")
        }
    }

    func handleDisconnect() {
        state.withLockedValue { $0 = .disconnected }
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
        // settings that connect() refuses are never tried
        guard configurationError == nil else { return }

        let isAlreadyReconnecting = _reconnectTask.withLockedValue { task -> Bool in
            guard let activeTask = task else { return false }
            return !activeTask.isCancelled
        }

        guard !isAlreadyReconnecting else {
            logger.debug("Reconnect already in progress. Ignoring duplicate trigger.")
            return
        }

        reconnectTask = Task {

            defer {
                _reconnectTask.withLockedValue { $0 = nil }
            }

            var connected = false
            while !Task.isCancelled
                && (maxReconnects == nil || self.reconnectAttempts < maxReconnects!)
            {
                do {
                    try await self.connect()
                    connected = true
                    break  // Successfully connected
                } catch is CancellationError {
                    logger.debug("Reconnect task cancelled")
                    return
                } catch {
                    logger.debug("Could not reconnect: \(error)")
                    // No error raised on the channel reports these as an event: no TLS offered,
                    // or a certificate not issued for the server's name.
                    if let error = error as? NatsError.ConnectError,
                        case .tlsFailure(let cause) = error,
                        cause is NatsError.TlsNotOffered
                            || cause is NatsError.CertificateNameMismatch
                    {
                        self.fire(.error(error))
                    }
                    if !Task.isCancelled {
                        try await Task.sleep(nanoseconds: self.reconnectWait)
                    }
                }
            }

            // Early return if cancelled
            if Task.isCancelled {
                logger.debug("Reconnect task cancelled after connection attempts")
                return
            }

            // If we got here without connecting and weren't cancelled, we hit max reconnects
            if !connected {
                logger.error("Could not reconnect; maxReconnects exceeded")
                try await self.close()
                return
            }

            self.channel?.eventLoop.execute {
                self.state.withLockedValue { $0 = .connected }
                self.fire(.connected)
            }
        }
    }

    private func restoreSubscriptions() async throws {
        let subsToRestore = subscriptions.withLockedValue { Array($0) }
        for (sid, sub) in subsToRestore {
            do {
                try await resubscribe(sid: sid, sub)
            } catch {
                logger.error("Error recreating subscription \(sid): \(error)")
                self.fire(.error((error as? NatsErrorProtocol) ?? NatsError.ClientError.io(error)))
                throw error
            }
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

extension URL {
    /// The scheme in lower case: schemes are compared without regard to case.
    fileprivate var lowercasedScheme: String? {
        scheme?.lowercased()
    }

    fileprivate var isWebsocket: Bool {
        lowercasedScheme == "ws" || lowercasedScheme == "wss"
    }

    /// The URL as messages print it: in quotes, without user and password.
    fileprivate var quoted: String {
        var components = URLComponents(url: self, resolvingAgainstBaseURL: false)
        components?.user = nil
        components?.password = nil
        return "\"\(components?.string ?? host ?? "")\""
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
