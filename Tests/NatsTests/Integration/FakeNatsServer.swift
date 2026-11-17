// Copyright 2026 The NATS Authors
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

import Foundation
import NIO
import NIOConcurrencyHelpers
import NIOHTTP1
import NIOSSL
import NIOTLS
import NIOWebSocket
import XCTest

/// A scripted NATS server for protocol-level tests.
///
/// On every accepted connection it writes `greeting` (usually an INFO, possibly followed by more
/// bytes), optionally switches the connection to TLS, then answers `PING` with `PONG`
/// and records every protocol line the client sends. The first raw bytes of each connection
/// are recorded before any TLS processing, so a test can tell a ClientHello from plaintext.
/// Each connection can follow a script of its own. A websocket script answers the upgrade
/// request first and then carries the same exchange in binary websocket frames.
final class FakeNatsServer: Sendable {
    struct Script: Sendable {
        /// Bytes written right after accept, or `greetingDelay` seconds later.
        var greeting: String = ""
        /// When set, written in place of `greeting` on every connection after the first.
        var laterGreeting: String? = nil
        var greetingDelay: TimeInterval = 0
        /// When set, the server expects TLS: after the greeting, or before it with `tlsFirst`.
        var tls: NIOSSLContext? = nil
        var tlsFirst = false
        /// Bytes written ahead of the first PONG (real servers send nothing there).
        var beforeFirstPong: String? = nil
        /// Bytes written in place of the first PONG, e.g. an `-ERR` that refuses CONNECT.
        var firstPingReply: String? = nil
        /// Bytes written after the first PONG, e.g. an async INFO.
        var afterFirstPong: String? = nil
        /// Bytes written after the second PONG.
        var afterSecondPong: String? = nil
        var answersPing = true
        /// The server closes the connection right after the greeting, as a full server does, or
        /// `closeDelay` seconds later.
        var closeAfterGreeting = false
        var closeDelay: TimeInterval = 0
        /// After the first PONG the server reads nothing more from the connection: later PINGs
        /// are not answered, and a TLS close the client starts does not complete until
        /// ``FakeNatsServer/closeConnection(_:)``.
        var stopsReadingAfterFirstPong = false
        /// What the server does once its TLS handshake is done.
        var afterTlsHandshake: AfterTlsHandshake? = nil
        /// Plaintext written `count` times, `interval` seconds apart, right after the greeting
        /// of the connection with index `sprayOnConnection`, until that connection closes. It
        /// bypasses the server's TLS.
        var spray: (text: String, interval: TimeInterval, count: Int)? = nil
        var sprayOnConnection = 0
        /// When set, the server first answers a websocket upgrade request, then reads and
        /// writes binary websocket frames; `firstBytes` are the first bytes read after the
        /// upgrade. Not used together with `tls`.
        var websocket = false
    }

    /// What a server does once its TLS handshake is done. It acts below its TLS, so it never
    /// answers a TLS close the client starts.
    enum AfterTlsHandshake: Sendable {
        /// It drops every byte it receives.
        case dropsBytes
        /// It closes the connection.
        case closes
        /// It writes `text` inside TLS, then resets the connection (SO_LINGER 0).
        case writesAndResets(String)
    }

    struct Connection: Sendable {
        var firstBytes: [UInt8] = []
        var lines: [String] = []
        var closed = false
    }

    let host: String
    let port: Int
    private let group: MultiThreadedEventLoopGroup
    private let listener: Channel
    private let state: NIOLockedValueBox<[Connection]>
    private let children: NIOLockedValueBox<[Channel]>
    private let stopped = NIOLockedValueBox(false)

    /// Every connection follows `script`.
    convenience init(
        _ script: Script, host: String = "127.0.0.1", port: Int = 0, accepting: Bool = true
    )
        throws
    {
        try self.init(scripts: [script], host: host, port: port, accepting: accepting)
    }

    /// Connection `i` follows `scripts[i]`, and every later connection the last script.
    ///
    /// With `accepting: false` the listener has a backlog of 1 and accepts nothing until
    /// ``startAccepting()``; once ``fillBacklog()`` has filled the queue, further TCP connects
    /// stay pending.
    init(
        scripts: [Script], host: String = "127.0.0.1", port: Int = 0, accepting: Bool = true
    )
        throws
    {
        precondition(!scripts.isEmpty, "a fake server needs a script")
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let state = NIOLockedValueBox<[Connection]>([])
        let children = NIOLockedValueBox<[Channel]>([])
        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .serverChannelOption(ChannelOptions.backlog, value: accepting ? 256 : 1)
            .serverChannelOption(ChannelOptions.autoRead, value: accepting)
            .childChannelInitializer { channel in
                let index = state.withLockedValue { conns -> Int in
                    conns.append(Connection())
                    return conns.count - 1
                }
                children.withLockedValue { $0.append(channel) }
                let recorder = RawRecorder(state: state, index: index)
                let script = scripts[min(index, scripts.count - 1)]
                let handler = ScriptHandler(
                    script: script, state: state, index: index, recorder: recorder)
                if script.websocket {
                    return upgradeToWebsocket(channel, recorder: recorder, handler: handler)
                }
                do {
                    try channel.pipeline.syncOperations.addHandler(recorder)
                    try channel.pipeline.syncOperations.addHandler(handler)
                } catch {
                    return channel.eventLoop.makeFailedFuture(error)
                }
                return channel.eventLoop.makeSucceededFuture(())
            }
        self.listener = try bootstrap.bind(host: host, port: port).wait()
        self.host = host
        self.port = listener.localAddress!.port!
        self.group = group
        self.state = state
        self.children = children
    }

    /// Starts accepting on a listener created with `accepting: false`.
    func startAccepting() {
        _ = try? listener.setOption(ChannelOptions.autoRead, value: true).wait()
        listener.read()
    }

    /// Opens connections until one stays pending, i.e. the accept queue is full. Returns the
    /// established ones, which must be kept open while connects are meant to stay pending.
    func fillBacklog() -> [Channel] {
        var dummies: [Channel] = []
        for _ in 0..<64 {
            let connect = ClientBootstrap(group: group).connectTimeout(.milliseconds(300))
                .connect(to: try! SocketAddress(ipAddress: host, port: port))
            guard let channel = try? connect.wait() else {
                break
            }
            dummies.append(channel)
        }
        return dummies
    }

    /// Recorded connections, in accept order.
    var connections: [Connection] {
        state.withLockedValue { $0 }
    }

    /// All protocol lines received on all connections.
    var lines: [String] {
        connections.flatMap { $0.lines }
    }

    /// Closes one accepted connection; the listener keeps accepting. A connection that stopped
    /// reading reads again first, so that a TLS close completes.
    func closeConnection(_ index: Int) {
        let child = children.withLockedValue { $0.indices.contains(index) ? $0[index] : nil }
        _ = try? child?.setOption(ChannelOptions.autoRead, value: true).wait()
        try? child?.close().wait()
    }

    /// Writes `text` on one accepted connection, inside TLS when that connection uses TLS.
    func write(_ text: String, on index: Int) {
        let child = children.withLockedValue { $0.indices.contains(index) ? $0[index] : nil }
        try? child?.writeAndFlush(ByteBuffer(string: text)).wait()
    }

    /// Closes the listener and every accepted connection. Safe to call more than once.
    func shutdown() {
        let alreadyStopped = stopped.withLockedValue { stopped -> Bool in
            defer { stopped = true }
            return stopped
        }
        guard !alreadyStopped else { return }
        try? listener.close().wait()
        for child in children.withLockedValue({ $0 }) {
            try? child.close().wait()
        }
        try? group.syncShutdownGracefully()
    }

    /// An INFO line. `connectUrls: nil` leaves the key out.
    static func info(
        tlsRequired: Bool = false, tlsAvailable: Bool = false, connectUrls: [String]? = [],
        connectInfo: Bool = false, nonce: String? = nil, lameDuckMode: Bool = false
    ) -> String {
        var info: [String: Any] = [
            "server_id": "FAKE", "server_name": "fake", "version": "2.14.0", "proto": 1,
            "go": "go1.25", "host": "127.0.0.1", "port": 4222, "headers": true,
            "max_payload": 1_048_576, "client_id": 1, "client_ip": "127.0.0.1",
            "tls_required": tlsRequired, "tls_available": tlsAvailable,
        ]
        if let connectUrls {
            info["connect_urls"] = connectUrls
        }
        if connectInfo {
            info["connect_info"] = true
        }
        if let nonce {
            info["nonce"] = nonce
        }
        if lameDuckMode {
            info["ldm"] = true
        }
        let json = try! JSONSerialization.data(withJSONObject: info)
        return "INFO \(String(decoding: json, as: UTF8.self))\r\n"
    }

    /// A server-side TLS context. The default test server certificate has DNS `localhost` and
    /// IP `::1`, issued by `rootCA`.
    static func tlsContext(
        cert: String = "server-cert", key: String = "server-key",
        maximumTLSVersion: TLSVersion? = nil
    ) throws
        -> NIOSSLContext
    {
        let bundle = Bundle.module
        let cert = bundle.url(forResource: cert, withExtension: "pem")!.path
        let key = bundle.url(forResource: key, withExtension: "pem")!.path
        var config = TLSConfiguration.makeServerConfiguration(
            certificateChain: try NIOSSLCertificate.fromPEMFile(cert).map { .certificate($0) },
            privateKey: .privateKey(try NIOSSLPrivateKey(file: key, format: .pem)))
        config.maximumTLSVersion = maximumTLSVersion
        return try NIOSSLContext(configuration: config)
    }
}

/// Answers the websocket upgrade request on `channel`, then puts `recorder` below the websocket
/// framing and `handler` above it.
private func upgradeToWebsocket(
    _ channel: Channel, recorder: RawRecorder, handler: ScriptHandler
) -> EventLoopFuture<Void> {
    let upgrader = NIOWebSocketServerUpgrader(
        shouldUpgrade: { channel, _ in channel.eventLoop.makeSucceededFuture(HTTPHeaders()) },
        upgradePipelineHandler: { channel, _ in
            do {
                try channel.pipeline.syncOperations.addHandler(recorder, position: .first)
                try channel.pipeline.syncOperations.addHandlers(WebSocketBytes(), handler)
            } catch {
                return channel.eventLoop.makeFailedFuture(error)
            }
            return channel.eventLoop.makeSucceededFuture(())
        })
    return channel.pipeline.configureHTTPServerPipeline(
        withServerUpgrade: (upgraders: [upgrader], completionHandler: { _ in }))
}

/// Reads the payload of binary and continuation frames as bytes, and writes bytes as binary
/// frames.
private final class WebSocketBytes: ChannelDuplexHandler, Sendable {
    typealias InboundIn = WebSocketFrame
    typealias InboundOut = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = WebSocketFrame

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)
        switch frame.opcode {
        case .binary, .continuation:
            context.fireChannelRead(wrapInboundOut(frame.unmaskedData))
        case .connectionClose:
            context.close(promise: nil)
        default:
            break
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let frame = WebSocketFrame(fin: true, opcode: .binary, data: unwrapOutboundIn(data))
        context.write(wrapOutboundOut(frame), promise: promise)
    }
}

/// Records the first raw bytes of a connection, before any TLS processing.
private final class RawRecorder: ChannelInboundHandler, RemovableChannelHandler, Sendable {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private let state: NIOLockedValueBox<[FakeNatsServer.Connection]>
    private let index: Int
    private let dropping = NIOLockedValueBox(false)

    init(state: NIOLockedValueBox<[FakeNatsServer.Connection]>, index: Int) {
        self.state = state
        self.index = index
    }

    /// Drops every byte read from now on.
    func dropReads() {
        dropping.withLockedValue { $0 = true }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !dropping.withLockedValue({ $0 }) else {
            return
        }
        let buffer = unwrapInboundIn(data)
        state.withLockedValue { conns in
            if conns[index].firstBytes.isEmpty {
                conns[index].firstBytes = Array(buffer.readableBytesView.prefix(64))
            }
        }
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        state.withLockedValue { $0[index].closed = true }
        context.fireChannelInactive()
    }
}

private final class ScriptHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let script: FakeNatsServer.Script
    private let state: NIOLockedValueBox<[FakeNatsServer.Connection]>
    private let index: Int
    private let recorder: RawRecorder
    private var buffer = ByteBuffer()
    private var pendingPayload = 0
    private var pongs = 0
    private var stoppedReading = false

    init(
        script: FakeNatsServer.Script, state: NIOLockedValueBox<[FakeNatsServer.Connection]>,
        index: Int, recorder: RawRecorder
    ) {
        self.script = script
        self.state = state
        self.index = index
        self.recorder = recorder
    }

    func handlerAdded(context: ChannelHandlerContext) {
        // after a websocket upgrade the channel is active already
        if script.websocket {
            start(context: context)
        }
    }

    func channelActive(context: ChannelHandlerContext) {
        start(context: context)
        context.fireChannelActive()
    }

    private func start(context: ChannelHandlerContext) {
        if script.greetingDelay > 0 {
            let loopBound = NIOLoopBound(context, eventLoop: context.eventLoop)
            let delay = TimeAmount.nanoseconds(Int64(script.greetingDelay * 1_000_000_000))
            context.eventLoop.scheduleTask(in: delay) { self.greet(context: loopBound.value) }
        } else {
            greet(context: context)
        }
    }

    private func greet(context: ChannelHandlerContext) {
        guard context.channel.isActive else {
            return
        }
        if let tls = script.tls, script.tlsFirst {
            addTls(context: context, tls: tls)
        }
        let greeting = index > 0 ? script.laterGreeting ?? script.greeting : script.greeting
        write(context: context, greeting)
        if script.closeAfterGreeting {
            if script.closeDelay > 0 {
                let channel = context.channel
                let delay = TimeAmount.nanoseconds(Int64(script.closeDelay * 1_000_000_000))
                context.eventLoop.scheduleTask(in: delay) { channel.close(promise: nil) }
            } else {
                context.close(promise: nil)
            }
            return
        }
        if let tls = script.tls, !script.tlsFirst {
            addTls(context: context, tls: tls)
        }
        if let spray = script.spray, index == script.sprayOnConnection {
            // written from the recorder's position, i.e. below the TLS handler
            let raw = NIOLoopBound(
                try! context.pipeline.syncOperations.context(handler: recorder),
                eventLoop: context.eventLoop)
            let remaining = NIOLoopBoundBox(spray.count, eventLoop: context.eventLoop)
            let delay = TimeAmount.nanoseconds(Int64(spray.interval * 1_000_000_000))
            context.eventLoop.scheduleRepeatedTask(initialDelay: delay, delay: delay) { task in
                guard remaining.value > 0, raw.value.channel.isActive else {
                    task.cancel()
                    return
                }
                remaining.value -= 1
                raw.value.writeAndFlush(NIOAny(ByteBuffer(string: spray.text)), promise: nil)
            }
        }
    }

    private func addTls(context: ChannelHandlerContext, tls: NIOSSLContext) {
        try! context.pipeline.syncOperations.addHandler(
            NIOSSLServerHandler(context: tls), position: .after(recorder))
    }

    private func write(context: ChannelHandlerContext, _ text: String) {
        guard !text.isEmpty else { return }
        context.writeAndFlush(wrapOutboundOut(ByteBuffer(string: text)), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !stoppedReading else {
            return
        }
        var incoming = unwrapInboundIn(data)
        buffer.writeBuffer(&incoming)
        while !stoppedReading {
            if pendingPayload > 0 {
                let skipped = min(pendingPayload, buffer.readableBytes)
                buffer.moveReaderIndex(forwardBy: skipped)
                pendingPayload -= skipped
                if pendingPayload > 0 {
                    break
                }
            }
            guard let crlf = buffer.readableBytesView.firstRange(of: [0x0d, 0x0a]) else {
                break
            }
            let line = buffer.readString(length: crlf.lowerBound - buffer.readerIndex) ?? ""
            buffer.moveReaderIndex(forwardBy: 2)
            state.withLockedValue { $0[index].lines.append(line) }
            if line.hasPrefix("PUB ") || line.hasPrefix("HPUB ") {
                // skip the payload and its CRLF
                pendingPayload = (Int(line.split(separator: " ").last ?? "") ?? 0) + 2
            } else if line == "PING" && script.answersPing {
                if pongs == 0, let before = script.beforeFirstPong {
                    write(context: context, before)
                }
                if pongs == 0, let reply = script.firstPingReply {
                    write(context: context, reply)
                    pongs += 1
                    continue
                }
                write(context: context, "PONG\r\n")
                pongs += 1
                if pongs == 1, let after = script.afterFirstPong {
                    write(context: context, after)
                } else if pongs == 2, let after = script.afterSecondPong {
                    write(context: context, after)
                }
                if pongs == 1 && script.stopsReadingAfterFirstPong {
                    stoppedReading = true
                    buffer.clear()
                    _ = context.channel.setOption(ChannelOptions.autoRead, value: false)
                }
            }
        }
        buffer.discardReadBytes()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let after = script.afterTlsHandshake, let event = event as? TLSUserEvent,
            case .handshakeCompleted = event
        {
            afterTlsHandshake(after, context: context)
        }
        context.fireUserInboundEventTriggered(event)
    }

    private func afterTlsHandshake(
        _ after: FakeNatsServer.AfterTlsHandshake, context: ChannelHandlerContext
    ) {
        // the recorder is below the TLS handler
        let raw = try! context.pipeline.syncOperations.context(handler: recorder)
        switch after {
        case .dropsBytes:
            // autoRead does not stop the TLS handler's reads, so the bytes are dropped below it
            recorder.dropReads()
        case .closes:
            raw.close(promise: nil)
        case .writesAndResets(let text):
            write(context: context, text)
            let rawBound = NIOLoopBound(raw, eventLoop: context.eventLoop)
            (context.channel as! SocketOptionProvider)
                .setSoLinger(linger(l_onoff: 1, l_linger: 0))
                .whenComplete { _ in rawBound.value.close(promise: nil) }
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }
}

/// Runs `operation` in a task and waits up to `timeout` seconds for it. Returns nil when it
/// does not finish in time, so a test can observe a hang without hanging itself.
func result<T: Sendable>(
    within timeout: TimeInterval, _ operation: @escaping @Sendable () async throws -> T
) async -> Result<T, Error>? {
    let box = NIOLockedValueBox<Result<T, Error>?>(nil)
    let done = XCTestExpectation(description: "operation finished within \(timeout)s")
    Task {
        do {
            let value = try await operation()
            box.withLockedValue { $0 = .success(value) }
        } catch {
            box.withLockedValue { $0 = .failure(error) }
        }
        done.fulfill()
    }
    await XCTWaiter().fulfillment(of: [done], timeout: timeout)
    return box.withLockedValue { $0 }
}
