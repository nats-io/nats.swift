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

import NIO
import NIOSSL
import NIOTLS

/// Checks the name the server's certificate is issued for once the TLS handshake is done, and
/// holds writes and drops reads until then. It goes right after the TLS handler, which verifies
/// only the certificate chain.
///
/// A host name is checked only against the certificate's DNS names, an IP address only against
/// its IP addresses. When the certificate is not issued for the name, the handler fails with
/// ``NatsError/CertificateNameMismatch`` and closes the connection at once, without waiting for
/// the server to answer a TLS close: nothing written before the check is sent, nothing the
/// server sends is read, and the errors that follow are dropped.
internal final class TlsNameCheckHandler: ChannelDuplexHandler {
    typealias InboundIn = NIOAny
    typealias InboundOut = NIOAny
    typealias OutboundIn = NIOAny
    typealias OutboundOut = NIOAny

    private enum State {
        case handshaking
        case checked
        case failed(Error)
    }

    /// The server as messages name it.
    private let server: String
    /// The host name or IP address the certificate must be issued for.
    private let name: String
    private var state = State.handshaking
    private var heldWrites: [(data: NIOAny, promise: EventLoopPromise<Void>?)] = []
    private var heldFlush = false

    init(server: String, name: String) {
        self.server = server
        self.name = name
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        guard case .handshaking = state, let tlsEvent = event as? TLSUserEvent,
            case .handshakeCompleted = tlsEvent
        else {
            context.fireUserInboundEventTriggered(event)
            return
        }
        let certificate = try? context.pipeline.syncOperations.nioSSL_peerCertificate()
        guard let certificate, Self.certificate(certificate, isIssuedFor: name) else {
            let names = certificate.map { Self.names(of: $0) } ?? []
            fail(context: context, error: Self.mismatch(server: server, name: name, names: names))
            return
        }
        state = .checked
        // what was written before the check goes first
        let writes = heldWrites
        heldWrites = []
        for write in writes {
            context.write(write.data, promise: write.promise)
        }
        if heldFlush {
            heldFlush = false
            context.flush()
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard case .checked = state else {
            return
        }
        context.fireChannelRead(data)
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        switch state {
        case .handshaking:
            heldWrites.append((data, promise))
        case .checked:
            context.write(data, promise: promise)
        case .failed(let error):
            promise?.fail(error)
        }
    }

    func flush(context: ChannelHandlerContext) {
        switch state {
        case .handshaking:
            heldFlush = true
        case .checked:
            context.flush()
        case .failed:
            break
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        failHeldWrites(ChannelError.ioOnClosedChannel)
        context.fireChannelInactive()
    }

    // a write after channelInactive is held until the handler is removed, one loop turn later
    func handlerRemoved(context: ChannelHandlerContext) {
        failHeldWrites(ChannelError.ioOnClosedChannel)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        // the failed check is reported; an error of the close that follows, e.g. an unclean
        // TLS shutdown, would replace it
        if case .failed = state {
            return
        }
        context.fireErrorCaught(error)
    }

    private func fail(context: ChannelHandlerContext, error: Error) {
        state = .failed(error)
        failHeldWrites(error)
        context.fireErrorCaught(error)
        // closed below the TLS handler: a TLS close would wait for the server to answer it
        let tls = try? context.pipeline.syncOperations.context(
            handlerType: NIOSSLClientHandler.self)
        (tls ?? context).close(promise: nil)
    }

    private func failHeldWrites(_ error: Error) {
        let writes = heldWrites
        heldWrites = []
        heldFlush = false
        for write in writes {
            write.promise?.fail(error)
        }
    }

    /// Whether `certificate` is issued for `name`: an IP address by an IP address entry equal
    /// to it, a host name by a DNS name entry that matches it.
    static func certificate(_ certificate: NIOSSLCertificate, isIssuedFor name: String) -> Bool {
        let address = try? SocketAddress(ipAddress: name, port: 0)
        return certificate._subjectAlternativeNames().contains { entry in
            if let address {
                return entry.nameType == .ipAddress
                    && (try? SocketAddress(
                        packedIPAddress: ByteBuffer(bytes: entry.contents), port: 0)) == address
            }
            return entry.nameType == .dnsName && dnsName(Array(entry.contents), matches: name)
        }
    }

    /// Whether `pattern`, a DNS name entry of a certificate, matches the host name `host`:
    /// label by label without regard to ASCII case, where a first label `*` of a name with more
    /// labels stands for any one label. One trailing dot is ignored on either.
    static func dnsName(_ pattern: [UInt8], matches host: String) -> Bool {
        let patternLabels = labels(of: pattern)
        let hostLabels = labels(of: Array(host.utf8))
        guard !patternLabels.isEmpty, patternLabels.count == hostLabels.count else {
            return false
        }
        return zip(patternLabels, hostLabels).enumerated().allSatisfy { index, labels in
            if index == 0 && patternLabels.count > 1 && labels.0.elementsEqual("*".utf8) {
                return !labels.1.isEmpty
            }
            return labels.0 == labels.1
        }
    }

    /// The labels of a DNS name in ASCII lower case, without one trailing dot.
    private static func labels(of name: [UInt8]) -> [ArraySlice<UInt8>] {
        var name = name.map {
            (UInt8(ascii: "A")...UInt8(ascii: "Z")).contains($0) ? $0 | 0x20 : $0
        }
        if name.last == UInt8(ascii: ".") {
            name.removeLast()
        }
        guard !name.isEmpty else {
            return []
        }
        return name.split(separator: UInt8(ascii: "."), omittingEmptySubsequences: false)
    }

    /// The DNS names and IP addresses `certificate` is issued for.
    static func names(of certificate: NIOSSLCertificate) -> [String] {
        certificate._subjectAlternativeNames().compactMap { entry in
            switch entry.nameType {
            case .dnsName:
                return String(decoding: entry.contents, as: UTF8.self)
            case .ipAddress:
                return
                    (try? SocketAddress(
                        packedIPAddress: ByteBuffer(bytes: entry.contents), port: 0))?.ipAddress
            default:
                return nil
            }
        }
    }

    /// The error for a certificate of `server` that is issued for `names` but not for `name`.
    static func mismatch(
        server: String, name: String, names: [String]
    ) -> NatsError.CertificateNameMismatch {
        let shown = 4
        var message = "the certificate of \(server) is not valid for \(String(reflecting: name))"
        if names.isEmpty {
            message +=
                ": it has no DNS name or IP address entry, and the common name of its subject is"
                + " not checked"
        } else {
            message +=
                ": it is issued for "
                + names.prefix(shown).map { String(reflecting: $0) }.joined(separator: ", ")
            if names.count > shown {
                message += " and \(names.count - shown) more"
            }
        }
        return NatsError.CertificateNameMismatch(description: message)
    }
}
