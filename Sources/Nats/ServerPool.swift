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

/// A `connect_urls` entry of an INFO: the host and port of the server it names, or why the
/// client skips it.
///
/// nats-server writes an entry as `host:port`, with an IPv6 address in brackets. User info in
/// front of the host is dropped: the server is connected to with the client's own
/// credentials. An entry with a scheme is skipped rather than reduced to `host:port`, and so
/// is anything else that is not `host:port`.
internal enum DiscoveredEntry: Equatable {
    case server(host: String, port: Int)
    case skipped(reason: String)

    init(_ entry: String) {
        if entry.contains("://") {
            self = .skipped(reason: "it has a scheme")
            return
        }
        let hostAndPort = entry.lastIndex(of: "@").map { entry[entry.index(after: $0)...] }
        guard let (host, port) = Self.parse(hostAndPort ?? entry[...]),
            Self.url(host: host, port: port, scheme: "nats") != nil
        else {
            self = .skipped(reason: "it is not host:port")
            return
        }
        self = .server(host: host, port: port)
    }

    /// The URL of the server with `scheme`, or nil when the entry is skipped.
    func url(scheme: String) -> URL? {
        guard case .server(let host, let port) = self else {
            return nil
        }
        return Self.url(host: host, port: port, scheme: scheme)
    }

    private static func url(host: String, port: Int, scheme: String) -> URL? {
        let urlHost = host.contains(":") ? "[\(host)]" : host
        // The URL must have this host and port and nothing else.
        guard let url = URL(string: "\(scheme)://\(urlHost):\(port)"), url.host == host,
            url.port == port, url.user == nil, url.password == nil, url.path.isEmpty,
            url.query == nil, url.fragment == nil
        else {
            return nil
        }
        return url
    }

    /// Parses `host:port` or `[address]:port`, byte by byte. A host is ASCII letters, digits,
    /// `-`, `.` and `_`, and does not end in a dot after an IP address; an address in brackets
    /// must be an IPv6 address; a port is 1 to 5 digits in 1...65535.
    private static func parse(_ entry: Substring) -> (host: String, port: Int)? {
        func isDigit(_ byte: UInt8) -> Bool {
            (UInt8(ascii: "0")...UInt8(ascii: "9")).contains(byte)
        }
        func isLetter(_ byte: UInt8) -> Bool {
            (UInt8(ascii: "a")...UInt8(ascii: "z")).contains(byte | 0x20)
        }
        func isHexDigit(_ byte: UInt8) -> Bool {
            isDigit(byte) || (UInt8(ascii: "a")...UInt8(ascii: "f")).contains(byte | 0x20)
        }
        let colon = UInt8(ascii: ":")
        let dot = UInt8(ascii: ".")
        let bytes = Array(entry.utf8)
        let hostBytes: ArraySlice<UInt8>
        let rest: ArraySlice<UInt8>
        let bracketed = bytes.first == UInt8(ascii: "[")
        if bracketed {
            guard let close = bytes.firstIndex(of: UInt8(ascii: "]")) else {
                return nil
            }
            hostBytes = bytes[1..<close]
            rest = bytes[(close + 1)...]
            guard hostBytes.allSatisfy({ isHexDigit($0) || $0 == colon || $0 == dot }) else {
                return nil
            }
        } else {
            guard let end = bytes.firstIndex(of: colon) else {
                return nil
            }
            hostBytes = bytes[..<end]
            rest = bytes[end...]
            let isName = hostBytes.allSatisfy {
                isDigit($0) || isLetter($0) || $0 == UInt8(ascii: "-") || $0 == dot
                    || $0 == UInt8(ascii: "_")
            }
            guard isName else {
                return nil
            }
        }
        let portBytes = rest.dropFirst()
        guard !hostBytes.isEmpty, rest.first == colon, (1...5).contains(portBytes.count),
            portBytes.allSatisfy(isDigit),
            let port = Int(String(decoding: portBytes, as: UTF8.self)), (1...65535).contains(port)
        else {
            return nil
        }
        let host = String(decoding: hostBytes, as: UTF8.self)
        if bracketed {
            guard host.contains(":"), isIPAddress(host) else {
                return nil
            }
        } else if host.hasSuffix("."), isIPAddress(String(host.dropLast())) {
            // a trailing dot roots a name; after an address it is not part of the address
            return nil
        }
        return (host, port)
    }

    private static func isIPAddress(_ host: String) -> Bool {
        (try? SocketAddress(ipAddress: host, port: 0)) != nil
    }
}

/// A server the client can connect to: one set in the options, or one that a server reported
/// in the `connect_urls` of its INFO.
internal struct Server: Sendable {
    /// Host and port as written: two servers with the same key are one server.
    struct Key: Hashable, Sendable {
        let host: String
        let port: Int
    }

    /// For a discovered server, `nats://host:port`, or `ws://host:port` when it was learned on
    /// a websocket connection; `tls://` or `wss://` once it was learned on a connection with
    /// TLS.
    private(set) var url: URL
    /// Reported by a server rather than set in the options.
    let isDiscovered: Bool
    let key: Key
    /// The name inherited from the server that reported it, nil when there is none. For a
    /// server given as an IP address, it is the host name its certificate is verified against:
    /// the name under which the reporting server was verified, or, learned without TLS, the
    /// host name of the configured server through which it was learned. For any discovered
    /// server, it is also the name that the servers it lists while connected without TLS
    /// inherit.
    private(set) var inheritedName: String?

    /// Learned on a connection with TLS: it is connected to with TLS, whatever its INFO says.
    var learnedOverTls: Bool {
        isDiscovered && (url.scheme == "tls" || url.scheme == "wss")
    }

    init(configured url: URL) {
        self.url = url
        self.isDiscovered = false
        self.key = Key(host: url.host ?? "", port: url.port ?? 0)
    }

    /// The server of `entry`, learned on a connection that is a websocket when `websocket` is
    /// set and has TLS when `tls` is set. It is connected to with the same transport and, when
    /// that connection had TLS, with TLS. Given as an IP address, it is verified against
    /// `name`.
    init?(discovered entry: DiscoveredEntry, websocket: Bool, tls: Bool, name: String?) {
        let scheme = websocket ? (tls ? "wss" : "ws") : (tls ? "tls" : "nats")
        guard case .server(let host, let port) = entry, let url = entry.url(scheme: scheme) else {
            return nil
        }
        self.url = url
        self.isDiscovered = true
        self.key = Key(host: host, port: port)
        self.inheritedName = name
    }

    /// Learned again on a connection with TLS, whose server was verified under `name`: from
    /// now on it is connected to with TLS, and verified against `name` when given as an IP
    /// address.
    mutating func gainTls(name: String?) {
        guard isDiscovered, !learnedOverTls,
            var components = URLComponents(url: url, resolvingAgainstBaseURL: false)
        else {
            return
        }
        components.scheme = url.scheme == "ws" ? "wss" : "tls"
        if let url = components.url {
            self.url = url
            self.inheritedName = name
        }
    }
}

/// The servers the client can connect to: the configured ones in their order, then the
/// discovered ones in the order they were learned, one per host and port.
internal struct ServerPool: Sendable {
    private(set) var servers: [Server]
    /// The index in `servers` of the first server with each key.
    private var indices: [Server.Key: Int] = [:]

    init(configured urls: [URL]) {
        servers = urls.map { Server(configured: $0) }
        index()
    }

    private mutating func index() {
        indices = [:]
        for (index, server) in servers.enumerated() where indices[server.key] == nil {
            indices[server.key] = index
        }
    }

    /// `server` as the pool has it now, or nil for a discovered server the pool no longer
    /// has. A discovered server may have gained TLS since `server` was read.
    func latest(_ server: Server) -> Server? {
        guard server.isDiscovered else {
            return server
        }
        guard let index = indices[server.key], servers[index].isDiscovered else {
            return nil
        }
        return servers[index]
    }

    /// Adds the servers in `entries`, learned on a connection that is a websocket when
    /// `websocket` is set and has TLS when `tls` is set, as discovered servers. An IP address
    /// among them is verified against `name`.
    ///
    /// An entry that is a server of the pool adds nothing, and a configured server stays as
    /// configured. A discovered server learned without TLS gains TLS when it is learned again
    /// on a connection with TLS, and never loses it; it then takes `name`.
    mutating func add(_ entries: [String], websocket: Bool, tls: Bool, name: String? = nil) {
        for entry in entries {
            let discovered = DiscoveredEntry(entry)
            if case .skipped(let reason) = discovered {
                logger.warning(
                    "skipping the connect_urls entry \"\(NatsError.escaped(entry))\": \(reason)")
            }
            guard
                let server = Server(
                    discovered: discovered, websocket: websocket, tls: tls, name: name)
            else {
                continue
            }
            if let index = indices[server.key] {
                if tls {
                    servers[index].gainTls(name: name)
                }
            } else {
                indices[server.key] = servers.count
                servers.append(server)
            }
        }
    }

    /// Replaces the discovered servers with those in `entries`, from an INFO received on the
    /// connection to `current`, which is a websocket when `websocket` is set and has TLS when
    /// `tls` is set. An IP address among them is verified against `name`. The configured
    /// servers and `current` stay. A discovered server that drops out of the list and comes
    /// back is learned again. An empty list changes nothing.
    mutating func replace(
        _ entries: [String], websocket: Bool, tls: Bool, current: Server.Key,
        name: String? = nil
    ) {
        guard !entries.isEmpty else {
            return
        }
        var listed: Set<Server.Key> = [current]
        for entry in entries {
            if case .server(let host, let port) = DiscoveredEntry(entry) {
                listed.insert(Server.Key(host: host, port: port))
            }
        }
        servers.removeAll { $0.isDiscovered && !listed.contains($0.key) }
        index()
        add(entries, websocket: websocket, tls: tls, name: name)
    }
}
