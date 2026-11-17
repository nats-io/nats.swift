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

    /// The entry as a log line quotes it: escaped, and cut at 256 characters, since the server
    /// chose it.
    static func quoted(_ entry: String) -> String {
        String(entry.prefix(256)).debugDescription
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
