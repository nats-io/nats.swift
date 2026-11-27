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

import XCTest

@testable import Nats

class DiscoveredEntryTests: XCTestCase {

    static var allTests = [
        ("testHostAndPort", testHostAndPort),
        ("testUserInfoIsDropped", testUserInfoIsDropped),
        ("testEntryWithSchemeIsSkipped", testEntryWithSchemeIsSkipped),
        ("testEntryThatIsNotHostPortIsSkipped", testEntryThatIsNotHostPortIsSkipped),
        ("testUrl", testUrl),
    ]

    func testHostAndPort() {
        let entries: [(String, String, Int)] = [
            ("127.0.0.1:4222", "127.0.0.1", 4222),
            ("localhost:4222", "localhost", 4222),
            ("NATS-1.Example.com:1", "NATS-1.Example.com", 1),
            ("my_host:65535", "my_host", 65535),
            ("rooted.name.:4222", "rooted.name.", 4222),
            ("[::1]:4222", "::1", 4222),
            ("[2001:DB8::1]:4222", "2001:DB8::1", 4222),
            ("[::ffff:127.0.0.1]:4222", "::ffff:127.0.0.1", 4222),
            ("10.0.0.1:04222", "10.0.0.1", 4222),
        ]
        for (entry, host, port) in entries {
            XCTAssertEqual(DiscoveredEntry(entry), .server(host: host, port: port), entry)
        }
    }

    func testUserInfoIsDropped() {
        for entry in ["user@host:4222", "user:pass@host:4222", "a@b@host:4222", "@host:4222"] {
            XCTAssertEqual(DiscoveredEntry(entry), .server(host: "host", port: 4222), entry)
        }
        XCTAssertEqual(DiscoveredEntry("user:pass@[::1]:4222"), .server(host: "::1", port: 4222))
    }

    func testEntryWithSchemeIsSkipped() {
        for entry in [
            "nats://host:4222", "tls://host:4222", "ws://host:4222", "wss://host:4222",
            "NATS://host:4222", "nats://user:pass@host:4222", "user@nats://host:4222",
        ] {
            XCTAssertEqual(DiscoveredEntry(entry), .skipped(reason: "it has a scheme"), entry)
        }
    }

    func testEntryThatIsNotHostPortIsSkipped() {
        for entry in [
            "", "host", "host:", ":4222", "host:0", "host:65536", "host:123456", "host:42a",
            "host:-1", "host:+1", " host:4222", "host:4222 ", "host/path:4222", "host:4222/path",
            "h?x=1:4222", "h#f:4222", "h%41:4222", "\u{00e9}:4222", "nats:host:4222",
            "127.0.0.1.:4222", "[::1]", "[::1]4222", "[::1:4222", "[127.0.0.1]:4222",
            "[host]:4222", "[::g]:4222", "[fe80::1%en0]:4222", "::1:4222", "user@",
        ] {
            XCTAssertEqual(DiscoveredEntry(entry), .skipped(reason: "it is not host:port"), entry)
        }
    }

    func testUrl() {
        XCTAssertEqual(
            DiscoveredEntry("user:pass@host:4222").url(scheme: "nats"),
            URL(string: "nats://host:4222"))
        XCTAssertEqual(DiscoveredEntry("[::1]:80").url(scheme: "ws"), URL(string: "ws://[::1]:80"))
        XCTAssertNil(DiscoveredEntry("nats://host:4222").url(scheme: "nats"))
    }
}
