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

class ServerPoolTests: XCTestCase {

    static var allTests = [
        ("testEntryOfAKnownServerAddsNothing", testEntryOfAKnownServerAddsNothing),
        ("testDiscoveredServerGainsTlsAndKeepsIt", testDiscoveredServerGainsTlsAndKeepsIt),
        ("testLatestHasTheTlsTheServerGained", testLatestHasTheTlsTheServerGained),
    ]

    private let configured = [URL(string: "tls://a:4222")!, URL(string: "nats://b:4222")!]

    private func urls(_ pool: ServerPool) -> [String] {
        pool.servers.map { $0.url.absoluteString }
    }

    func testEntryOfAKnownServerAddsNothing() {
        var pool = ServerPool(configured: configured)
        pool.add(["a:4222", "user@b:4222", "c:4222", "u:p@c:4222"], websocket: false, tls: true)
        XCTAssertEqual(urls(pool), ["tls://a:4222", "nats://b:4222", "tls://c:4222"])
        XCTAssertEqual(pool.servers.map { $0.isDiscovered }, [false, false, true])
    }

    func testDiscoveredServerGainsTlsAndKeepsIt() {
        var pool = ServerPool(configured: [])
        pool.add(["c:4222"], websocket: false, tls: false)
        pool.add(["w:80"], websocket: true, tls: false)
        XCTAssertEqual(urls(pool), ["nats://c:4222", "ws://w:80"])
        pool.add(["c:4222", "w:80"], websocket: false, tls: true)
        XCTAssertEqual(urls(pool), ["tls://c:4222", "wss://w:80"])
        pool.add(["c:4222", "w:80"], websocket: false, tls: false)
        XCTAssertEqual(urls(pool), ["tls://c:4222", "wss://w:80"])
        XCTAssertEqual(pool.servers.map { $0.learnedOverTls }, [true, true])
    }

    func testLatestHasTheTlsTheServerGained() {
        var pool = ServerPool(configured: configured)
        pool.add(["c:4222"], websocket: false, tls: false)
        let read = pool.servers[2]
        pool.add(["c:4222"], websocket: false, tls: true)
        XCTAssertEqual(pool.latest(read)?.url, URL(string: "tls://c:4222"))
        XCTAssertEqual(pool.latest(pool.servers[0])?.url, configured[0])
    }
}
