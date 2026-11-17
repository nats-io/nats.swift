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
        ("testLaterListReplacesDiscoveredServers", testLaterListReplacesDiscoveredServers),
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

    func testLaterListReplacesDiscoveredServers() {
        var pool = ServerPool(configured: configured)
        pool.add(["c:1", "d:1", "e:1"], websocket: false, tls: false)
        let dropped = pool.servers[3]
        let current = Server.Key(host: "c", port: 1)
        pool.replace(["e:1", "f:1", "a:4222"], websocket: false, tls: true, current: current)
        XCTAssertEqual(
            urls(pool), ["tls://a:4222", "nats://b:4222", "nats://c:1", "tls://e:1", "tls://f:1"])
        XCTAssertNil(pool.latest(dropped))
        pool.replace([], websocket: false, tls: false, current: current)
        XCTAssertEqual(pool.servers.count, 5)
        // back in a later list, a dropped server is learned again
        pool.replace(["d:1"], websocket: false, tls: false, current: current)
        XCTAssertEqual(urls(pool), ["tls://a:4222", "nats://b:4222", "nats://c:1", "nats://d:1"])
    }
}
