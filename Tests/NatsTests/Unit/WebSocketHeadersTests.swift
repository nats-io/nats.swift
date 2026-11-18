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

import NIOCore
import NIOHTTP1
import NIOPosix
import XCTest

@testable import Nats

class WebSocketHeadersTests: XCTestCase {
    static var allTests = [
        ("testWebSocketUpgradeSendsConfiguredHeaders", testWebSocketUpgradeSendsConfiguredHeaders)
    ]

    /// Records the first request head and answers 403, so the client's connect fails right after the upgrade request.
    private final class RecordingHandler: ChannelInboundHandler {
        typealias InboundIn = HTTPServerRequestPart
        typealias OutboundOut = HTTPServerResponsePart

        let received: EventLoopPromise<HTTPRequestHead>

        init(received: EventLoopPromise<HTTPRequestHead>) {
            self.received = received
        }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            guard case .head(let head) = unwrapInboundIn(data) else { return }
            received.succeed(head)
            let response = HTTPResponseHead(
                version: .http1_1, status: .forbidden, headers: ["Content-Length": "0"])
            context.write(wrapOutboundOut(.head(response)), promise: nil)
            context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
        }
    }

    func testWebSocketUpgradeSendsConfiguredHeaders() async throws {
        let group = MultiThreadedEventLoopGroup.singleton
        let received = group.next().makePromise(of: HTTPRequestHead.self)
        let server = try await ServerBootstrap(group: group)
            .childChannelInitializer { channel in
                channel.pipeline.configureHTTPServerPipeline().flatMap {
                    channel.pipeline.addHandler(RecordingHandler(received: received))
                }
            }
            .bind(host: "127.0.0.1", port: 0).get()
        let port = try XCTUnwrap(server.localAddress?.port)

        let client = NatsClientOptions()
            .url(URL(string: "ws://127.0.0.1:\(port)")!)
            .websocketHeaders(["User-Agent": "nats-test/1.0", "X-Test": "1"])
            .build()
        do {
            try await client.connect()
            XCTFail("the 403 should fail the connect")
        } catch {}

        let head = try await received.futureResult.get()
        XCTAssertEqual(head.headers["User-Agent"], ["nats-test/1.0"])
        XCTAssertEqual(head.headers["X-Test"], ["1"])
        XCTAssertEqual(head.headers["Host"], ["127.0.0.1"])
        try await server.close()
    }
}
