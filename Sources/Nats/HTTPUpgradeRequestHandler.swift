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

import NIO
import NIOHTTP1
import NIOWebSocket

// Adapted from https://github.com/vapor/websocket-kit/blob/main/Sources/WebSocketKit/HTTPUpgradeRequestHandler.swift
internal final class HTTPUpgradeRequestHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundOut = HTTPClientRequestPart

    let host: String
    let headers = HTTPHeaders()
    let upgradePromise: EventLoopPromise<Void>

    private var requestSent = false

    init(host: String, upgradePromise: EventLoopPromise<Void>) {
        self.host = host
        self.upgradePromise = upgradePromise
    }

    func channelActive(context: ChannelHandlerContext) {
        self.sendRequest(context: context)
        context.fireChannelActive()
    }

    func handlerAdded(context: ChannelHandlerContext) {
        if context.channel.isActive {
            self.sendRequest(context: context)
        }
    }

    private func sendRequest(context: ChannelHandlerContext) {
        if self.requestSent {
            // we might run into this handler twice, once in handlerAdded and once in channelActive.
            return
        }
        self.requestSent = true

        var headers = self.headers
        headers.add(name: "Host", value: self.host)

        let requestHead = HTTPRequestHead(
            version: HTTPVersion(major: 1, minor: 1),
            method: .GET,
            uri: "/",
            headers: headers
        )
        context.write(self.wrapOutboundOut(.head(requestHead)), promise: nil)

        let emptyBuffer = context.channel.allocator.buffer(capacity: 0)
        let body = HTTPClientRequestPart.body(.byteBuffer(emptyBuffer))
        context.write(self.wrapOutboundOut(body), promise: nil)

        context.writeAndFlush(self.wrapOutboundOut(.end(nil)), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        // `NIOHTTPClientUpgradeHandler` should consume the first response in the success case,
        // any response we see here indicates a failure. Report the failure and tidy up at the end of the response.
        let clientResponse = self.unwrapInboundIn(data)
        switch clientResponse {
        case .head(let responseHead):
            //let error = WebSocketClient.Error.invalidResponseStatus(responseHead)
            self.upgradePromise.fail(NatsClientError("ws error \(responseHead)"))
        case .body: break
        case .end:
            context.close(promise: nil)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.upgradePromise.fail(error)
        context.close(promise: nil)
    }
}

internal final class WebSocketByteBufferCodec: ChannelDuplexHandler {
    typealias InboundIn = WebSocketFrame
    typealias InboundOut = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = WebSocketFrame

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)

        switch frame.opcode {
        case .binary:
            context.fireChannelRead(wrapInboundOut(frame.data))
        case .text:
            preconditionFailure("We will never receive a text frame")
        case .continuation:
            preconditionFailure("We will never receive a continuation frame")
        case .pong:
            break
        case .ping:
            if frame.fin {
                var frameData = frame.data
                let maskingKey = frame.maskKey
                if let maskingKey = maskingKey {
                    frameData.webSocketUnmask(maskingKey)
                }
                let bb = context.channel.allocator.buffer(bytes: frameData.readableBytesView)
                self.send(
                    bb,
                    context: context,
                    opcode: .pong
                )
            } else {
                context.close(promise: nil)
            }
        default:
            // We ignore all other frames.
            break
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let buffer = unwrapOutboundIn(data)
        let frame = WebSocketFrame(
            fin: true,
            opcode: .binary,
            maskKey: self.makeMaskKey(),
            data: buffer
        )
        context.write(wrapOutboundOut(frame), promise: promise)
    }

    public func send(
        _ data: ByteBuffer,
        context: ChannelHandlerContext,
        opcode: WebSocketOpcode = .binary,
        fin: Bool = true,
        promise: EventLoopPromise<Void>? = nil
    ) {
        let frame = WebSocketFrame(
            fin: fin,
            opcode: opcode,
            maskKey: self.makeMaskKey(),
            data: data
        )
        context.writeAndFlush(wrapOutboundOut(frame), promise: promise)
    }

    func makeMaskKey() -> WebSocketMaskingKey? {
        /// See https://github.com/apple/swift/issues/66099
        var generator = SystemRandomNumberGenerator()
        return WebSocketMaskingKey.random(using: &generator)
    }
}
