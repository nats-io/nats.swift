//
//  NatsClient+ChannelInboundHandler.swift
//  SwiftyNats
//

import Foundation
import NIO

extension NatsClient: ChannelInboundHandler {

    public typealias InboundIn = ByteBuffer

    public func channelActive(context: ChannelHandlerContext) {
        logger.debug("Channel gets active")
        
        self.state = .connected
        inputBuffer = context.channel.allocator.buffer(capacity: 1024 * 1024 * 8)
    }
    
    public func channelReadComplete(context: ChannelHandlerContext) {
        logger.debug("Channel read complete")

        guard let chunkLength = inputBuffer?.readableBytes else {
            logger.warning("Input buffer has no data")
            return
        }

        guard let inputChunk = inputBuffer?.readString(length: chunkLength) else {
            logger.warning("Input buffer can not read into string")
            return
        }
        
        let messages = inputChunk.parseOutMessages()
        for message in messages {
            guard let type = message.getMessageType() else { return }

            switch type {
            case .ping:
                self.sendMessage(NatsMessage.pong())
            case .ok:
                self.fire(.response)
            case .error:
                self.fire(.error)
            case .message:
                self.handleIncomingMessage(message)
            case .info:
                self.updateServerInfo(with: message)
            default:
                continue
            }
        }
        inputBuffer?.clear()
    }
    
    public func channelInactive(context: ChannelHandlerContext) {
        logger.debug("Channel gets inactive")
        context.close(promise: nil)

        self.state = .disconnected
    }
    
    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        logger.debug("Channel read")
        var byteBuffer = self.unwrapInboundIn(data)
        inputBuffer?.writeBuffer(&byteBuffer)
    }

    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        logger.error("Error caught: \(error.localizedDescription)")

        context.close(promise: nil)
        self.disconnect()
        
        self.state = .disconnected
    }
}


extension NatsClient {

    // MARK: - Implement Internal Methods

    internal func sendMessage(_ message: String) {

        guard self.state == .connected else { return }

        var buffer = self.channel?.allocator.buffer(capacity: message.utf8.count)
        buffer?.writeString(message)
        let _ = self.channel?.writeAndFlush(buffer)

    }

    // MARK: - Implement Private Methods

    fileprivate func updateServerInfo(with info: String) -> Void {
        // TODO: CP - Possibly quit here if reading the config failed.
        // Then disconnect. Not 100% sure when this would happen
        if let config = info.removeNewlines().removePrefix(NatsOperation.info.rawValue).toJsonDicitonary() {
            self.server = NatsServer(config)
            self.fire(.informed)
        }
    }

    fileprivate func handleIncomingMessage(_ messageStr: String) {

        if self.queueCount > self.config.internalQueueMax {
            self.fire(.dropped)
            return
        }

        guard let message = NatsMessage.parse(messageStr) else { return }

        guard let handler = self.subjectHandlerStore[message.subject] else { return }

        self.messageQueue.addOperation {
            handler(message)
        }

    }

}
