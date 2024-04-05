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

import Foundation
import NIO
import NIOConcurrencyHelpers

extension BatchBuffer {
    struct State {
        enum Index {
            case first
            case second
        }

        private var firstBuffer: ByteBuffer
        private var secondBuffer: ByteBuffer
        var index: Index = .first
        var waitingPromises: [(ClientOp, UnsafeContinuation<Void, Error>)] = []
        var isWriteInProgress: Bool = false
        var writeStallsForBufferSpace = 0
        var writeStallCurrentlyWriting = 0
        var totalFlushes = 0

        internal init(firstBuffer: ByteBuffer, secondBuffer: ByteBuffer) {
            self.firstBuffer = firstBuffer
            self.secondBuffer = secondBuffer
        }

        var readableBytes: Int {
            switch index {
            case .first:
                return self.firstBuffer.readableBytes
            case .second:
                return self.secondBuffer.readableBytes
            }
        }

        mutating func clear() {
            self.firstBuffer.clear()
            self.secondBuffer.clear()
        }

        mutating func getWriteBuffer() -> ByteBuffer {
            switch index {
                case .first:
                    index = .second
                    self.secondBuffer.moveReaderIndex(to: 0)
                    self.secondBuffer.moveWriterIndex(to: 0)
                    let buffer = self.firstBuffer
                    return buffer
                case .second:
                    index = .first
                    self.firstBuffer.moveReaderIndex(to: 0)
                    self.firstBuffer.moveWriterIndex(to: 0)
                    let buffer = self.secondBuffer
                    return buffer
            }
        }

        mutating func writeMessage(_ message: ClientOp) {
            switch index {
            case .first:
                self.firstBuffer.writeClientOp(message)
            case .second:
                self.secondBuffer.writeClientOp(message)
            }
        }
    }
}

internal class BatchBuffer {
    private let batchSize: Int
    private let channel: Channel
    private let state: NIOLockedValueBox<State>
    var writeStallsForBufferSpace: Int {
        self.state.withLockedValue {
            $0.writeStallsForBufferSpace
        }
    }
    var writeStallCurrentlyWriting: Int {
        self.state.withLockedValue {
            $0.writeStallCurrentlyWriting
        }
    }
    var totalFlushes: Int {
        self.state.withLockedValue {
            $0.totalFlushes
        }
    }

    init(channel: Channel, batchSize: Int = 16 * 1024) {
        self.batchSize = batchSize
        self.channel = channel
        self.state = .init(
            State(
                firstBuffer: channel.allocator.buffer(capacity: batchSize),
                secondBuffer: channel.allocator.buffer(capacity: batchSize))
        )
    }

    func writeMessage(_ message: ClientOp) async throws {
        try await self.write(message)
    }

    private func write(_ message: ClientOp) async throws {
        #if SWIFT_NATS_BATCH_BUFFER_DISABLED
            let b = channel.allocator.buffer(bytes: data)
            try await channel.writeAndFlush(b)
        #else
            // Batch writes and if we have more than the batch size
            // already in the buffer await until buffer is flushed
            // to handle any back pressure
            try await withUnsafeThrowingContinuation { continuation in
                self.state.withLockedValue { state in
                    guard state.readableBytes < self.batchSize else {
                        state.writeStallsForBufferSpace &+= 1
                        state.waitingPromises.append((message, continuation))
                        return
                    }

                    state.writeMessage(message)
                    self.flushWhenIdle(state: &state)
                    continuation.resume()
                }

            }
        #endif
    }

    func clear() {
        self.state.withLockedValue {
            $0.clear()
        }
    }

    private func flushWhenIdle(state: inout State) {
        // The idea is to keep writing to the buffer while a writeAndFlush() is
        // in progress, so we can batch as many messages as possible.
        guard !state.isWriteInProgress else {
            state.writeStallCurrentlyWriting &+= 1
            return
        }
        state.totalFlushes &+= 1
        // We need a separate write buffer so we can free the message buffer for more
        // messages to be collected.
        let writeBuffer = state.getWriteBuffer()
        state.isWriteInProgress = true

        let writePromise = self.channel.eventLoop.makePromise(of: Void.self)
        writePromise.futureResult.whenComplete { result in
            self.state.withLockedValue { state in
                state.isWriteInProgress = false
                switch result {
                case .success:
                    for (message, continuation) in state.waitingPromises {
                        state.writeMessage(message)
                        continuation.resume()
                    }
                    state.waitingPromises.removeAll()
                case .failure(let error):
                    state.waitingPromises.forEach { $0.1.resume(throwing: error) }
                    state.waitingPromises.removeAll()
                }

                // Check if there are any pending flushes
                if state.readableBytes > 0 {
                    self.flushWhenIdle(state: &state)
                }
            }
        }

        self.channel.writeAndFlush(writeBuffer, promise: writePromise)
    }
}
