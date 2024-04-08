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
        private var buffer: ByteBuffer
        private var allocator: ByteBufferAllocator
        var waitingPromises: [(ClientOp, UnsafeContinuation<Void, Error>)] = []
        var isWriteInProgress: Bool = false

        internal init(allocator: ByteBufferAllocator, batchSize: Int = 16 * 1024) {
            self.allocator = allocator
            self.buffer = allocator.buffer(capacity: batchSize)
        }

        var readableBytes: Int {
            return self.buffer.readableBytes
        }

        mutating func getWriteBuffer() -> ByteBuffer {
            var writeBuffer = allocator.buffer(capacity: buffer.readableBytes)
            writeBuffer.writeBytes(buffer.readableBytesView)
            buffer.clear()

            return writeBuffer
        }

        mutating func writeMessage(_ message: ClientOp) {
            self.buffer.writeClientOp(message)
        }
    }
}

internal class BatchBuffer {
    private let batchSize: Int
    private let channel: Channel
    private let state: NIOLockedValueBox<State>

    init(channel: Channel, batchSize: Int = 16 * 1024) {
        self.batchSize = batchSize
        self.channel = channel
        self.state = .init(
            State(allocator: channel.allocator)
        )
    }

    func writeMessage(_ message: ClientOp) async throws {
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

    private func flushWhenIdle(state: inout State) {
        // The idea is to keep writing to the buffer while a writeAndFlush() is
        // in progress, so we can batch as many messages as possible.
        guard !state.isWriteInProgress else {
            return
        }
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
                    for promise in state.waitingPromises {
                        promise.1.resume(throwing: error)
                    }
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
