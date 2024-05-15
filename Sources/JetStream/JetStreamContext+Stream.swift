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

/// Extension to `JetStreamContext` adding stream management functionalities.
extension JetStreamContext {

    /// Creates a stream with the specified configuration.
    /// Throws an error if the stream configuration is invalid or a stream with given name already exists and has different configuration.
    public func createStream(cfg: StreamConfig) async throws -> Stream {
        try Stream.validate(name: cfg.name)
        let req = try! JSONEncoder().encode(cfg)
        let subj = "STREAM.CREATE.\(cfg.name)"
        let info: Response<StreamInfo> = try await request(subj, message: req)
        switch info {
        case .success(let info):
            return Stream(ctx: self, info: info)
        case .error(let apiResponse):
            throw apiResponse.error
        }
    }

    /// Retrieves a stream by its name.
    /// Throws an error if the stream does not exist.
    public func getStream(name: String) async throws -> Stream {
        try Stream.validate(name: name)
        let subj = "STREAM.INFO.\(name)"
        let info: Response<StreamInfo> = try await request(subj)
        switch info {
        case .success(let info):
            return Stream(ctx: self, info: info)
        case .error(let apiResponse):
            throw apiResponse.error
        }
    }

    /// Updates an existing stream with new configuration.
    /// Throws an error if the stream configuration is invalid or if the stream with provided name does not exist.
    public func updateStream(cfg: StreamConfig) async throws -> Stream {
        try Stream.validate(name: cfg.name)
        let req = try! JSONEncoder().encode(cfg)
        let subj = "STREAM.UPDATE.\(cfg.name)"
        let info: Response<StreamInfo> = try await request(subj, message: req)
        switch info {
        case .success(let info):
            return Stream(ctx: self, info: info)
        case .error(let apiResponse):
            throw apiResponse.error
        }
    }

    /// Deletes a stream by its name.
    /// Throws an error if the stream does not exist.
    public func deleteStream(name: String) async throws {
        try Stream.validate(name: name)
        let subj = "STREAM.DELETE.\(name)"
        let info: Response<StreamDeleteResponse> = try await request(subj)
        switch info {
        case .success(_):
            return
        case .error(let apiResponse):
            throw apiResponse.error
        }
    }

    struct StreamDeleteResponse: Codable {
        let success: Bool
    }

    /// Used to list stream infos.
    /// Returns an AsyncSequence allowing iteration over streams.
    /// Subject can be provided to filter the response.
    public func streams(subject: String? = nil) async -> Streams {
        return Streams(ctx: self, subject: subject)
    }

    /// Used to list stream names.
    /// Returns an AsyncSequence allowing iteration over streams.
    /// Subject can be provided to filter the response.
    public func streamNames(subject: String? = nil) async -> StreamNames {
        return StreamNames(ctx: self, subject: subject)
    }
}

internal struct StreamsPagedRequest: Codable {
    let offset: Int
    let subject: String?
}

public struct Streams: AsyncSequence {
    public typealias Element = StreamInfo
    public typealias AsyncIterator = StreamsIterator

    private let ctx: JetStreamContext
    private let subject: String?
    private var buffer: [StreamInfo]
    private var offset: Int
    private var total: Int?

    struct StreamsInfoPage: Codable {
        let total: Int
        let streams: [StreamInfo]?
    }

    init(ctx: JetStreamContext, subject: String?) {
        self.ctx = ctx
        self.subject = subject
        self.buffer = []
        self.offset = 0
    }

    public func makeAsyncIterator() -> StreamsIterator {
        return StreamsIterator(seq: self)
    }

    public mutating func next() async throws -> Element? {
        if let stream = buffer.first {
            buffer.removeFirst()
            return stream
        }

        if let total = self.total, self.offset >= total {
            return nil
        }

        // poll streams
        let request = StreamsPagedRequest(offset: offset, subject: subject)

        let res: Response<StreamsInfoPage> = try await ctx.request(
            "STREAM.LIST", message: JSONEncoder().encode(request))
        switch res {
        case .success(let infos):
            guard let streams = infos.streams else {
                return nil
            }
            self.offset += streams.count
            self.total = infos.total
            buffer.append(contentsOf: streams)
            return try await self.next()
        case .error(let err):
            throw err.error
        }

    }

    public struct StreamsIterator: AsyncIteratorProtocol {
        var seq: Streams

        public mutating func next() async throws -> Element? {
            try await seq.next()
        }
    }
}

public struct StreamNames: AsyncSequence {
    public typealias Element = String
    public typealias AsyncIterator = StreamNamesIterator

    private let ctx: JetStreamContext
    private let subject: String?
    private var buffer: [String]
    private var offset: Int
    private var total: Int?

    struct StreamNamesPage: Codable {
        let total: Int
        let streams: [String]?
    }

    init(ctx: JetStreamContext, subject: String?) {
        self.ctx = ctx
        self.subject = subject
        self.buffer = []
        self.offset = 0
    }

    public func makeAsyncIterator() -> StreamNamesIterator {
        return StreamNamesIterator(seq: self)
    }

    public mutating func next() async throws -> Element? {
        if let stream = buffer.first {
            buffer.removeFirst()
            return stream
        }

        if let total = self.total, self.offset >= total {
            return nil
        }

        // poll streams
        let request = StreamsPagedRequest(offset: offset, subject: subject)

        let res: Response<StreamNamesPage> = try await ctx.request(
            "STREAM.NAMES", message: JSONEncoder().encode(request))
        switch res {
        case .success(let names):
            guard let streams = names.streams else {
                return nil
            }
            self.offset += streams.count
            self.total = names.total
            buffer.append(contentsOf: streams)
            return try await self.next()
        case .error(let err):
            throw err.error
        }

    }

    public struct StreamNamesIterator: AsyncIteratorProtocol {
        var seq: StreamNames

        public mutating func next() async throws -> Element? {
            try await seq.next()
        }
    }
}
