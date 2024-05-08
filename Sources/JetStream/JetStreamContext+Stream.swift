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
}
