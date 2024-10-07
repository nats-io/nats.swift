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

extension Stream {

    /// Creates a consumer with the specified configuration.
    ///
    /// - Parameters:
    ///  - cfg: consumer config
    ///
    /// - Returns: ``Consumer`` object containing ``ConsumerConfig`` and exposing operations on the consumer
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError``: if there was am error creating the stream. There are several errors which may occur, most common being:
    /// >   - ``JetStreamError/ConsumerError/invalidConfig(_:)``: if the provided configuration is not valid.
    /// >   - ``JetStreamError/ConsumerError/consumerNameExist(_:)``: if attempting to overwrite an existing consumer (with different configuration)
    /// >   - ``JetStreamError/ConsumerError/maximumConsumersLimit(_:)``: if a max number of consumers (specified on stream/account level) has been reached.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func createConsumer(cfg: ConsumerConfig) async throws -> Consumer {
        return try await ctx.upsertConsumer(stream: info.config.name, cfg: cfg, action: "create")
    }

    /// Updates an existing consumer using specified config.
    ///
    /// - Parameters:
    ///  - cfg: consumer config
    ///
    /// - Returns: ``Consumer`` object containing ``ConsumerConfig`` and exposing operations on the consumer
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError``: if there was am error creating the stream. There are several errors which may occur, most common being:
    /// >   - ``JetStreamError/ConsumerError/invalidConfig(_:)``: if the provided configuration is not valid or atteppting to update an illegal property
    /// >   - ``JetStreamError/ConsumerError/consumerDoesNotExist(_:)``: if attempting to update a non-existing consumer
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func updateConsumer(cfg: ConsumerConfig) async throws -> Consumer {
        return try await ctx.upsertConsumer(stream: info.config.name, cfg: cfg, action: "update")
    }

    /// Creates a consumer with the specified configuration or updates an existing consumer.
    ///
    /// - Parameters:
    ///  - stream: name of the stream the consumer will be created on
    ///  - cfg: consumer config
    ///
    /// - Returns: ``Consumer`` object containing ``ConsumerConfig`` and exposing operations on the consumer
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError``: if there was am error creating the stream. There are several errors which may occur, most common being:
    /// >   - ``JetStreamError/ConsumerError/invalidConfig(_:)``: if the provided configuration is not valid  or atteppting to update an illegal property
    /// >   - ``JetStreamError/ConsumerError/maximumConsumersLimit(_:)``: if a max number of consumers (specified on stream/account level) has been reached.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func createOrUpdateConsumer(cfg: ConsumerConfig) async throws -> Consumer {
        return try await ctx.upsertConsumer(stream: info.config.name, cfg: cfg)
    }

    /// Retrieves a consumer with given name from a stream.
    ///
    /// - Parameters:
    ///  - name: name of the stream
    ///
    /// - Returns a ``Stream`` object containing ``StreamInfo`` and exposing operations on the stream or nil if stream with given name does not exist.
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError/consumerNotFound(_:)``: if the consumer with given name does not exist on a given stream.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func getConsumer(name: String) async throws -> Consumer? {
        return try await ctx.getConsumer(stream: info.config.name, name: name)
    }

    /// Deletes a consumer from a stream.
    ///
    /// - Parameters:
    ///  - name: consumer name
    ///
    /// > **Throws:**
    /// > - ``JetStreamError/ConsumerError/consumerNotFound(_:)``: if the consumer with given name does not exist on a given stream.
    /// > - ``JetStreamError/RequestError``: if the request fails if e.g. JetStream is not enabled.
    /// > - ``JetStreamError/APIError``: if there was a different API error returned from JetStream.
    public func deleteConsumer(name: String) async throws {
        try await ctx.deleteConsumer(stream: info.config.name, name: name)
    }

    /// Used to list consumer names.
    ///
    /// - Parameters:
    ///  - stream: the name of the strem to list the consumers from.
    ///
    /// - Returns ``Consumers`` which implements AsyncSequence allowing iteration over stream infos.
    public func consumers() async -> Consumers {
        return Consumers(ctx: ctx, stream: info.config.name)
    }

    /// Used to list consumer names.
    ///
    /// - Parameters:
    ///  - stream: the name of the strem to list the consumers from.
    ///
    /// - Returns ``ConsumerNames`` which implements AsyncSequence allowing iteration over consumer names.
    public func consumerNames() async -> ConsumerNames {
        return ConsumerNames(ctx: ctx, stream: info.config.name)
    }
}
