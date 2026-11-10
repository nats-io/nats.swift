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

import JetStream

extension JetStreamError {
    /// Errors of the Key-Value Store.
    public enum KeyValueError: JetStreamErrorProtocol {
        /// The bucket name contains characters which are not allowed.
        case invalidBucketName(String)
        /// The key contains characters which are not allowed.
        case invalidKey(String)
        /// The bucket does not exist.
        case bucketNotFound(String)
        /// A bucket with the name and a different configuration exists.
        case bucketExists(String)
        /// The stream of the bucket is not a valid key-value bucket.
        case badBucket(String)
        /// The history exceeds ``KeyValueConfig/maxHistory``.
        case historyTooLarge
        /// The server does not support limit marker TTLs.
        case limitMarkerTTLNotSupported
        /// The key to create already exists.
        case keyExists(String)
        /// The latest revision of the key did not match the expected one.
        case wrongLastRevision(String)

        /// A description of the error.
        public var description: String {
            switch self {
            case .invalidBucketName(let bucket):
                return "nats: invalid bucket name: \(bucket)"
            case .invalidKey(let key):
                return "nats: invalid key: \(key)"
            case .bucketNotFound(let bucket):
                return "nats: bucket not found: \(bucket)"
            case .bucketExists(let bucket):
                return "nats: bucket name already in use: \(bucket)"
            case .badBucket(let bucket):
                return "nats: bucket not valid key-value store: \(bucket)"
            case .historyTooLarge:
                return "nats: history limited to a max of \(KeyValueConfig.maxHistory)"
            case .limitMarkerTTLNotSupported:
                return "nats: limit marker TTLs not supported by server"
            case .keyExists(let key):
                return "nats: key exists: \(key)"
            case .wrongLastRevision(let key):
                return "nats: wrong last revision of key: \(key)"
            }
        }
    }
}
