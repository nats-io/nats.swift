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
    /// Errors of the Object Store.
    public enum ObjectStoreError: JetStreamErrorProtocol {
        /// The bucket name contains characters which are not allowed.
        case invalidStoreName(String)
        /// The bucket does not exist.
        case bucketNotFound(String)
        /// A bucket with the name and a different configuration exists.
        case bucketExists(String)
        /// A name is required.
        case nameRequired
        /// The object does not exist.
        case objectNotFound(String)
        /// The metadata of the object is invalid.
        case badObjectMeta(String)
        /// The content of the object does not match its digest.
        case digestMismatch(String)
        /// Links cannot be stored with put, use ``ObjectStore/addLink(_:to:)``.
        case linkNotAllowed
        /// The object is a link to a bucket and has no content.
        case cantGetBucket(String)
        /// An object with the name already exists.
        case objectAlreadyExists(String)
        /// The object to update does not exist or was deleted.
        case updateMetaDeleted(String)
        /// Links to deleted objects are not allowed.
        case noLinkToDeleted
        /// Links to links are not allowed.
        case noLinkToLink

        /// A description of the error.
        public var description: String {
            switch self {
            case .invalidStoreName(let bucket):
                return "nats: invalid object-store name: \(bucket)"
            case .bucketNotFound(let bucket):
                return "nats: bucket not found: \(bucket)"
            case .bucketExists(let bucket):
                return "nats: bucket name already in use: \(bucket)"
            case .nameRequired:
                return "nats: object name is required"
            case .objectNotFound(let name):
                return "nats: object not found: \(name)"
            case .badObjectMeta(let name):
                return "nats: object-store meta information invalid: \(name)"
            case .digestMismatch(let name):
                return "nats: received a corrupt object, digests do not match: \(name)"
            case .linkNotAllowed:
                return "nats: link cannot be set when putting the object in bucket"
            case .cantGetBucket(let name):
                return "nats: invalid get, object is a link to a bucket: \(name)"
            case .objectAlreadyExists(let name):
                return "nats: an object already exists with that name: \(name)"
            case .updateMetaDeleted(let name):
                return "nats: cannot update meta for a deleted object: \(name)"
            case .noLinkToDeleted:
                return "nats: not allowed to link to a deleted object"
            case .noLinkToLink:
                return "nats: not allowed to link to another link"
            }
        }
    }
}
