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

import XCTest

@testable import JetStream
@testable import Nats

class JetStreamMessageTests: XCTestCase {

    static var allTests = [
        ("testValidOldFormatMessage", testValidOldFormatMessage),
        ("testValidNewFormatMessage", testValidNewFormatMessage),
        ("testMissingTokens", testMissingTokens),
        ("testInvalidTokenValues", testInvalidTokenValues),
        ("testInvalidPrefix", testInvalidPrefix),
        ("testNoReplySubject", testNoReplySubject),
    ]

    func testValidOldFormatMessage() async throws {
        let replySubject = "$JS.ACK.myStream.myConsumer.10.20.30.1234567890.5"
        let natsMessage = NatsMessage(
            payload: nil, subject: "", replySubject: replySubject, length: 0, headers: nil,
            status: nil, description: nil)
        let jetStreamMessage = JetStreamMessage(message: natsMessage, client: NatsClient())

        let metadata = try jetStreamMessage.metadata()

        XCTAssertNil(metadata.domain)
        XCTAssertNil(metadata.accountHash)
        XCTAssertEqual(metadata.stream, "myStream")
        XCTAssertEqual(metadata.consumer, "myConsumer")
        XCTAssertEqual(metadata.delivered, 10)
        XCTAssertEqual(metadata.streamSequence, 20)
        XCTAssertEqual(metadata.consumerSequence, 30)
        XCTAssertEqual(metadata.timestamp, "1234567890")
        XCTAssertEqual(metadata.pending, 5)
    }

    func testValidNewFormatMessage() async throws {
        let replySubject = "$JS.ACK.domain.accountHash123.myStream.myConsumer.10.20.30.1234567890.5"
        let natsMessage = NatsMessage(
            payload: nil, subject: "", replySubject: replySubject, length: 0, headers: nil,
            status: nil, description: nil)
        let jetStreamMessage = JetStreamMessage(message: natsMessage, client: NatsClient())
        let metadata = try jetStreamMessage.metadata()

        XCTAssertEqual(metadata.domain, "domain")
        XCTAssertEqual(metadata.accountHash, "accountHash123")
        XCTAssertEqual(metadata.stream, "myStream")
        XCTAssertEqual(metadata.consumer, "myConsumer")
        XCTAssertEqual(metadata.delivered, 10)
        XCTAssertEqual(metadata.streamSequence, 20)
        XCTAssertEqual(metadata.consumerSequence, 30)
        XCTAssertEqual(metadata.timestamp, "1234567890")
        XCTAssertEqual(metadata.pending, 5)
    }

    func testMissingTokens() async throws {
        let replySubject = "$JS.ACK.myStream.myConsumer"
        let natsMessage = NatsMessage(
            payload: nil, subject: "", replySubject: replySubject, length: 0, headers: nil,
            status: nil, description: nil)
        let jetStreamMessage = JetStreamMessage(message: natsMessage, client: NatsClient())
        do {
            _ = try jetStreamMessage.metadata()
        } catch JetStreamError.MessageMetadataError.invalidTokenNum {
            return
        }
    }

    func testInvalidTokenValues() async throws {
        let replySubject = "$JS.ACK.myStream.myConsumer.invalid.20.30.1234567890.5"
        let natsMessage = NatsMessage(
            payload: nil, subject: "", replySubject: replySubject, length: 0, headers: nil,
            status: nil, description: nil)
        let jetStreamMessage = JetStreamMessage(message: natsMessage, client: NatsClient())
        do {
            _ = try jetStreamMessage.metadata()
        } catch JetStreamError.MessageMetadataError.invalidTokenValue {
            return
        }
    }

    func testInvalidPrefix() async throws {
        let replySubject = "$JS.WRONG.myStream.myConsumer.10.20.30.1234567890.5"
        let natsMessage = NatsMessage(
            payload: nil, subject: "", replySubject: replySubject, length: 0, headers: nil,
            status: nil, description: nil)
        let jetStreamMessage = JetStreamMessage(message: natsMessage, client: NatsClient())
        do {
            _ = try jetStreamMessage.metadata()
        } catch JetStreamError.MessageMetadataError.invalidPrefix {
            return
        }
    }

    func testNoReplySubject() async throws {
        let natsMessage = NatsMessage(
            payload: nil, subject: "", replySubject: nil, length: 0, headers: nil, status: nil,
            description: nil)
        let jetStreamMessage = JetStreamMessage(message: natsMessage, client: NatsClient())
        do {
            _ = try jetStreamMessage.metadata()
        } catch JetStreamError.MessageMetadataError.noReplyInMessage {
            return
        }
    }
}
