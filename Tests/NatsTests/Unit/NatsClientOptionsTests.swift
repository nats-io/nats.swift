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

@testable import Nats

class NatsClientOptionsTests: XCTestCase {
    static var allTests = [
        ("testDefaultInboxPrefix", testDefaultInboxPrefix),
        ("testCustomInboxPrefix", testCustomInboxPrefix),
    ]

    func testDefaultInboxPrefix() {
        let client = NatsClientOptions().build()
        let inbox = client.newInbox()
        XCTAssertTrue(inbox.hasPrefix("_INBOX."), "Default inbox prefix should be '_INBOX.'")
        XCTAssertEqual(inbox.count, "_INBOX.".count + 22, "Inbox should have prefix plus NUID")
    }

    func testCustomInboxPrefix() {
        let customPrefix = "_INBOX_abc123."
        let client = NatsClientOptions().inboxPrefix(customPrefix).build()
        let inbox = client.newInbox()
        XCTAssertTrue(inbox.hasPrefix(customPrefix), "Inbox should use custom prefix")
        XCTAssertEqual(
            inbox.count, customPrefix.count + 22, "Inbox should have custom prefix plus NUID")
    }
}
