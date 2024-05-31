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

class ErrorsTests: XCTestCase {

    static var allTests = [
        ("testServerErrorPermissionsDenied", testServerErrorPermissionsDenied)
    ]

    func testServerErrorPermissionsDenied() {
        var err = NatsError.ServerError(
            "Permissions Violation for Subscription to \"events.A.B.*\"]")
        XCTAssertEqual(err, NatsError.ServerError.permissionsViolation(.subscribe, "events.A.B.*"))

        err = NatsError.ServerError("Permissions Violation for Publish to \"events.A.B.*\"]")
        XCTAssertEqual(err, NatsError.ServerError.permissionsViolation(.publish, "events.A.B.*"))

        err = NatsError.ServerError("Some other error")
        XCTAssertEqual(err, NatsError.ServerError.proto("some other error"))
    }
}
