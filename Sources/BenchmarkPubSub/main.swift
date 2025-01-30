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
import Nats

let nats = NatsClientOptions().urls([URL(string: "nats://localhost:4222")!]).build()
print("Connecting...")
try! await nats.connect()
print("Connected!")

let data = "foo".data(using: .utf8)!
// Warmup
print("Warming up...")
for _ in 0..<10_000 {
    try! await nats.publish(data, subject: "foo")
}
print("Starting benchmark...")
let now = DispatchTime.now()
let numMsgs = 1_000_000
let sub = try await nats.subscribe(subject: "foo")
try await withThrowingTaskGroup(of: Void.self) { group in
    group.addTask {
        var hm = NatsHeaderMap()
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("bar"))
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("baz"))
        hm.insert(try! NatsHeaderName("another"), NatsHeaderValue("one"))
        var i = 0
        for try await msg in await sub {
            let payload = msg.payload!
            if String(data: payload, encoding: .utf8) != "\(i)" {
                let emptyString = ""
                print(
                    "invalid payload; expected: \(i); got: \(String(data: payload, encoding: .utf8) ?? emptyString)"
                )
            }
            guard let headers = msg.headers else {
                print("empty headers!")
                continue
            }
            if headers != hm {
                print("invalid headers; expected: \(hm); got: \(headers)")
            }
            if i % 1000 == 0 {
                print("received \(i) msgs")
            }
            i += 1
        }
    }

    group.addTask {
        var hm = NatsHeaderMap()
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("bar"))
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("baz"))
        hm.insert(try! NatsHeaderName("another"), NatsHeaderValue("one"))
        for i in 0..<numMsgs {
            try await nats.publish("\(i)".data(using: .utf8)!, subject: "foo", headers: hm)
            if i % 1000 == 0 {
                print("published \(i) msgs")
            }
        }
    }

    // Wait for all tasks in the group to complete
    try await group.waitForAll()
}

try! await nats.flush()
let elapsed = DispatchTime.now().uptimeNanoseconds - now.uptimeNanoseconds
let msgsPerSec: Double = Double(numMsgs) / (Double(elapsed) / 1_000_000_000)
print("Elapsed: \(elapsed / 1_000_000)ms")
print("\(msgsPerSec) msgs/s")
