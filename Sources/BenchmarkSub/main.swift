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

let nats = ClientOptions().url(URL(string: "nats://localhost:4222")!).build()
print("Connecting...")
try! await nats.connect()
print("Connected!")

let data = "foo".data(using: .utf8)!
// Warmup
print("Starting benchmark...")
print("Waiting for first message...")
var now = DispatchTime.now()
let numMsgs = 1_000_000
let sub = try await nats.subscribe(to: "foo").makeAsyncIterator()
var hm = HeaderMap()
hm.append(try! HeaderName("foo"), HeaderValue("bar"))
hm.append(try! HeaderName("foo"), HeaderValue("baz"))
for i in 1...numMsgs {
    let msg = await sub.next()
    if i == 0 {
        print("Received first message! Starting the timer")
        now = DispatchTime.now()
    }
    guard let payload = msg?.payload else {
        print("empty payload!")
        continue
    }
    if String(data: payload, encoding: .utf8) != "\(i)" {
        let emptyString = ""
        print(
            "invalid payload; expected: \(i); got: \(String(data: payload, encoding: .utf8) ?? emptyString)"
        )
    }
    guard msg?.headers != nil else {
        print("empty headers!")
        continue
    }
    if i % 1000 == 0 {
        print("received \(i) msgs")
    }
}

let elapsed = DispatchTime.now().uptimeNanoseconds - now.uptimeNanoseconds
let msgsPerSec: Double = Double(numMsgs) / (Double(elapsed) / 1_000_000_000)
print("Elapsed: \(elapsed / 1_000_000)ms")
print("\(msgsPerSec) msgs/s")
