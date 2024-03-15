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

let nats = ClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()
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
let numMsgs = 10_000_000
for _ in 0..<numMsgs {
    try! await nats.publish(data, subject: "x")
}
try! await nats.flush()
let elapsed = DispatchTime.now().uptimeNanoseconds - now.uptimeNanoseconds
let msgsPerSec: Double = Double(numMsgs) / (Double(elapsed) / 1_000_000_000)
print("Elapsed: \(elapsed / 1_000_000)ms")
print("\(msgsPerSec) msgs/s")
