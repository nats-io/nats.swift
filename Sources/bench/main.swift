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

if CommandLine.arguments.count != 4 {
    exit(usage())
}

let cmd = CommandLine.arguments[1]

if cmd != "pub" {
    exit(usage())
}

let subject = CommandLine.arguments[2]
guard let msgs = Int(CommandLine.arguments[3]) else {
    exit(usage())
}

let nats = ClientOptions()
    .url(URL(string: ProcessInfo.processInfo.environment["NATS_URL"] ?? "nats://localhost:4222")!)
    .build()

print("connect")
try await nats.connect()

let data = Data(repeating: 0, count: 128)

print("start")
let now = DispatchTime.now()

if cmd == "pub" {
    try await pub()
}

let elapsed = DispatchTime.now().uptimeNanoseconds - now.uptimeNanoseconds
let msgsPerSec: Double = Double(msgs) / (Double(elapsed) / 1_000_000_000)
print("elapsed: \(elapsed / 1_000_000)ms ~ \(msgsPerSec) msgs/s")

func pub() async throws {
    print("publish")
    for _ in 1...msgs {
        try await nats.publish(data, subject: subject)
    }

    print("flush")
    _ = try await nats.rtt()
}

func usage() -> Int32 {
    print("Usage: bench pub <subject> <msgs>")
    return 2
}
