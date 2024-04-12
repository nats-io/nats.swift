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

print("\n### Setup NATS Connection")

let certsURL = URL(fileURLWithPath: "/Users/mtmk/src/natsbench/server-confs/ws/ca-cert.pem")
let nats = NatsClientOptions()
    .url(URL(string: "wss://localhost:8443")!)
    .rootCertificates(certsURL)
    .build()

nats.on(.connected) { event in
    print("event: connected")
}

print("connecting...")
try await nats.connect()

print("\n### Publish / Subscribe")

print("subscribing...")
let sub = try await nats.subscribe(subject: "foo.>")

let loop = Task {
    print("starting message loop...")

    for try await msg in sub {

        if msg.subject == "foo.done" {
            break
        }

        if let payload = msg.payload {
            print("received \(msg.subject): \(String(data: payload, encoding: .utf8) ?? "")")
        }

        if let headers = msg.headers {
            if let headerValue = headers.get(try! NatsHeaderName("X-Example")) {
                print("  header: X-Example: \(headerValue.description)")
            }
        }
    }

    print("message loop done...")
}

print("publishing data...")
for i in 1...3 {
    var headers = NatsHeaderMap()
    headers.append(try! NatsHeaderName("X-Example"), NatsHeaderValue("example value"))

    if let data = "data\(i)".data(using: .utf8) {
        try await nats.publish(data, subject: "foo.\(i)", headers: headers)
    }
}

print("signalling done...")
try await nats.publish(Data(), subject: "foo.done")

try await loop.value

print("closing...")
try await nats.close()

print("bye")
