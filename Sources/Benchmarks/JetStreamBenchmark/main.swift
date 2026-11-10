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

// Measures the throughput of JetStream operations against a server with JetStream enabled.
//
//   nats-server -js
//   swift run -c release JetStreamBenchmark
//
// Environment: NATS_URL (nats://localhost:4222), MESSAGES (10000), SIZE (128 bytes),
// OBJECT_SIZE (16 MiB).

import Foundation
import JetStream
import JetStreamKeyValue
import JetStreamObjectStore
import Nats

let environment = ProcessInfo.processInfo.environment
let url = environment["NATS_URL"] ?? "nats://localhost:4222"
let messages = Int(environment["MESSAGES"] ?? "") ?? 10_000
let size = Int(environment["SIZE"] ?? "") ?? 128
let objectSize = Int(environment["OBJECT_SIZE"] ?? "") ?? 16 * 1024 * 1024
let payload = Data(repeating: 65, count: size)

logger.logLevel = .error
let client = NatsClientOptions().url(URL(string: url)!).build()
try await client.connect()
let js = JetStreamContext(client: client)

/// Runs `body`, which processes `count` messages of `bytes` bytes, and prints its throughput.
func measure(_ name: String, count: Int, bytes: Int, _ body: () async throws -> Void) async throws {
    let start = ContinuousClock.now
    try await body()
    let elapsed = ContinuousClock.now - start
    let seconds =
        Double(elapsed.components.seconds) + Double(elapsed.components.attoseconds) / 1e18
    let rate = Double(count) / seconds
    let throughput = Double(count * bytes) / seconds / 1_048_576
    let line =
        name.padding(toLength: 34, withPad: " ", startingAt: 0)
        + String(format: "%10.0f msgs/s %9.2f MB/s %8.3f s", rate, throughput, seconds)
    print(line)
}

func recreateStream(_ name: String, atomic: Bool = false) async throws -> JetStream.Stream {
    if try await js.getStream(name: name) != nil {
        try await js.deleteStream(name: name)
    }
    return try await js.createStream(
        cfg: StreamConfig(
            name: name, subjects: ["\(name).>"], storage: .file,
            allowAtomicPublish: atomic ? true : nil))
}

print("JetStream benchmark: \(messages) messages of \(size) bytes, server \(url)\n")

// publishing, waiting for each acknowledgement
_ = try await recreateStream("bench")
try await measure("publish (sync)", count: messages, bytes: size) {
    for _ in 0..<messages {
        _ = try await js.publish("bench.sync", message: payload).wait()
    }
}

// publishing, collecting acknowledgements at the end
_ = try await recreateStream("bench")
try await measure("publish (async)", count: messages, bytes: size) {
    var futures: [AckFuture] = []
    futures.reserveCapacity(messages)
    for _ in 0..<messages {
        futures.append(try await js.publish("bench.async", message: payload))
    }
    await js.publishAsyncComplete()
    for future in futures {
        _ = try await future.wait()
    }
}

// atomic batches of 100 messages
let stream = try await recreateStream("bench", atomic: true)
try await measure("publish (atomic batches of 100)", count: messages, bytes: size) {
    for _ in 0..<(messages / 100) {
        let batch = js.batchPublisher()
        for _ in 0..<99 {
            try await batch.add("bench.batch", message: payload)
        }
        _ = try await batch.commit("bench.batch", message: payload)
    }
}

// consuming with pipelined pull requests
let consumer = try await stream.createConsumer(
    cfg: ConsumerConfig(name: "bench", ackPolicy: .none))
try await measure("consume (no ack)", count: messages, bytes: size) {
    var received = 0
    for try await _ in try await consumer.consume(options: ConsumeOptions(maxMessages: 1000)) {
        received += 1
        if received == messages {
            break
        }
    }
}

let acked = try await stream.createConsumer(cfg: ConsumerConfig(name: "acked"))
try await measure("consume (ack)", count: messages, bytes: size) {
    var received = 0
    for try await message in try await acked.consume(options: ConsumeOptions(maxMessages: 1000)) {
        try await message.ack()
        received += 1
        if received == messages {
            break
        }
    }
}

try await measure("fetch (batches of 500)", count: messages, bytes: size) {
    let fetcher = try await stream.createConsumer(
        cfg: ConsumerConfig(name: "fetcher", ackPolicy: .none))
    var received = 0
    while received < messages {
        for try await _ in try await fetcher.fetch(batch: 500, expires: 5) {
            received += 1
        }
    }
}

let ordered = stream.orderedConsumer()
try await measure("ordered consumer", count: messages, bytes: size) {
    var received = 0
    for try await _ in try await ordered.consume(options: ConsumeOptions(maxMessages: 1000)) {
        received += 1
        if received == messages {
            break
        }
    }
}

// key-value store
if try await js.keyValue(bucket: "bench") != nil {
    try await js.deleteKeyValue(bucket: "bench")
}
let kv = try await js.createKeyValue(cfg: KeyValueConfig(bucket: "bench"))
try await measure("kv put", count: messages, bytes: size) {
    for i in 0..<messages {
        try await kv.put("key.\(i % 1000)", payload)
    }
}
try await measure("kv get", count: messages, bytes: size) {
    for i in 0..<messages {
        guard try await kv.get("key.\(i % 1000)") != nil else {
            fatalError("missing key")
        }
    }
}

// object store
if try await js.objectStore(bucket: "bench") != nil {
    try await js.deleteObjectStore(bucket: "bench")
}
let objects = try await js.createObjectStore(cfg: ObjectStoreConfig(bucket: "bench"))
let blob = Data(repeating: 66, count: objectSize)
let chunks =
    (objectSize + Int(ObjectStore.defaultChunkSize) - 1) / Int(ObjectStore.defaultChunkSize)
try await measure(
    "object put (\(objectSize / 1_048_576) MiB)", count: chunks,
    bytes: Int(ObjectStore.defaultChunkSize)
) {
    try await objects.put(name: "blob", data: blob)
}
try await measure(
    "object get (\(objectSize / 1_048_576) MiB)", count: chunks,
    bytes: Int(ObjectStore.defaultChunkSize)
) {
    guard try await objects.getData("blob")?.count == objectSize else {
        fatalError("incomplete object")
    }
}

try await js.deleteStream(name: "bench")
try await js.deleteKeyValue(bucket: "bench")
try await js.deleteObjectStore(bucket: "bench")
try await client.close()
