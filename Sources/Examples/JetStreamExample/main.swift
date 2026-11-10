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
import JetStream
import JetStreamKeyValue
import JetStreamObjectStore
import Nats

// Requires a server with JetStream enabled, e.g. `nats-server -js`.
let url = ProcessInfo.processInfo.environment["NATS_URL"] ?? "nats://localhost:4222"
let nats = NatsClientOptions().url(URL(string: url)!).build()
try await nats.connect()
let js = JetStreamContext(client: nats)

print("\n### Streams")
let stream = try await js.createOrUpdateStream(
    cfg: StreamConfig(name: "ORDERS", subjects: ["orders.>"], duplicates: NanoTimeInterval(60)))
print("stream \(stream.info.config.name) has \(stream.info.state.messages) messages")

print("\n### Publish")
for i in 1...5 {
    // the message id deduplicates retried publishes
    let ack = try await js.publish(
        "orders.new", message: Data("order \(i)".utf8),
        options: PublishOptions(messageId: "order-\(i)")
    ).wait()
    print("stored in \(ack.stream) at sequence \(ack.seq), duplicate: \(ack.duplicate)")
}

print("\n### Consume")
let consumer = try await stream.createOrUpdateConsumer(
    cfg: ConsumerConfig(durable: "processor", ackPolicy: .explicit))
var processed = 0
for try await msg in try await consumer.consume(options: ConsumeOptions(maxMessages: 100)) {
    print("processing \(String(decoding: msg.payload ?? Data(), as: UTF8.self))")
    try await msg.ack()
    processed += 1
    if processed == 5 {
        break
    }
}

print("\n### Fetch and next")
// a pull request of the consume loop above may still be pending on the server,
// so a separate consumer is used here
let auditor = try await stream.createOrUpdateConsumer(cfg: ConsumerConfig(durable: "auditor"))
for try await msg in try await auditor.fetch(batch: 2, expires: 1) {
    print("fetched \(String(decoding: msg.payload ?? Data(), as: UTF8.self))")
    try await msg.ack()
}
if let msg = try await auditor.next(expires: 1) {
    let meta = try msg.metadata()
    print("next message at stream sequence \(meta.streamSequence)")
    try await msg.doubleAck()
}

print("\n### Ordered consumer")
let ordered = stream.orderedConsumer(config: OrderedConsumerConfig(deliverPolicy: .all))
for try await msg in try await ordered.fetch(batch: 3, expires: 1) {
    print("replayed \(String(decoding: msg.payload ?? Data(), as: UTF8.self))")
}

print("\n### Key-value store")
let kv = try await js.createOrUpdateKeyValue(cfg: KeyValueConfig(bucket: "settings", history: 5))
let watcher = try await kv.watchAll()
let revision = try await kv.put("theme", "dark")
try await kv.update("theme", Data("light".utf8), revision: revision)
try await kv.create("language", Data("swift".utf8))
if let theme = try await kv.get("theme") {
    print("theme = \(String(decoding: theme.value, as: UTF8.self)) (revision \(theme.revision))")
}
print("keys: \(try await kv.keys())")
var updates = 0
for try await entry in watcher {
    if let entry {
        print(
            "watch: \(entry.operation) \(entry.key) = \(String(decoding: entry.value, as: UTF8.self))"
        )
        updates += 1
    }
    if updates == 3 {
        break
    }
}
await watcher.stop()

print("\n### Object store")
let files = try await js.createOrUpdateObjectStore(cfg: ObjectStoreConfig(bucket: "files"))
let info = try await files.put(name: "readme.txt", data: Data(repeating: 65, count: 300_000))
print("stored \(info.name): \(info.size) bytes in \(info.chunks) chunks, \(info.digest ?? "")")
let content = try await files.getData("readme.txt")
print("read back \(content?.count ?? 0) bytes")
print("objects: \(try await files.list().map { $0.name })")

print("\n### Cleanup")
try await js.deleteStream(name: "ORDERS")
try await js.deleteKeyValue(bucket: "settings")
try await js.deleteObjectStore(bucket: "files")
try await nats.close()
print("bye")
