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
import Nats

// Benchmark: consume() vs fetch() throughput
//
// Runs fetch first to eliminate cold-start bias, then alternates
// between consume and fetch across multiple passes.
//
// Usage:
//   swift run -c release BenchmarkConsume [numMessages]

let numMsgs =
    CommandLine.arguments.count > 1
    ? Int(CommandLine.arguments[1]) ?? 100_000
    : 100_000

let client = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()

print("Connecting...")
try await client.connect()
print("Connected!")

let ctx = JetStreamContext(client: client)

// Setup: create stream
do {
    _ = try await ctx.getStream(name: "BENCH")
    _ = try await ctx.deleteStream(name: "BENCH")
} catch {}
let stream = try await ctx.createStream(
    cfg: StreamConfig(name: "BENCH", subjects: ["bench.*"]))

let payload = String(repeating: "x", count: 128).data(using: .utf8)!

// Helper to publish N messages
func publishMessages(_ n: Int) async throws {
    for i in 0..<n {
        _ = try await ctx.publish("bench.data", message: payload)
        if (i + 1) % 50_000 == 0 {
            print("  published \(i + 1)...")
        }
    }
}

var consumerSeq = 0
func nextConsumerName() -> String {
    consumerSeq += 1
    return "bench_\(consumerSeq)"
}

// Benchmark helpers
func benchmarkConsume(ack: Bool) async throws -> Double {
    let name = nextConsumerName()
    let cfg =
        ack
        ? ConsumerConfig(name: name)
        : ConsumerConfig(name: name, ackPolicy: .none)
    let consumer = try await stream.createConsumer(cfg: cfg)
    try await publishMessages(numMsgs)

    let msgs = try await consumer.consume(
        config: ConsumeConfig(maxMessages: 500, expires: 30))
    var count = 0
    let start = DispatchTime.now()
    for try await msg in msgs {
        if ack { try await msg.ack() }
        count += 1
        if count >= numMsgs { msgs.stop() }
    }
    let elapsed =
        Double(DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000_000
    return Double(count) / elapsed
}

func benchmarkFetch(ack: Bool) async throws -> Double {
    let name = nextConsumerName()
    let cfg =
        ack
        ? ConsumerConfig(name: name)
        : ConsumerConfig(name: name, ackPolicy: .none)
    let consumer = try await stream.createConsumer(cfg: cfg)
    try await publishMessages(numMsgs)

    var count = 0
    let start = DispatchTime.now()
    while count < numMsgs {
        let batch = try await consumer.fetch(batch: 500, expires: 5)
        for try await msg in batch {
            if ack { try await msg.ack() }
            count += 1
        }
    }
    let elapsed =
        Double(DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000_000
    return Double(count) / elapsed
}

func fmt(_ rate: Double) -> String { String(format: "%7d", Int(rate)) }
func fmtRatio(_ a: Double, _ b: Double) -> String { String(format: "%.2f", a / b) }

// ═══════════════════════════════════════════════════════
//  Warmup
// ═══════════════════════════════════════════════════════
print("\n--- Warmup (1000 msgs, results discarded) ---")
let warmupCfg = ConsumerConfig(name: "warmup", ackPolicy: .none)
let warmupCons = try await stream.createConsumer(cfg: warmupCfg)
for _ in 0..<1000 {
    _ = try await ctx.publish("bench.data", message: payload)
}
let warmupMsgs = try await warmupCons.consume(
    config: ConsumeConfig(maxMessages: 500, expires: 5))
var warmupCount = 0
for try await _ in warmupMsgs {
    warmupCount += 1
    if warmupCount >= 1000 { warmupMsgs.stop() }
}
print("Warmup done (\(warmupCount) msgs)")

// ═══════════════════════════════════════════════════════
//  Pass 1: fetch first, then consume (with ack)
// ═══════════════════════════════════════════════════════
print("\n=== Pass 1: with ack (fetch first) ===")
print("  fetch...")
let fetchAck1 = try await benchmarkFetch(ack: true)
print("  fetch()+ack:    \(fmt(fetchAck1)) msgs/s")
print("  consume...")
let consumeAck1 = try await benchmarkConsume(ack: true)
print("  consume()+ack:  \(fmt(consumeAck1)) msgs/s")

// ═══════════════════════════════════════════════════════
//  Pass 2: consume first, then fetch (with ack)
// ═══════════════════════════════════════════════════════
print("\n=== Pass 2: with ack (consume first) ===")
print("  consume...")
let consumeAck2 = try await benchmarkConsume(ack: true)
print("  consume()+ack:  \(fmt(consumeAck2)) msgs/s")
print("  fetch...")
let fetchAck2 = try await benchmarkFetch(ack: true)
print("  fetch()+ack:    \(fmt(fetchAck2)) msgs/s")

// ═══════════════════════════════════════════════════════
//  Pass 3: no ack (fetch first)
// ═══════════════════════════════════════════════════════
print("\n=== Pass 3: no ack (fetch first) ===")
print("  fetch...")
let fetchNoAck1 = try await benchmarkFetch(ack: false)
print("  fetch() no ack:    \(fmt(fetchNoAck1)) msgs/s")
print("  consume...")
let consumeNoAck1 = try await benchmarkConsume(ack: false)
print("  consume() no ack:  \(fmt(consumeNoAck1)) msgs/s")

// ═══════════════════════════════════════════════════════
//  Pass 4: no ack (consume first)
// ═══════════════════════════════════════════════════════
print("\n=== Pass 4: no ack (consume first) ===")
print("  consume...")
let consumeNoAck2 = try await benchmarkConsume(ack: false)
print("  consume() no ack:  \(fmt(consumeNoAck2)) msgs/s")
print("  fetch...")
let fetchNoAck2 = try await benchmarkFetch(ack: false)
print("  fetch() no ack:    \(fmt(fetchNoAck2)) msgs/s")

// ═══════════════════════════════════════════════════════
//  Summary
// ═══════════════════════════════════════════════════════
let avgConsumeAck = (consumeAck1 + consumeAck2) / 2
let avgFetchAck = (fetchAck1 + fetchAck2) / 2
let avgConsumeNoAck = (consumeNoAck1 + consumeNoAck2) / 2
let avgFetchNoAck = (fetchNoAck1 + fetchNoAck2) / 2

print("\n══════════════════════════════════════════════")
print("  \(numMsgs) messages, 128-byte payload")
print("══════════════════════════════════════════════")
print("              With ack          No ack")
print("  consume: \(fmt(avgConsumeAck))  avg    \(fmt(avgConsumeNoAck))  avg")
print("  fetch:   \(fmt(avgFetchAck))  avg    \(fmt(avgFetchNoAck))  avg")
print(
    "  ratio:    \(fmtRatio(avgConsumeAck, avgFetchAck))x          \(fmtRatio(avgConsumeNoAck, avgFetchNoAck))x"
)
print("")
print("  Per-pass detail:")
print("    ack pass1 (fetch 1st): consume \(fmt(consumeAck1))  fetch \(fmt(fetchAck1))")
print("    ack pass2 (cons  1st): consume \(fmt(consumeAck2))  fetch \(fmt(fetchAck2))")
print("    noack p3  (fetch 1st): consume \(fmt(consumeNoAck1))  fetch \(fmt(fetchNoAck1))")
print("    noack p4  (cons  1st): consume \(fmt(consumeNoAck2))  fetch \(fmt(fetchNoAck2))")
print("══════════════════════════════════════════════")

// Cleanup
try await ctx.deleteStream(name: "BENCH")
try await client.close()
