import Foundation
import NatsSwift

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
    try! await nats.publish(data, subject: "nil")
}
print("Starting benchmark...")
let now = DispatchTime.now()
let numMsgs = 1_000_000
for _ in 0..<numMsgs {
    try! await nats.publish(data, subject: "foo")
}
_ = try! await nats.rtt()
let elapsed = DispatchTime.now().uptimeNanoseconds - now.uptimeNanoseconds
let msgsPerSec: Double = Double(numMsgs) / (Double(elapsed) / 1_000_000_000)
print("Elapsed: \(elapsed / 1_000_000)ms")
print("\(msgsPerSec) msgs/s")
