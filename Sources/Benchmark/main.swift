import NatsSwift
import Foundation

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
    try! nats.publish(data, subject: "foo")
}
print("Starting benchmark...")
let now = DispatchTime.now()
let numMsgs = 10_000_000
for _ in 0..<numMsgs {
    try! nats.publish(data, subject: "foo")
}
try! await nats.flush()
let elapsed = DispatchTime.now().uptimeNanoseconds - now.uptimeNanoseconds
let msgsPerSec: Double = Double(numMsgs)/(Double(elapsed)/1_000_000_000)
print("Elapsed: \(elapsed / 1000000)ms")
print("\(msgsPerSec) msgs/s")
