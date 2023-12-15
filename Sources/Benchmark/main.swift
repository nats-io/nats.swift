import NatsSwift
import Foundation

let nats = Client(url: URL(string: "nats://localhost:4222")!)
print("Connecting...")
try! await nats.connect()
print("Connected!")

let data = "foo"
// Warmup
print("Warming up...")
for _ in 0..<10_000 {
    try!  nats.publish(data, subject: "foo")
}
print("Starting benchmark...")
let now = DispatchTime.now()
for _ in 0..<10_000_000 {
    try!  nats.publish(data, subject: "foo")
}
try! await nats.flush()
let elapsed = DispatchTime.now().uptimeNanoseconds - now.uptimeNanoseconds
print("Elapsed: \(elapsed / 1000000)ms")
