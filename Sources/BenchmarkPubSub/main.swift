import NatsSwift
import Foundation

let nats = Client(url: URL(string: "nats://localhost:4222")!)
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
let numMsgs = 1_000_000
let sub = try await nats.subscribe(to: "foo")
try await withThrowingTaskGroup(of: Void.self) { group in
    // Adding a task for subscription
    group.addTask {
        var diffFound = false
        for i in 0..<numMsgs {
            let msg = await sub.next() // Assuming 'sub.next()' is an async function
            if i % 1000 == 0 {
                print("received \(i) msgs: \(String(data: msg!.payload!, encoding: .utf8))")
            }
        }
    }

    // Adding a task for publishing
    group.addTask {
        for i in 0..<numMsgs {
            try nats.publish("\(i)".data(using: .utf8)!, subject: "foo") // Assuming 'nats.publish()' is an async throwing function
            if i%1000 == 0 {
                print("published \(i) msgs")
            }
        }
    }

    // Wait for all tasks in the group to complete
    try await group.waitForAll()
}

try! await nats.flush()
let elapsed = DispatchTime.now().uptimeNanoseconds - now.uptimeNanoseconds
let msgsPerSec: Double = Double(numMsgs)/(Double(elapsed)/1_000_000_000)
print("Elapsed: \(elapsed / 1000000)ms")
print("\(msgsPerSec) msgs/s")
