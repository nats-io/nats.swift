import NatsSwift
import Foundation

let nats = ClientOptions().urls([URL(string: "nats://localhost:4222")!]).build()
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
    group.addTask {
        var hm = HeaderMap()
        hm.append(try! HeaderName("foo"), HeaderValue("bar"))
        hm.append(try! HeaderName("foo"), HeaderValue("baz"))
        hm.insert(try! HeaderName("another"), HeaderValue("one"))
        var i = 0
        for await msg in sub {
            let payload = msg.payload!
            if String(data: payload, encoding: .utf8) != "\(i)" {
                print("invalid payload; expected: \(i); got: \(String(data: payload, encoding: .utf8))")
            }
            guard let headers = msg.headers else {
                print("empty headers!")
                continue
            }
            if headers != hm {
                print("invalid headers; expected: \(hm); got: \(headers)")
            }
            if i%1000 == 0 {
                print("received \(i) msgs")
            }
        }
    }

    group.addTask {
        var hm = HeaderMap()
        hm.append(try! HeaderName("foo"), HeaderValue("bar"))
        hm.append(try! HeaderName("foo"), HeaderValue("baz"))
        hm.insert(try! HeaderName("another"), HeaderValue("one"))
        for i in 0..<numMsgs {
            try nats.publish("\(i)".data(using: .utf8)!, subject: "foo", headers: hm)
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
