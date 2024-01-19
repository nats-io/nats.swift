import NatsSwift
import Foundation

let nats = ClientOptions().url(URL(string: "nats://localhost:4222")!).build()
print("Connecting...")
try! await nats.connect()
print("Connected!")

let data = "foo".data(using: .utf8)!
// Warmup
print("Starting benchmark...")
print("Waiting for first message...")
var now = DispatchTime.now()
let numMsgs = 1_000_000
let sub = try await nats.subscribe(to: "foo").makeAsyncIterator()
var hm = HeaderMap()
hm.append(try! HeaderName("foo"), HeaderValue("bar"))
hm.append(try! HeaderName("foo"), HeaderValue("baz"))
for i in 1...numMsgs {
    let msg = await sub.next()
    if i == 0 {
        print("Received first message! Starting the timer")
        now = DispatchTime.now()
    }
    guard let payload = msg?.payload else {
        print("empty payload!")
        continue
    }
    if String(data: payload, encoding: .utf8) != "\(i)" {
        print("invalid payload; expected: \(i); got: \(String(data: payload, encoding: .utf8))")
    }
    guard let headers = msg?.headers else {
        print("empty headers!")
        continue
    }
    if i%1000 == 0 {
        print("received \(i) msgs")
    }
}

let elapsed = DispatchTime.now().uptimeNanoseconds - now.uptimeNanoseconds
let msgsPerSec: Double = Double(numMsgs)/(Double(elapsed)/1_000_000_000)
print("Elapsed: \(elapsed / 1000000)ms")
print("\(msgsPerSec) msgs/s")
