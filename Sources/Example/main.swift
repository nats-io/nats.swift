import Foundation
import NatsSwift

print("\n### Setup NATS Connection")

print("connecting with defaults...")
let nats1 = NatsConnection()
try await nats1.connect()

print("connecting with options...")
let options = NatsOptions(
    urls: [URL(string: "nats://127.0.0.1:4222")!],
    loggerLevel: .error)
let nats = NatsConnection(options)

nats.on(.connected) { event in
    print("event: connected")
}

print("connecting...")
try await nats.connect()

print("\n### Publish / Subscribe")

print("subscribing...")
let sub = try await nats.subscribe(to: "foo.>")

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
            for (name, value) in headers {
                print("  \(name): \(value)")
            }
        }
    }
    
    print("message loop done...")
}

print("publishing data...")
for i in 1...3 {
    var headers = HeaderMap()
    headers["X-Type"] = "data"
    headers["X-Index"] = "index-\(i)"
    
    if let data = "data\(i)".data(using: .utf8) {
        try await nats.publish(data, to: "foo.\(i)", headers: headers)
    }
}

print("signalling done...")
try await nats.publish(Data(), to: "foo.done")

try await loop.value

print("closing...")
try await nats.close()

print("bye")
