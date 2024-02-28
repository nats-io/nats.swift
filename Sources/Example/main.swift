import Foundation
import NatsSwift

print("\n### Setup NATS Connection")

let nats = ClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()

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
            if let headerValue = headers.get(try! HeaderName("X-Example")) {
                print("  header: X-Example: \(headerValue.description)")
            }
        }
    }
    
    print("message loop done...")
}

print("publishing data...")
for i in 1...3 {
    var headers = HeaderMap()
    headers.append(try! HeaderName("X-Example"), HeaderValue("example value"))
    
    if let data = "data\(i)".data(using: .utf8) {
        try nats.publish(data, subject: "foo.\(i)", headers: headers)
    }
}

print("signalling done...")
try nats.publish(Data(), subject: "foo.done")

try await loop.value

print("closing...")
try await nats.close()

print("bye")
