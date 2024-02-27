import Foundation
import NatsSwift

print("\n### Setup NATS Connection")

let nats = ClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .logLevel(.error)
    .build()

do {
    print("connecting...")
    try await nats.connect()
} catch {
    print("Error: \(error)")
    exit(1)
}

print("\n### Publish / Subscribe")
do {
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
        }
        
        print("message loop done...")
    }

    print("publishing data...")
    for i in 1...3 {
        if let data = "data\(i)".data(using: .utf8) {
            try await nats.publish(data, subject: "foo.\(i)")
        }
    }
    
    print("signal done...")
    try await nats.publish(Data(), subject: "foo.done")
    
    try await loop.value
    
    print("done")
    
} catch {
    print("Error: \(error)")
    exit(1)
}

print("\n### Publish / Subscribe with Headers")
do {
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
            try await nats.publish(data, subject: "foo.\(i)", headers: headers)
        }
    }
    
    print("signal done...")
    try await nats.publish(Data(), subject: "foo.done")
    
    try await loop.value
    
    print("done")
    
} catch {
    print("Error: \(error)")
    exit(1)
}

print("bye")
