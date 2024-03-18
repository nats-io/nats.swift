![NATS Swift Client](./Resources/Logo@256.png)

[![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Faus-der-Technik%2Fswifty-nats%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/aus-der-Technik/swifty-nats) [![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Faus-der-Technik%2Fswifty-nats%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/aus-der-Technik/swifty-nats)

# NATS Swift Client

Welcome to the [Swift](https://www.swift.org) Client for [NATS](https://nats.io),
your gateway to asynchronous messaging in Swift applications. This client library
is designed to provide Swift developers with a seamless interface to NATS
messaging, enabling swift and efficient communication across distributed systems.

## Support

Join the [#swift](https://natsio.slack.com/channels/swift) channel on nats.io Slack.
We'll do our best to help quickly. You can also just drop by and say hello. We're looking forward to developing the community.

## Installation via Swift Package Manager

Include this package as a dependency in your project's `Package.swift` file and add the package name to your target as shown in the following example:

```swift
// swift-tools-version:5.7

import PackageDescription

let package = Package(
    name: "YourApp",
    products: [
        .executable(name: "YourApp", targets: ["YourApp"]),
    ],
    dependencies: [
        .package(name: "Nats", url: "https://github.com/nats-io/nats.swift.git", from: "0.1")
    ],
    targets: [
        .target(name: "YourApp", dependencies: ["Nats"]),
    ]
)

```

### Xcode Package Dependencies

Open the project inspector in Xcode and select your project. It is important to select the **project** and not a target!
Click on the third tab `Package Dependencies` and add the git url `https://github.com/nats-io/nats.swift.git` by selecting the little `+`-sign at the end of the package list.

## Basic Usage

Here is a quick start example to see everything at a glance:

```swift
import Nats

// create the client
let nats = NatsClientOptions().url(URL(string: "nats://localhost:4222")!).build()

// connect to the server
try await nats.connect()

// subscribe to a subject
let subscription = try await nats.subscribe(to: "events.>")

// publish a message
try nats.publish("my event".data(using: .utf8)!, subject: "events.example")

// receive published messages
for await msg in subscriptions {
    print( "Received: \(String(data:msg.payload!, encoding: .utf8)!)")
}
 ```

### Connecting to a NATS Server

The first step is establishing a connection to a NATS server.
This example demonstrates how to connect to a NATS server using the default settings, which assume the server is
running locally on the default port (4222). You can also customize your connection by specifying additional options:

```swift
let nats = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()

try await nats.connect()
```

### Publishing Messages

Once you've established a connection to a NATS server, the next step is to publish messages.
Publishing messages to a subject allows any subscribed clients to receive these messages
asynchronously. This example shows how to publish a simple text message to a specific subject.

```swift
let data = "message text".data(using: .utf8)!
try nats.publish(data, subject: "foo.msg")
```

In more complex scenarios, you might want to include additional metadata with your messages in
the form of headers. Headers allow you to pass key-value pairs along with your message, providing
extra context or instructions for the subscriber. This example shows how to publish a
message with headers:

```swift
let data = "message text".data(using: .utf8)!

var headers = NatsHeaderMap()
headers.append(try! NatsHeaderName("X-Example"), NatsHeaderValue("example value"))

try nats.publish(data, subject: "foo.msg.1", headers: headers)
```

### Subscribing to Subjects

After establishing a connection and publishing messages to a NATS server, the next crucial step is
subscribing to subjects. Subscriptions enable your client to listen for messages published to
specific subjects, facilitating asynchronous communication patterns. This example
will guide you through creating a subscription to a subject, allowing your application to process
incoming messages as they are received.


```swift
let subscription = try await nats.subscribe(to: "foo.>")

for try await msg in subscription {

    if msg.subject == "foo.done" {
        break
    }

    if let payload = msg.payload {
        print("received \(msg.subject): \(String(data: payload, encoding: .utf8) ?? "")")
    }

    if let headers = msg.headers {
        if let headerValue = headers.get(try! NatsHeaderName("X-Example")) {
            print("  header: X-Example: \(headerValue.description)")
        }
    }
}
```

Notice that the subject `foo.>` uses a special wildcard syntax, allowing for subscription
to a hierarchy of subjects. For more detailed information, please refer to the [NATS documentation
on _Subject-Based Messaging_](https://docs.nats.io/nats-concepts/subjects).

### Setting Log Levels

The default log level is `.info`. You can set it to see more or less verbose messages. Possible values are `.debug`, `.info`, `.error` or `.critical`.

```swift
// TODO
```

### Events

 You can also monitor when your app connects, disconnects, or encounters an error using events:

```swift
let nats = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()

nats.on(.connected) { event in
    print("event: connected")
}
```

## Attribution

This library is based on excellent work in https://github.com/aus-der-Technik/SwiftyNats
