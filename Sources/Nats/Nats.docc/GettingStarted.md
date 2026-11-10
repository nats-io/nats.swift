# Getting Started

Add the package, connect to a server and exchange your first messages.

## Overview

### Install a server

Install `nats-server` and start it locally:

```bash
curl --fail https://binaries.nats.dev/nats-io/nats-server/v2@latest | PREFIX='/usr/local/bin' sh
nats-server
```

### Add the package

Add the package to your `Package.swift` and the `Nats` product to your target:

```swift
dependencies: [
    .package(url: "https://github.com/nats-io/nats.swift.git", from: "0.5.0")
],
targets: [
    .executableTarget(
        name: "YourApp",
        dependencies: [.product(name: "Nats", package: "nats.swift")]),
]
```

In Xcode, select your project, open **Package Dependencies**, click **+** and enter the repository
URL.

### Connect

Configure a client with ``NatsClientOptions`` and connect:

```swift
import Nats

let client = NatsClientOptions()
    .url(URL(string: "nats://localhost:4222")!)
    .build()

try await client.connect()
```

### Publish and subscribe

```swift
let subscription = try await client.subscribe(subject: "orders.>")

try await client.publish(Data("order 1".utf8), subject: "orders.created")

for try await message in subscription {
    print("\(message.subject): \(String(decoding: message.payload ?? Data(), as: UTF8.self))")
}
```

### Close the connection

```swift
try await client.close()
```

## See Also

- <doc:Connecting>
- <doc:Messaging>
