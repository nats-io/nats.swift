# ``Nats``

A modern, `async`/`await` client for the NATS messaging system.

## Overview

[NATS](https://nats.io) connects applications, services and devices through **subjects**:
clients publish messages to subjects such as `orders.created`, and subscribe to subjects,
optionally with wildcards, without needing to know about each other.

The `Nats` module provides Core NATS: connections with automatic reconnects, authentication, TLS,
publish/subscribe, queue groups, request–reply, headers and connection events. Subscriptions are
`AsyncSequence`s you iterate with `for try await`.

```swift
import Nats

let client = NatsClientOptions().url(URL(string: "nats://localhost:4222")!).build()
try await client.connect()

let subscription = try await client.subscribe(subject: "greetings.>")
try await client.publish(Data("hello".utf8), subject: "greetings.world")

for try await message in subscription {
    print(String(decoding: message.payload ?? Data(), as: UTF8.self))
}
```

For persistence, streams, the Key-Value Store and the Object Store, see the `JetStream` module.

## Topics

### Essentials

- <doc:GettingStarted>
- ``NatsClient``
- ``NatsClientOptions``

### Connections

- <doc:Connecting>
- ``NatsState``
- ``NatsEvent``
- ``NatsEventKind``
- ``Auth``

### Messaging

- <doc:Messaging>
- ``NatsMessage``
- ``NatsSubscription``
- ``NatsHeaderMap``
- ``NatsHeaderName``
- ``NatsHeaderValue``
- ``StatusCode``

### Errors

- ``NatsError``
- ``NatsErrorProtocol``

### Logging

- ``logger``
