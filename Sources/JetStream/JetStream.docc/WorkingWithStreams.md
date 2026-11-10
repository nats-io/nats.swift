# Streams

Create streams which store the messages published on their subjects.

## Overview

A stream stores the messages published on its subjects, within limits of age, size and count, and
according to a retention policy: ``RetentionPolicy/limits`` keeps messages until a limit is
reached, ``RetentionPolicy/interest`` until all consumers acknowledged them and
``RetentionPolicy/workqueue`` until any consumer acknowledged them.

```swift
let stream = try await js.createStream(
    cfg: StreamConfig(
        name: "ORDERS",
        subjects: ["orders.>"],
        maxAge: NanoTimeInterval(7 * 24 * 3600),
        storage: .file,
        replicas: 3))

// update, or create if it does not exist
_ = try await js.createOrUpdateStream(
    cfg: StreamConfig(name: "ORDERS", subjects: ["orders.>"], maxMsgsPerSubject: 100))
```

### Inspecting streams

```swift
let info = try await stream.info()
print("\(info.state.messages) messages, \(info.state.bytes) bytes")

// message count of each subject
let perSubject = try await stream.info(subjectFilter: "orders.>")

for try await info in await js.streams() {
    print(info.config.name)
}
let name = try await js.streamNameBySubject("orders.created")
```

### Reading and removing messages

```swift
let first = try await stream.getMessage(sequence: 1)
let latest = try await stream.getMessage(lastForSubject: "orders.created")
try await stream.deleteMessage(sequence: 1)
_ = try await stream.purge(subject: "orders.cancelled")
_ = try await stream.purge(keep: 10)

try await js.deleteStream(name: "ORDERS")
```

With ``StreamConfig/allowDirect`` set, ``Stream/getMessageDirect(lastForSubject:)`` reads messages
from any replica.

### Mirrors and sources

A stream can mirror another stream, or aggregate messages of several streams, including streams of
other JetStream domains:

```swift
let mirror = try await js.createStream(
    cfg: StreamConfig(name: "ORDERS_BACKUP", mirror: StreamSource(name: "ORDERS")))

let remote = try await js.createStream(
    cfg: StreamConfig(name: "HUB_ORDERS", mirror: StreamSource(name: "ORDERS", domain: "hub")))
```
