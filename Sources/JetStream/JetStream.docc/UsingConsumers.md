# Consumers

Receive messages from streams with pull, ordered and push consumers.

## Overview

A consumer tracks which messages of a stream were delivered and acknowledged. Durable consumers keep
their state on the server; consumers without a durable name are removed after a period of
inactivity.

```swift
let consumer = try await stream.createOrUpdateConsumer(
    cfg: ConsumerConfig(
        durable: "billing", ackPolicy: .explicit, ackWait: NanoTimeInterval(30),
        maxDeliver: 5, filterSubject: "orders.created"))
```

### Fetching messages

```swift
for try await message in try await consumer.fetch(batch: 10, expires: 5) {
    try await message.ack()
}
let single = try await consumer.next(expires: 5)
let available = try await consumer.fetchNoWait(batch: 10)
let sized = try await consumer.fetchBytes(maxBytes: 1_048_576)
```

### Continuous consumption

``Consumer/consume(options:errorHandler:)`` pipelines pull requests and recovers from missed
heartbeats, reconnects and leader elections:

```swift
let messages = try await consumer.consume(
    options: ConsumeOptions(maxMessages: 100),
    errorHandler: { error in print("recovered from: \(error)") })

for try await message in messages {
    try await message.ack()
}
```

``ConsumeResult/stop()`` ends consumption immediately and ``ConsumeResult/drain()`` after the
buffered messages.

### Acknowledging messages

```swift
try await message.ack()
try await message.ack(ackType: .nak(delay: 10))
try await message.ack(ackType: .inProgress)
try await message.ack(ackType: .term(reason: "invalid"))
try await message.doubleAck()
```

A message is acknowledged once; further acknowledgements throw
``JetStreamError/AckError/alreadyAcknowledged``.

### Ordered consumers

An ``OrderedConsumer`` delivers every message in order without acknowledgements and recreates
itself when a message is missed:

```swift
let ordered = stream.orderedConsumer()
for try await message in try await ordered.consume() {
    print(message.subject)
}
```

### Push consumers

A ``PushConsumer`` delivers messages to a subject, optionally load-balanced in a deliver group:

```swift
let push = try await stream.createPushConsumer(
    cfg: ConsumerConfig(
        durable: "mailer", deliverSubject: "deliver.mailer", deliverGroup: "mailers",
        flowControl: true, idleHeartbeat: NanoTimeInterval(5)))
for try await message in try await push.consume() {
    try await message.ack()
}
```

### Managing consumers

```swift
_ = try await stream.pauseConsumer(name: "billing", until: Date().addingTimeInterval(3600))
_ = try await stream.resumeConsumer(name: "billing")
_ = try await stream.resetConsumer(name: "billing", sequence: 100)
```

Consumers with a ``PriorityPolicy`` distribute messages between clients according to
``PriorityGroupOptions``.
