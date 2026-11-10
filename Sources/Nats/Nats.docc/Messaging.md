# Messaging

Publish messages, subscribe to subjects, use queue groups and make requests.

## Overview

### Subjects

Subjects are dot-separated names such as `orders.eu.created`. Subscriptions may use wildcards:
`*` matches a single token (`orders.*.created`) and `>` matches one or more tokens (`orders.>`).

### Publishing

```swift
try await client.publish(Data("order created".utf8), subject: "orders.created")

var headers = NatsHeaderMap()
headers.append(try NatsHeaderName("X-Order-Id"), NatsHeaderValue("1234"))
try await client.publish(Data("order created".utf8), subject: "orders.created", headers: headers)

// wait until the server processed everything sent so far
try await client.flush()
```

### Subscribing

A ``NatsSubscription`` is an `AsyncSequence` of ``NatsMessage``s:

```swift
let subscription = try await client.subscribe(subject: "orders.>")
for try await message in subscription {
    print(message.subject, message.headers?[try NatsHeaderName("X-Order-Id")] ?? "")
}
```

Subscribers sharing a **queue group** split the messages of a subject between them:

```swift
let worker = try await client.subscribe(subject: "tasks", queue: "workers")
```

Stop a subscription with ``NatsSubscription/unsubscribe(after:)``, immediately or after a number
of messages.

### Request–reply

```swift
// responder
let service = try await client.subscribe(subject: "time")
Task {
    for try await request in service {
        if let reply = request.replySubject {
            try await client.publish(Data(Date().description.utf8), subject: reply)
        }
    }
}

// requester
let response = try await client.request(Data(), subject: "time", timeout: 2)
```

A request throws ``NatsError/RequestError/noResponders`` when nobody listens on the subject, and
``NatsError/RequestError/timeout`` when no reply arrives in time.

## See Also

- ``NatsMessage``
- ``NatsHeaderMap``
- ``NatsError/SubscriptionError``
