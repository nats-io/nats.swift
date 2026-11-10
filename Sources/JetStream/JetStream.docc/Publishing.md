# Publishing

Publish messages to streams and handle their acknowledgements.

## Overview

``JetStreamContext/publish(_:message:headers:options:)`` sends a message and returns an
``AckFuture``. ``AckFuture/wait()`` returns the ``Ack`` once the server stored the message:

```swift
let ack = try await js.publish("orders.created", message: Data("order 1".utf8)).wait()
print("stored in \(ack.stream) at sequence \(ack.seq)")
```

### Deduplication and concurrency control

```swift
let ack = try await js.publish(
    "orders.created", message: Data("order 1".utf8),
    options: PublishOptions(
        messageId: "order-1",             // duplicates within the window are dropped
        expectedLastSubjectSequence: 41,  // reject if the subject changed since
        ttl: .after(3600))                // remove the message after an hour
).wait()
```

Rejected expectations throw ``JetStreamError/PublishError``.

### Asynchronous publishing

Acknowledgements of all publishes of a context are received on a single subscription, so many
messages can be published before waiting for their acknowledgements:

```swift
var futures: [AckFuture] = []
for i in 1...1000 {
    futures.append(try await js.publish("orders.created", message: Data("\(i)".utf8)))
}
await js.publishAsyncComplete()
for future in futures {
    _ = try await future.wait()
}
```

``JetStreamContext/setPublishAsyncMaxPending(_:stallWait:)`` limits the number of unacknowledged
publishes. When no stream responds, for example during a leader election, messages are republished
according to ``PublishOptions/retryAttempts``.

### Scheduled messages

On streams with ``StreamConfig/allowMsgSchedules``, a message can be delivered on a schedule:

```swift
_ = try await js.publish(
    "schedules.report", message: Data(),
    options: PublishOptions(schedule: .cron("0 0 9 * * *", timeZone: "Europe/Amsterdam"),
                            scheduleTarget: "reports.daily"))
```
