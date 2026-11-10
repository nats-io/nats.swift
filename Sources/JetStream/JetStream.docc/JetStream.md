# ``JetStream``

Persistent streams, consumers, a Key-Value Store and an Object Store for NATS.

## Overview

JetStream is the persistence layer of the NATS server. It stores messages in **streams** and
replays them to **consumers** with at-least-once delivery.

The **Key-Value Store** and the **Object Store**, built on top of streams, are provided by the
`JetStreamKeyValue` and `JetStreamObjectStore` modules.

All operations start from a ``JetStreamContext`` created from a connected `NatsClient`:

```swift
import JetStream
import Nats

let js = JetStreamContext(client: client)

let stream = try await js.createOrUpdateStream(
    cfg: StreamConfig(name: "ORDERS", subjects: ["orders.>"]))
try await js.publish("orders.created", message: Data("order 1".utf8)).wait()

let consumer = try await stream.createOrUpdateConsumer(cfg: ConsumerConfig(durable: "billing"))
for try await message in try await consumer.consume() {
    try await message.ack()
}
```

JetStream requires a server with JetStream enabled, e.g. `nats-server -js`.

## Topics

### Essentials

- ``JetStreamContext``
- <doc:Lifecycles>
- ``AccountInfo``
- ``AccountTier``
- ``AccountLimits``
- ``APIStats``

### Streams

- <doc:WorkingWithStreams>
- ``Stream``
- ``StreamConfig``
- ``StreamInfo``
- ``StreamState``
- ``StreamMessage``
- ``Streams``
- ``StreamNames``
- ``RetentionPolicy``
- ``DiscardPolicy``
- ``StorageType``
- ``StoreCompression``
- ``PersistMode``
- ``Placement``
- ``StreamSource``
- ``StreamSourceInfo``
- ``StreamConsumerSource``
- ``ExternalStream``
- ``SubjectTransformConfig``
- ``RePublish``
- ``StreamConsumerLimits``
- ``StreamAlternate``
- ``ClusterInfo``
- ``PeerInfo``
- ``LostStreamData``

### Publishing

- <doc:Publishing>
- ``PublishOptions``
- ``MessageTTL``
- ``MessageSchedule``
- ``AckFuture``
- ``Ack``

### Consumers

- <doc:UsingConsumers>
- ``Consumer``
- ``ConsumerConfig``
- ``ConsumerInfo``
- ``SequenceInfo``
- ``DeliverPolicy``
- ``AckPolicy``
- ``ReplayPolicy``
- ``FetchResult``
- ``ConsumeOptions``
- ``ConsumeResult``
- ``OrderedConsumer``
- ``OrderedConsumerConfig``
- ``OrderedConsumeResult``
- ``OrderedFetchResult``
- ``PushConsumer``
- ``PushConsumeResult``
- ``PriorityPolicy``
- ``PriorityGroupOptions``
- ``PriorityGroupState``
- ``ConsumerPauseResponse``
- ``ConsumerResetResponse``
- ``Consumers``
- ``ConsumerNames``

### Messages

- ``JetStreamMessage``
- ``MessageMetadata``
- ``AckKind``

### Errors

- ``JetStreamError``
- ``JetStreamErrorProtocol``
- ``ErrorCode``

### Utilities

- ``NanoTimeInterval``
- ``Response``
- ``JetStreamAPIResponse``
