# ``JetStreamKeyValue``

A key-value store built on JetStream, with history, compare-and-set and watchers.

## Overview

A key-value bucket stores values under keys, keeps a configurable history of each key, and lets
you watch keys for changes. Buckets are backed by JetStream streams named `KV_<bucket>` and are
compatible with other NATS clients.

```swift
import JetStream
import JetStreamKeyValue

let js = JetStreamContext(client: client)
let kv = try await js.createKeyValue(cfg: KeyValueConfig(bucket: "config", history: 5))

try await kv.put("app.theme", "dark")
let entry = try await kv.get("app.theme")

for try await entry in try await kv.watch("app.*") {
    guard let entry else { continue }
    print(entry.key, entry.operation)
}
```

## Topics

### Essentials

- <doc:KeyValueStore>
- ``KeyValue``
- ``KeyValueConfig``

### Entries

- ``KeyValueEntry``
- ``KeyValueOperation``

### Watching

- ``KeyValueWatcher``
- ``KeyValueWatchOptions``

### Status and Errors

- ``KeyValueStatus``
- ``JetStream/JetStreamError/KeyValueError``
