# Key-Value Store

Store values under keys, keep their history and watch them for changes.

## Overview

A key-value bucket is created with a ``KeyValueConfig``. Every change of a key gets a revision
number, and the bucket keeps a configurable history of each key.

```swift
let kv = try await js.createKeyValue(cfg: KeyValueConfig(bucket: "config", history: 5))

let revision = try await kv.put("app.theme", "dark")
let entry = try await kv.get("app.theme")

try await kv.create("app.language", Data("swift".utf8))          // only if missing
try await kv.update("app.theme", Data("light".utf8), revision: revision)  // compare-and-set
try await kv.delete("app.theme")                                   // keeps history
try await kv.purge("app.language")                                 // removes history
```

### Watching keys

A ``KeyValueWatcher`` delivers the current values, then `nil`, then every change:

```swift
for try await entry in try await kv.watch("app.*") {
    guard let entry else { continue }  // all current values were delivered
    print(entry.key, entry.operation)
}
```

### Keys and history

```swift
let keys = try await kv.keys(filters: ["app.>"])
let history = try await kv.history("app.theme")
try await kv.purgeDeletes()
```

### Per-key TTLs

With ``KeyValueConfig/limitMarkerTTL`` set, keys can expire individually:

```swift
let sessions = try await js.createKeyValue(
    cfg: KeyValueConfig(bucket: "sessions", limitMarkerTTL: 60))
try await sessions.create("user-1", Data("token".utf8), ttl: 1800)
```

### Mirrors and sources

```swift
let replica = try await js.createKeyValue(
    cfg: KeyValueConfig(bucket: "config-replica", mirror: StreamSource(name: "config")))
```

Writes to a mirror are forwarded to the origin bucket, including buckets of other JetStream
domains.
