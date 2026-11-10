# ``JetStreamObjectStore``

An object store built on JetStream, for files and other large blobs.

## Overview

The Object Store splits objects into chunks, verifies them with a SHA-256 digest, and describes
them with metadata. Buckets are backed by JetStream streams named `OBJ_<bucket>` and are
compatible with other NATS clients.

```swift
import JetStream
import JetStreamObjectStore

let js = JetStreamContext(client: client)
let files = try await js.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))

try await files.putFile(URL(fileURLWithPath: "/path/to/report.pdf"))
let data = try await files.getData("report.pdf")
```

## Topics

### Essentials

- <doc:ObjectStoreGuide>
- ``ObjectStore``
- ``ObjectStoreConfig``

### Objects

- ``ObjectMeta``
- ``ObjectMetaOptions``
- ``ObjectLink``
- ``ObjectInfo``
- ``ObjectResult``

### Watching

- ``ObjectWatcher``
- ``ObjectWatchOptions``

### Status and Errors

- ``ObjectStoreStatus``
- ``JetStream/JetStreamError/ObjectStoreError``
