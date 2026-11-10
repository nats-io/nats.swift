# Object Store

Store files and other large objects, split into chunks and verified with SHA-256.

## Overview

An object store bucket is created with an ``ObjectStoreConfig``. Objects are stored in chunks,
described by ``ObjectMeta`` and verified with a SHA-256 digest when they are read.

```swift
let files = try await js.createObjectStore(cfg: ObjectStoreConfig(bucket: "files"))

let info = try await files.put(name: "notes.txt", string: "remember the milk")
try await files.putFile(URL(fileURLWithPath: "/path/to/report.pdf"))

let notes = try await files.getString("notes.txt")
try await files.getFile("report.pdf", to: URL(fileURLWithPath: "/tmp/report.pdf"))
```

### Streaming large objects

The generic `put(_:data:)` accepts any `AsyncSequence` of `Data`, and
``ObjectStore/get(_:showDeleted:)`` returns an ``ObjectResult`` delivering the content chunk by
chunk:

```swift
if let video = try await files.get("video.mp4") {
    for try await chunk in video {
        print("received \(chunk.count) bytes")
    }
}
```

### Managing objects

```swift
let objects = try await files.list()
try await files.updateMeta("notes.txt", meta: ObjectMeta(name: "todo.txt"))
try await files.addLink("latest", to: info)
try await files.delete("todo.txt")
try await files.seal()
```

Watch objects being added, changed and deleted with ``ObjectStore/watch(options:)``.
