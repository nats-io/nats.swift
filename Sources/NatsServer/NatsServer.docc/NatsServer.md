# ``NatsServer``

Run `nats-server` processes in integration tests.

## Overview

``NatsServer`` starts a `nats-server` process with a configuration file, waits until it accepts
connections and stops it again. The `nats-server` binary must be available on the `PATH`.

```swift
import NatsServer
import XCTest

final class MyTests: XCTestCase {
    var server = NatsServer()

    override func tearDown() {
        server.stop()
    }

    func testPublish() async throws {
        server.start(cfg: Bundle.module.url(forResource: "jetstream", withExtension: "conf")!.path)
        let client = NatsClientOptions().url(URL(string: server.clientURL)!).build()
        try await client.connect()
    }
}
```

## Topics

### Server

- ``NatsServer/NatsServer``
