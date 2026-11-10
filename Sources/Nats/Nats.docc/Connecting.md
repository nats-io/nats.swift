# Connecting

Configure servers, reconnects, authentication and TLS, and observe the connection.

## Overview

### Servers and reconnects

``NatsClientOptions`` configures the servers and how the client reconnects. When the connection is
lost the client reconnects automatically and restores its subscriptions.

```swift
let client = NatsClientOptions()
    .urls([
        URL(string: "nats://server-1:4222")!,
        URL(string: "nats://server-2:4222")!,
    ])
    .maxReconnects(10)
    .reconnectWait(2)
    .pingInterval(60)
    .retryOnfailedConnect()
    .build()

try await client.connect()
```

Use `ws://` or `wss://` URLs to connect over WebSocket.

### Authentication

```swift
// username and password
NatsClientOptions().usernameAndPassword("user", "secret")

// token
NatsClientOptions().token("s3cr3t")

// NKEY seed, from a string or a file
NatsClientOptions().nkeyFile(URL(fileURLWithPath: "/path/to/user.nk"))

// credentials (JWT) file
NatsClientOptions().credentialsFile(URL(fileURLWithPath: "/path/to/user.creds"))
```

### TLS

```swift
let client = NatsClientOptions()
    .url(URL(string: "tls://localhost:4222")!)
    .requireTls()
    .rootCertificates(URL(fileURLWithPath: "/path/to/ca.pem"))
    .clientCertificate(
        URL(fileURLWithPath: "/path/to/client-cert.pem"),
        URL(fileURLWithPath: "/path/to/client-key.pem"))
    .build()
```

Use ``NatsClientOptions/withTlsFirst()`` for servers expecting the TLS handshake first.

### Events

Register handlers for ``NatsEvent``s, such as reconnects or lame duck mode:

```swift
let listener = client.on([.connected, .disconnected, .lameDuckMode]) { event in
    print("event: \(event.kind())")
}
client.off(listener)
```

### App lifecycle

Suspend the connection when an app moves to the background, and resume it when it becomes active:

```swift
try await client.suspend()
try await client.resume()
```

## See Also

- ``NatsClientOptions``
- ``NatsEvent``
- ``NatsError/ConnectError``
