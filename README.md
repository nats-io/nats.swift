<p align="center">
  <img src="./Resources/Logo@256.png">
</p>

<p align="center">
    A <a href="https://www.swift.org">Swift</a> client for the <a href="https://nats.io">NATS messaging system</a>.
</p>

**:warning: WARNING: THIS REPOSITORY IS UNDER REWORK AND SHOULD NOT BE USED AT THE MOMENT :warning: **

Tested with Swift 5.4 on [![macos](https://github.com/aus-der-Technik/swifty-nats/actions/workflows/macos.yml/badge.svg?branch=main)](https://github.com/aus-der-Technik/swifty-nats/actions/workflows/macos.yml) and [![Linux](https://github.com/aus-der-Technik/swifty-nats/actions/workflows/linux.yml/badge.svg?branch=main)](https://github.com/aus-der-Technik/swifty-nats/actions/workflows/linux.yml)

Swift Version Compatibility: [![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Faus-der-Technik%2Fswifty-nats%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/aus-der-Technik/swifty-nats)

Platform Compatibility: [![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Faus-der-Technik%2Fswifty-nats%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/aus-der-Technik/swifty-nats)

## Support
Join the [#swift](https://natsio.slack.com/archives/C02D41BU0PQ) channel on nats.io Slack.
We'll do our best to help quickly. You can also just drop by and say hello. We're looking forward to developing the community.

## Installation via Swift Package Manager
### In Package.swift
Add this packages as a dependency in your projects `Package.swift` file and add the Name to your target like shown in this example:

```swift
// swift-tools-version:5.4

import PackageDescription

let package = Package(
    name: "YourApp",
    products: [
        .executable(name: "YourApp", targets: ["YourApp"]),
    ],
    dependencies: [
        .package(name: "NatsSwift", url: "https://github.com/aus-der-technik/NatsSwift.git", from: "2.2.0")
    ],
    targets: [
        .target(
            name: "YourApp",
            dependencies: ["NatsSwift"]
        ),
    ]
)

```
### In an .xcodeproj
Open the project inspector in XCode and select your project. It is importent to select the **project** and not a target!
Klick on the third tab `Package Dependencies` and add the git url `https://github.com/aus-der-technik/NatsSwift.git` by selecting the litte `+`-sign at the end of the package list.


## Basic Usage
```swift

import NatsSwift

// register a new client
let client = NatsClient("http://nats.server:4222")

// listen to an event
client.on(.connect) { _ in
    print("Client connected")
}

// try to connect to the server
try? client.connect()

// subscribe to a channel with a inline message handler.
client.subscribe("foo.bar") { message in
    print("payload: \(message.payload)")
    print("size: \(message.byteCount)")
    print("reply subject: \(message.replySubject.subject)")
}

// publish an event onto the message strem into a subject
client.publish("this event happened", to: "foo.bar")

```


### Setting the loglevel
The default loglevel is `.error`. You can reset it to see more verbose messages. Possible
Values are `.debug`, `.info`, `.error` or `.critical`

```swift
let client = NatsClient("http://nats.server:4222")
client.config.loglevel = .info
```

### Reconnection is up to you
Reconnection is not part of this package, because if a server diconnects your application have to be sure that
subscribtions are made up again correctly.

With NatsSwift this is a very easy step:

```swift

// register a new client
let client = NatsClient(url)

// listen to the .disconnected event to try a reconnect
client.on(.disconnected) { [self] _ in
    sleep(5);
    try? client.reconnect()
    doSubscribe()
}

// subscribe to the channels
doSubscribe()

// private function to subscribe to channels
private func doSubscribe(){
    client.subscribe("foo.bar") { message in
        print("payload: \(message.payload)")
    }
}
```

### List of events
The public class `NatsEvent` contains all events you can subscribt to.

| event        | description                                                            |
| ------------ | ---------------------------------------------------------------------- |
| connected    | The client is conected to the server.                                  |
| disconnected | The client disconnects and was connectd before.                        |
| response     | The client gets an response from the server (internal).                |
| error        | The server sends an error that can't be handled.                       |
| dropped      | The clients droped a message. Mostly because of queue length to short. |
| reconnecting | The client reconencts to the server, (Because of a called reconnect()).|
| informed     | The server sends his information data successfully to the client.      |


### Information about the connected server

Since 2.0.2 it is possible to get the informations from the conencted server

```swift
let client = NatsClient("http://nats.server:4222")
print("\(client.serverInformation.serverName) has Version: \(client.serverInformation.version))");
```


## Contribution
Contribution is always welcome. Just send me a pull request.

# Changelog

## 2.2.0
- The client configuration is now publicly available
- The handling of the connection status has been rewritten
- Rewrite of the connection setup
- The connection termination was rewritten
- All classes have been cleaned up (refactoring)
- A new license was added (BSD-0)
- The reconnection code was removed
- Subscription queue is an optional property of NatsSubject

## 2.1.1
- rewrite the ChannelHandler: remove a bug that could lead into dropped messages.

## 2.1.0
- uses NIO2
- works with Vapor, now!

## 2.0.3
- supports iOS anf tvOS

## 2.0.2
- Get information from the connected Server (Version, name, id, ...)

## 2.0.1
- Test with GitHub Actions
- Update Dockerfile to build and test on Swift 5.4 (Linux)
- Test with a real NATS-Server on macos
- Cleanup unused definitions
- Update Informations, do project care

## 2.0.0
- Tested with NATS 2.3.4
- Introduced logging
- Updated depricated functions

# Roadmap
See: Contribution ;)
- Propper function description is needed

# Attribution
This library is based on excellent work in https://github.com/aus-der-Technik/NatsSwift
