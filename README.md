<p align="center">
  <img src="./Resources/Logo@256.png">
</p>

<p align="center">
    A <a href="https://www.swift.org">Swift</a> client for the <a href="https://nats.io">NATS messaging system</a>.
</p>

**:warning: WARNING: THIS REPOSITORY IS UNDER REWORK AND SHOULD NOT BE USED AT THE MOMENT :warning: **

Swift Version Compatibility: [![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Faus-der-Technik%2Fswifty-nats%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/aus-der-Technik/swifty-nats)
Platform Compatibility: [![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Faus-der-Technik%2Fswifty-nats%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/aus-der-Technik/swifty-nats)

## Support
Join the [#swift](https://natsio.slack.com/archives/C02D41BU0PQ) channel on nats.io Slack.
We'll do our best to help quickly. You can also just drop by and say hello. We're looking forward to developing the community.

## Installation via Swift Package Manager
### In Package.swift
Add this packages as a dependency in your projects `Package.swift` file and add the Name to your target like shown in this example:

```swift
// swift-tools-version:5.6

import PackageDescription

let package = Package(
    name: "YourApp",
    products: [
        .executable(name: "YourApp", targets: ["YourApp"]),
    ],
    dependencies: [
        .package(name: "NatsSwift", url: "https://github.com/nats-io/NatsSwift.git", from: "0.1")
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
Klick on the third tab `Package Dependencies` and add the git url `https://github.com/nats-io/NatsSwift.git` by selecting the litte `+`-sign at the end of the package list.


## Basic Usage
```swift

import NatsSwift

 // Setup the logger level
 logger.logLevel = .debug

 // create the client
 let client = ClientOptions().url(URL(string: natsServer.clientURL)!).build()

 // connect to the server
 try await client.connect()

 // subscribe to a subject
 let subscription = try await client.subscribe(to: "events.>")

 // publish a message
 try client.publish("msg".data(using: .utf8)!, subject: "events.example")

 for await message in subscriptions {
    print( "payload: \(String(data:message.payload!, encoding: .utf8)!)")
 }

### Setting the loglevel
The default loglevel is `.error`. You can reset it to see more verbose messages. Possible
Values are `.debug`, `.info`, `.error` or `.critical`

//FIXME: not a proper way to set a logging level from outside the library
```swift
let client = NatsClient("http://nats.server:4222")
client.config.loglevel = .info
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


# Attribution
This library is based on excellent work in https://github.com/aus-der-Technik/NatsSwift
