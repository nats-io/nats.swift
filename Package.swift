// swift-tools-version:5.7

import PackageDescription

let package = Package(
    name: "NatsSwift",
    platforms: [
        .macOS(.v10_15)
    ],
    products: [
        .library(name: "NatsSwift", targets: ["NatsSwift"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.2")
    ],
    targets: [
        .target(name: "NatsSwift", dependencies: [
            .product(name: "NIO", package: "swift-nio"),
            .product(name: "Logging", package: "swift-log")
        ]),
        .testTarget(name: "NatsSwiftTests", dependencies: ["NatsSwift"]),

        .executableTarget(name: "Benchmark", dependencies: ["NatsSwift"])
    ]
)

