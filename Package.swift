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
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.2"),
        .package(url: "https://github.com/nats-io/nkeys.swift.git", from: "0.1.1"),
    ],
    targets: [
        .target(
            name: "NatsSwift",
            dependencies: [
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "NKeys", package: "nkeys.swift"),
            ]),
        .testTarget(
            name: "NatsSwiftTests",
            dependencies: ["NatsSwift"],
            resources: [
                .process("Integration/Resources")
            ]
        ),

        .executableTarget(name: "Benchmark", dependencies: ["NatsSwift"]),
        .executableTarget(name: "BenchmarkPubSub", dependencies: ["NatsSwift"]),
        .executableTarget(name: "BenchmarkSub", dependencies: ["NatsSwift"]),
    ]
)
