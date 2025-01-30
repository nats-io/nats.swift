// swift-tools-version:5.7

import PackageDescription

let package = Package(
    name: "nats-swift",
    platforms: [
        .macOS(.v13),
        .iOS(.v13),
    ],
    products: [
        .library(name: "Nats", targets: ["Nats"]),
        .library(name: "JetStream", targets: ["JetStream"]),
        .library(name: "NatsServer", targets: ["NatsServer"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.2"),
        .package(url: "https://github.com/benbenbenbenbenben/nkeys.swift.git", branch: "main"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.0.0"),
        .package(url: "https://github.com/benbenbenbenbenben/swift-nuid.git", branch: "main"),
        .package(url: "https://github.com/apple/swift-crypto.git", "1.0.0" ..< "4.0.0"),
    ],
    targets: [
        .target(
            name: "Nats",
            dependencies: [
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(name: "NIOWebSocket", package: "swift-nio"),
                .product(name: "NKeys", package: "nkeys.swift"),
                .product(name: "Nuid", package: "swift-nuid"),
            ]),
        .target(
            name: "JetStream",
            dependencies: [
                "Nats",
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Crypto", package: "swift-crypto", condition: .when(platforms: [.linux])),
            ]),
        .target(
            name: "NatsServer",
            dependencies: [
                .product(name: "Logging", package: "swift-log"),
            ]),

        .testTarget(
                name: "NatsTests",
                dependencies: ["Nats", "NatsServer"],
                resources: [
                .process("Integration/Resources")
                ]
        ),
        .testTarget(
                name: "JetStreamTests",
                dependencies: ["Nats", "JetStream", "NatsServer"],
                resources: [
                .process("Integration/Resources")
                ]
        ),
        .executableTarget(name: "bench", dependencies: ["Nats"]),
        .executableTarget(name: "Benchmark", dependencies: ["Nats"]),
        .executableTarget(name: "BenchmarkPubSub", dependencies: ["Nats"]),
        .executableTarget(name: "BenchmarkSub", dependencies: ["Nats"]),
        .executableTarget(name: "Example", dependencies: ["Nats"]),
    ]
)
