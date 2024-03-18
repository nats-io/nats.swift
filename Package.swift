// swift-tools-version:5.7

import PackageDescription

let package = Package(
    name: "nats-swift",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .library(name: "Nats", targets: ["Nats"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.2"),
        .package(url: "https://github.com/nats-io/nkeys.swift.git", from: "0.1.1"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.0.0"),
        .package(url: "https://github.com/Jarema/swift-nuid.git", from: "0.2.0"),
    ],
    targets: [
        .target(
            name: "Nats",
            dependencies: [
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "NKeys", package: "nkeys.swift"),
                .product(name: "Nuid", package: "swift-nuid"),
            ]),
        .testTarget(
            name: "NatsTests",
            dependencies: ["Nats"],
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
