// swift-tools-version:6.0

import PackageDescription

let package = Package(
    name: "nats-swift",
    platforms: [
        .macOS(.v13),
        .iOS(.v13),
        .tvOS(.v13),
        .watchOS(.v6),
        .visionOS(.v1),
    ],
    products: [
        .library(name: "Nats", targets: ["Nats"]),
        .library(name: "JetStream", targets: ["JetStream"]),
        .library(name: "JetStreamKeyValue", targets: ["JetStreamKeyValue"]),
        .library(name: "JetStreamObjectStore", targets: ["JetStreamObjectStore"]),
        .library(name: "NatsServer", targets: ["NatsServer"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.105.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.17.0"),
        .package(url: "https://github.com/nats-io/nkeys.swift.git", from: "0.2.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.37.6"),
        .package(url: "https://github.com/Jarema/swift-nuid.git", from: "0.3.0"),
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
            ]),
        .target(
            name: "JetStreamKeyValue",
            dependencies: ["Nats", "JetStream"]),
        .target(
            name: "JetStreamObjectStore",
            dependencies: ["Nats", "JetStream"]),
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
                dependencies: [
                    "Nats", "JetStream", "JetStreamKeyValue", "JetStreamObjectStore", "NatsServer",
                ],
                resources: [
                .process("Integration/Resources")
                ]
        ),
        .executableTarget(
            name: "bench", dependencies: ["Nats"],
            path: "Sources/Benchmarks/bench"),
        .executableTarget(
            name: "Benchmark", dependencies: ["Nats"],
            path: "Sources/Benchmarks/Benchmark"),
        .executableTarget(
            name: "BenchmarkPubSub", dependencies: ["Nats"],
            path: "Sources/Benchmarks/BenchmarkPubSub"),
        .executableTarget(
            name: "BenchmarkSub", dependencies: ["Nats"],
            path: "Sources/Benchmarks/BenchmarkSub"),
        .executableTarget(
            name: "JetStreamBenchmark",
            dependencies: ["Nats", "JetStream", "JetStreamKeyValue", "JetStreamObjectStore"],
            path: "Sources/Benchmarks/JetStreamBenchmark"),
        .executableTarget(
            name: "Example", dependencies: ["Nats"],
            path: "Sources/Examples/Example"),
        .executableTarget(
            name: "JetStreamExample",
            dependencies: ["Nats", "JetStream", "JetStreamKeyValue", "JetStreamObjectStore"],
            path: "Sources/Examples/JetStreamExample"),
    ],
    swiftLanguageModes: [.v6]
)
