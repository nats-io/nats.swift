//
//  NatsClientConfig.swift
//  NatsSwift
//

import Foundation
import Logging

var logger = Logger(label: "NatsSwift")
public let libVersion = "0.1"

public struct NatsClientConfig {

    // logging
    public var loglevel: Logger.Level = .error {
        didSet {
            logger.logLevel = loglevel
        }
    }

    // Required for nats server
    public let verbose: Bool
    public let pedantic: Bool
    public let name: String
    let lang: String = "Swift"
    let version: String = libVersion

    // Internal config vars
    public var internalQueueMax: Int = Int.max

    public init(
        verbose: Bool = false,
        pedantic: Bool = false,
        name: String = "NatsSwift \(libVersion)",
        loglevel: Logger.Level? = .error
    ) {
        self.verbose = verbose
        self.pedantic = pedantic
        self.name = name

        if let level = loglevel {
            self.loglevel = level
        }
    }
}
