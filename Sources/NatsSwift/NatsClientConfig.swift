//
//  NatsClientConfig.swift
//  SwiftyNats
//

import Foundation
import Logging

var logger = Logger(label: "SwiftyNats")
public let libVersion = "2.2"

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
        name: String = "SwiftyNats \(libVersion)",
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
