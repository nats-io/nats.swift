//
//  NatsClient.swift
//  NatsSwift
//

import Foundation
import NIO
import Dispatch

/// Client connection states
public enum NatsState {
    case connected
    case disconnected
}

/// Nats events
public enum NatsEvent: String {
    case connected      = "connected"
    case disconnected   = "disconnected"
    case response       = "response"
    case error          = "error"
    case dropped        = "dropped"
    case reconnecting   = "reconnecting"
    case informed       = "informed"
    static let all      = [ connected, disconnected, response, error, dropped, reconnecting ]
}

internal enum NatsOperation: String {
    case connect        = "CONNECT"
    case subscribe      = "SUB"
    case unsubscribe    = "UNSUB"
    case publish        = "PUB"
    case message        = "MSG"
    case info           = "INFO"
    case ok             = "+OK"
    case error          = "-ERR"
    case ping           = "PING"
    case pong           = "PONG"
}

/// a Nats client
open class NatsClient: NSObject {
    var urls = [String]()
    var connectedUrl: URL?
    public var config: NatsClientConfig

    internal var server: NatsServer?
    internal var writeQueue = OperationQueue()
    internal var eventHandlerStore: [ NatsEvent: [ NatsEventHandler ] ] = [:]
    internal var subjectHandlerStore: [ NatsSubject: (NatsMessage) -> Void] = [:]
    internal var messageQueue = OperationQueue()
    public var connectionState: NatsState {
        get { return state }
    }
    internal var state: NatsState = .disconnected {
        didSet {
            // fire event when state is changed only
            if oldValue != state {
            switch state {
                case .connected:
                    self.fire(.connected)
                case .disconnected:
                    self.fire(.disconnected)
            }
            }
        }
    }
    internal var connectionError: NatsError?
    internal var group: MultiThreadedEventLoopGroup?
    internal var thread: Thread?
    internal var channel: Channel?
    internal let dispatchGroup = DispatchGroup()

    // Buffer where incoming messages will be stroed
    internal var inputBuffer: ByteBuffer?

    public init(_ aUrls: [String], _ config: NatsClientConfig) {
        for u in aUrls { self.urls.append(u) }
        self.config = config

        writeQueue.maxConcurrentOperationCount = 1
        logger.debug("Init NatsClient with config: \(config)")
    }

    public convenience init(_ url: String, _ config: NatsClientConfig? = nil) {
        let config = config ?? NatsClientConfig()
        self.init([ url ], config)
    }
}

// MARK: - Protocols

protocol NatsConnection {
    func connect() throws
    func disconnect()
}

protocol NatsSubscribe {
    func subscribe(to subject: String, _ handler: @escaping (NatsMessage) -> Void) -> NatsSubject
    func subscribe(to subject: String, asPartOf queue: String, _ handler: @escaping (NatsMessage) -> Void) -> NatsSubject
    func unsubscribe(from subject: NatsSubject)

    func subscribeSync(to subject: String, _ handler: @escaping (NatsMessage) -> Void) throws -> NatsSubject
    func subscribeSync(to subject: String, asPartOf queue: String, _ handler: @escaping (NatsMessage) -> Void) throws -> NatsSubject
    func unsubscribeSync(from subject: NatsSubject) throws
}

protocol NatsPublish {
    func publish(_ payload: String, to subject: String)
    func publish(_ payload: String, to subject: NatsSubject)
    func reply(to message: NatsMessage, withPayload payload: String)

    func publishSync(_ payload: String, to subject: String) throws
    func publishSync(_ payload: String, to subject: NatsSubject) throws
    func replySync(to message: NatsMessage, withPayload payload: String) throws
}

protocol NatsEventBus {
    func on(_ events: [NatsEvent], _ handler: @escaping (NatsEvent) -> Void) -> String
    func on(_ event: NatsEvent, _ handler: @escaping (NatsEvent) -> Void) -> String
    func on(_ event: NatsEvent, autoOff: Bool, _ handler: @escaping (NatsEvent) -> Void) -> String
    func on(_ events: [NatsEvent], autoOff: Bool, _ handler: @escaping (NatsEvent) -> Void) -> String
    func off(_ id: String)
}

protocol NatsQueue {
    var queueCount: Int { get }
    func flushQueue(maxWait: TimeInterval?) throws
}
