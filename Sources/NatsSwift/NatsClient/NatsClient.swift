//
//  NatsClient.swift
//  NatsSwift
//

import Dispatch
import Foundation
import Logging
import NIO
import NIOFoundationCompat

var logger = Logger(label: "NatsSwift")

/// Client connection states
public enum NatsState {
    case pending
    case connected
    case disconnected
    case closed
}

public struct Auth {
    var user: String?
    var password: String?
    var token: String?
    var credentialsPath: URL?

    init() {

    }

    init(user: String, password: String) {
        self.user = user
        self.password = password
    }
    init(token: String) {
        self.token = token
    }
    static func fromCredentials(_ credentials: URL) -> Auth {
        var auth = Auth()
        auth.credentialsPath = credentials
        return auth
    }
}

public class Client {
    public var connectedUrl: URL? {
        connectionHandler?.connectedUrl
    }
    internal let allocator = ByteBufferAllocator()
    internal var buffer: ByteBuffer
    internal var connectionHandler: ConnectionHandler?

    internal init() {
        self.buffer = allocator.buffer(capacity: 1024)
    }
}

extension Client {
    public func connect() async throws {
        logger.debug("connect")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        if !connectionHandler.retryOnFailedConnect {
            try await connectionHandler.connect()
        } else {
            connectionHandler.handleReconnect()
        }
    }

    public func close() async throws {
        logger.debug("close")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        try await connectionHandler.close()
    }

    public func publish(
        _ payload: Data, subject: String, reply: String? = nil, headers: HeaderMap? = nil
    ) async throws {
        logger.debug("publish")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        try await connectionHandler.write(operation: ClientOp.publish((subject, reply, payload, headers)))
    }

    public func subscribe(to subject: String) async throws -> Subscription {
        logger.info("subscribe to subject \(subject)")
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        return try await connectionHandler.subscribe(subject)
    }
    
    public func rtt() async throws -> Duration {
        guard let connectionHandler = self.connectionHandler else {
            throw NatsClientError("internal error: empty connection handler")
        }
        let ping = RttCommand.makeFrom(channel: connectionHandler.channel)
        connectionHandler.sendPing(ping)
        return try await ping.getRoundTripTime	()
    }
}

// Experimetal class to make applying options a bit more natural.
// This is also very close to how C# client is designed.
// If accepted we can refactor `Client` and `ClientOptions` types.
public class NatsConnection {
    let client: Client
    
    public init() {
        let options = NatsOptions()
        self.client = Self.buildClient(options)
        logger.logLevel = options.loggerLevel
    }

    public init(_ options: NatsOptions) {
        self.client = Self.buildClient(options)
        logger.logLevel = options.loggerLevel
    }

    public func connect() async throws {
        try await self.client.connect()
    }

    public func close() async throws {
        try await self.client.close()
    }
    
    public func publish(_ payload: Data, to subject: String, reply: String? = nil, headers: HeaderMap? = nil) async throws {
        try await self.client.publish(payload, subject: subject, reply: reply, headers: headers)
    }
    
    public func subscribe(to subject: String) async throws -> Subscription {
        try await self.client.subscribe(to: subject)
    }
    
    public func rtt() async throws -> Duration {
        try await self.client.rtt()
    }
    
    @discardableResult
    public func on(_ events: [NatsEventKind], _ handler: @escaping (NatsEvent) -> Void) -> String {
        guard let connectionHandler = self.client.connectionHandler else {
            return ""
        }
        return connectionHandler.addListeners(for: events, using: handler)
    }

    @discardableResult
    public func on(_ event: NatsEventKind, _ handler: @escaping (NatsEvent) -> Void) -> String {
        guard let connectionHandler = self.client.connectionHandler else {
            return ""
        }
        return connectionHandler.addListeners(for: [event], using: handler)
    }

    func off(_ id: String) {
        guard let connectionHandler = self.client.connectionHandler else {
            return
        }
        connectionHandler.removeListener(id)
    }
    
    static func buildClient(_ options: NatsOptions) -> Client {
        let client = Client()
        
        client.connectionHandler = ConnectionHandler(
            inputBuffer: client.buffer,
            urls: options.urls,
            reconnectWait: options.reconnectWait,
            maxReconnects: options.maxReconnects,
            retainServersOrder: options.noRandomize,
            pingInterval: options.pingInterval,
            auth: options.auth,
            requireTls: options.withTls,
            tlsFirst: options.tlsFirst,
            clientCertificate: options.clientCertificate,
            clientKey: options.clientKey,
            rootCertificate: options.rootCertificate,
            retryOnFailedConnect: options.initialReconnect
        )
        
        return client
    }
}

public struct NatsOptions {
    let urls: [URL]
    let loggerLevel: Logger.Level
    let pingInterval: TimeInterval
    let reconnectWait: TimeInterval
    let maxReconnects: Int?
    let initialReconnect: Bool
    let noRandomize: Bool
    let auth: Auth?
    let withTls: Bool
    let tlsFirst: Bool
    let rootCertificate: URL?
    let clientCertificate: URL?
    let clientKey: URL?
    
    public init(
        urls: [URL] = [URL(string: "nats://localhost:4222")!],
        loggerLevel: Logger.Level = .info,
        pingInterval: TimeInterval = 60.0,
        reconnectWait: TimeInterval = 2.0,
        maxReconnects: Int? = nil,
        initialReconnect: Bool = false,
        noRandomize: Bool = false,
        auth: Auth? = nil,
        withTls: Bool = false,
        tlsFirst: Bool = false,
        rootCertificate: URL? = nil,
        clientCertificate: URL? = nil,
        clientKey: URL? = nil
    ) {
        self.urls = urls
        self.loggerLevel = loggerLevel
        self.pingInterval = pingInterval
        self.reconnectWait = reconnectWait
        self.maxReconnects = maxReconnects
        self.initialReconnect = initialReconnect
        self.noRandomize = noRandomize
        self.auth = auth
        self.withTls = withTls
        self.tlsFirst = tlsFirst
        self.rootCertificate = rootCertificate
        self.clientCertificate = clientCertificate
        self.clientKey = clientKey
    }
}
