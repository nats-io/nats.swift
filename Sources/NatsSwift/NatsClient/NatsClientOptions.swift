//
//  NatsClientOptions.swift
//  NatsSwift
//

import Foundation
import NIO
import NIOFoundationCompat
import Dispatch

public class ClientOptions {
    private var urls: [URL] = []
    private var pingInterval: TimeInterval = 60.0
    private var reconnectWait: TimeInterval = 2.0
    private var maxReconnects: Int = 60

    public init() {}

    public func urls(_ urls: [URL]) -> ClientOptions {
        self.urls = urls
        return self
    }
    
    public func url(_ url: URL) -> ClientOptions {
        self.urls = [url]
        return self
    }

    public func pingInterval(_ pingInterval: TimeInterval) -> ClientOptions {
        self.pingInterval = pingInterval
        return self
    }

    public func reconnectWait(_ reconnectWait: TimeInterval) -> ClientOptions {
        self.reconnectWait = reconnectWait
        return self
    }

    public func maxReconnects(_ maxReconnects: Int) -> ClientOptions {
        self.maxReconnects = maxReconnects
        return self
    }

    public func build() -> Client {
        let client = Client()
        client.connectionHandler = ConnectionHandler(
            inputBuffer: client.buffer,
            urls: urls,
            reconnectWait: reconnectWait,
            maxReconnects: maxReconnects,
            pingInterval: pingInterval
        )
        
        return client
    }
}
