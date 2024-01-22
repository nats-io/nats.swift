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
    private var auth: Auth? = nil

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

    public func username_and_password(_ username: String, _ password: String) -> ClientOptions {
        if self.auth == nil {
            self.auth = Auth(user: username, password: password)
        } else {
            self.auth?.user = username
            self.auth?.password = password
        }
        return self
    }

    public func token(_ token: String) -> ClientOptions {
        if self.auth == nil {
            self.auth = Auth(token: token)
        } else {
            self.auth?.token = token
        }
        return self
    }

    public func build() -> Client {
        let client = Client()
        client.connectionHandler = ConnectionHandler(
            inputBuffer: client.buffer,
            urls: urls,
            reconnectWait: reconnectWait,
            maxReconnects: maxReconnects,
            pingInterval: pingInterval,
            auth: auth
        )

        return client
    }
}
