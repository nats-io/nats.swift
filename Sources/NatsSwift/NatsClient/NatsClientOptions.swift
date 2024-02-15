//
//  NatsClientOptions.swift
//  NatsSwift
//

import Dispatch
import Foundation
import NIO
import NIOFoundationCompat

public class ClientOptions {
    private var urls: [URL] = []
    private var pingInterval: TimeInterval = 60.0
    private var reconnectWait: TimeInterval = 2.0
    private var maxReconnects: Int = 60
    private var auth: Auth? = nil
    private var withTls: Bool = false
    private var tlsFirst: Bool = false
    private var rootCertificate: URL? = nil
    private var clientCertificate: URL? = nil
    private var clientKey: URL? = nil

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

    public func usernameAndPassword(_ username: String, _ password: String) -> ClientOptions {
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

    public func credentialsFile(_ credentials: URL) -> ClientOptions {
        if self.auth == nil {
            self.auth = Auth.fromCredentials(credentials)
        } else {
            self.auth?.credentialsPath = credentials
        }
        return self
    }

    public func requireTls() -> ClientOptions {
        self.withTls = true
        return self
    }

    public func withTlsFirst() -> ClientOptions {
        self.tlsFirst = true
        return self
    }

    public func rootCertificates(_ rootCertificate: URL) -> ClientOptions {
        self.rootCertificate = rootCertificate
        return self
    }

    public func clientCertificate(_ clientCertificate: URL, _ clientKey: URL) -> ClientOptions {
        self.clientCertificate = clientCertificate
        self.clientKey = clientKey
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
            auth: auth,
            requireTls: withTls,
            tlsFirst: tlsFirst,
            clientCertificate: clientCertificate,
            clientKey: clientKey,
            rootCertificate: rootCertificate
        )

        return client
    }
}
