// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Dispatch
import Foundation
import Logging
import NIO
import NIOFoundationCompat

public class NatsClientOptions {
    private var urls: [URL] = []
    private var pingInterval: TimeInterval = 60.0
    private var reconnectWait: TimeInterval = 2.0
    private var maxReconnects: Int?
    private var initialReconnect = false
    private var noRandomize = false
    private var auth: Auth? = nil
    private var withTls = false
    private var tlsFirst = false
    private var rootCertificate: URL? = nil
    private var clientCertificate: URL? = nil
    private var clientKey: URL? = nil

    public init() {}

    public func urls(_ urls: [URL]) -> NatsClientOptions {
        self.urls = urls
        return self
    }

    public func url(_ url: URL) -> NatsClientOptions {
        self.urls = [url]
        return self
    }

    public func pingInterval(_ pingInterval: TimeInterval) -> NatsClientOptions {
        self.pingInterval = pingInterval
        return self
    }

    public func reconnectWait(_ reconnectWait: TimeInterval) -> NatsClientOptions {
        self.reconnectWait = reconnectWait
        return self
    }

    public func maxReconnects(_ maxReconnects: Int) -> NatsClientOptions {
        self.maxReconnects = maxReconnects
        return self
    }

    public func usernameAndPassword(_ username: String, _ password: String) -> NatsClientOptions {
        if self.auth == nil {
            self.auth = Auth(user: username, password: password)
        } else {
            self.auth?.user = username
            self.auth?.password = password
        }
        return self
    }

    public func token(_ token: String) -> NatsClientOptions {
        if self.auth == nil {
            self.auth = Auth(token: token)
        } else {
            self.auth?.token = token
        }
        return self
    }

    public func credentialsFile(_ credentials: URL) -> NatsClientOptions {
        if self.auth == nil {
            self.auth = Auth.fromCredentials(credentials)
        } else {
            self.auth?.credentialsPath = credentials
        }
        return self
    }

    public func requireTls() -> NatsClientOptions {
        self.withTls = true
        return self
    }

    public func withTlsFirst() -> NatsClientOptions {
        self.tlsFirst = true
        return self
    }

    public func rootCertificates(_ rootCertificate: URL) -> NatsClientOptions {
        self.rootCertificate = rootCertificate
        return self
    }

    public func clientCertificate(_ clientCertificate: URL, _ clientKey: URL) -> NatsClientOptions {
        self.clientCertificate = clientCertificate
        self.clientKey = clientKey
        return self
    }

    public func retainServersOrder() -> NatsClientOptions {
        self.noRandomize = true
        return self
    }

    public func retryOnfailedConnect() -> NatsClientOptions {
        self.initialReconnect = true
        return self
    }

    public func build() -> NatsClient {
        let client = NatsClient()
        client.connectionHandler = ConnectionHandler(
            inputBuffer: client.buffer,
            urls: urls,
            reconnectWait: reconnectWait,
            maxReconnects: maxReconnects,
            retainServersOrder: noRandomize,
            pingInterval: pingInterval,
            auth: auth,
            requireTls: withTls,
            tlsFirst: tlsFirst,
            clientCertificate: clientCertificate,
            clientKey: clientKey,
            rootCertificate: rootCertificate,
            retryOnFailedConnect: initialReconnect
        )

        return client
    }
}
