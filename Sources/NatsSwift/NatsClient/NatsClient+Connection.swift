//
//  NatsClient+Connection.swift
//  NatsSwift
//

import Foundation
import NIO
import Dispatch

extension NatsClient: NatsConnection {

    // MARK: - Implement NatsConnection Protocol

    /// Connect to the NATS server
    open func connect() throws {
        logger.debug("Try to connect.")
        guard self.state != .connected else {
            logger.info("Already connected, skip connection.")
            return
        }

        self.dispatchGroup.enter()

        #if os(Linux)
        thread = Thread { self.setupConnection() }
        #else
        thread = Thread(target: self, selector: #selector(self.setupConnection), object: nil)
        #endif
        thread?.start()
        self.dispatchGroup.wait()

        if let error = self.connectionError {
            logger.error("Error while connectig.")
            throw error
        }

        if self.server?.authRequired == true {
            logger.warning("Authorisation is required.")
            try self.authenticateWithServer()
        }
    }

    /// Disconnect from the NATS server
    open func disconnect() {
        logger.debug("Try to disconnect.")
        do {
            try self.channel?.close().wait()
        } catch {
            logger.error("\(error.localizedDescription)")
        }
        do {
            try self.group?.syncShutdownGracefully()
        } catch {
            logger.error("\(error.localizedDescription)")
        }
        self.server = nil
    }

    // MARK: - Internal Methods

    open func reconnect() throws {
        self.fire(.reconnecting)

        // disconnect - if not already
        if state == .connected {
            self.disconnect()
        }
        thread?.cancel()
        try self.connect()
    }

    // MARK: - Private Methods

    fileprivate func _setupConnection() {
        self.connectionError = nil
        // If we have a list of `connectUrls` in our current server
        // add them to the list of knownServers here so we can attempt
        // to connect to them as well
        var knownServers = self.urls
        if let otherServers = self.server?.connectUrls {
            knownServers.append(contentsOf: otherServers)
        }
        for server in knownServers {
            do {
                try self.openStream(to: server)
            } catch let e as NatsError {
                self.connectionError = e
                continue // to try next server
            } catch {
                self.connectionError = NatsConnectionError(error.localizedDescription)
                continue
            }
            self.connectedUrl = URL(string: server)
            break // If we got here then we connected successfully, break out of here and stop trying servers
        }
        self.dispatchGroup.leave()
        RunLoop.current.run()
    }

    #if os(macOS) || os(tvOS) || os(iOS)
    @objc fileprivate func setupConnection() {
        _setupConnection()
    }
    #else
    fileprivate func setupConnection() {
        _setupConnection()
    }
    #endif

    /// open the client connection to the streaming serer
    fileprivate func openStream(to url: String) throws {
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        guard let server = URL(string: url) else {
            throw NatsConnectionError("Invalid url provided: (\(url))")
        }

        guard let host = server.host, let port = server.port else {
            throw NatsConnectionError("Invalid url provided: (\(server.absoluteString))")
        }

        // listener
        var isInformed = false
        var hasErrored = false
        self.on([.informed, .error], autoOff: true) { e in
            switch e {
            case .informed:
                isInformed = true
                break
            case .error:
                hasErrored = true
                break
            default:
                break
            }
        }

        let bootstrap = ClientBootstrap(group: self.group!)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
            .channelInitializer { channel in
                channel.pipeline.addHandler(self)
            }.connectTimeout( TimeAmount.seconds(5) )


        let futureConnection = bootstrap.connect(host: host, port: port)
        futureConnection.whenFailure({ err in
            logger.error("\(err.localizedDescription)")
        })
        futureConnection.whenSuccess({channel in
            if hasErrored {
                //throw NatsConnectionError("Server returned an error while trying to connect")
                logger.error("Server returned an error while trying to connect")
            } else {
                self.channel = channel
            }
        })
        _ = try futureConnection.wait()

        // after the connection is done, we need to wait for the server answer
        let timeout: TimeInterval = 5
        let waiterStartTime = Date()
        while isInformed != true {
            if hasErrored {
                throw NatsConnectionError("Server returned an error while trying to connect")
            }
            sleep(1)
            let waitingInterval = Date().timeIntervalSince(waiterStartTime)
            if waitingInterval >= timeout {
                logger.error("Timeout waitung for information.")
                throw NatsConnectionError("Server timedout. Waited \(timeout) seconds for info response but never got it")
            }
        }
    }

    fileprivate func authenticateWithServer() throws {
        guard let user = self.connectedUrl?.user, let password = self.connectedUrl?.password else {
            throw NatsConnectionError("Server authentication requires url with basic authentication")
        }
        let config = [
            "verbose": self.config.verbose,
            "pedantic": self.config.pedantic,
            "ssl_required": server!.sslRequired ?? false,
            "name": self.config.name,
            "lang": self.config.lang,
            "version": self.config.version,
            "user": user,
            "pass": password
            ] as [String : Any]

        self.sendMessage(NatsMessage.connect(config: config))
    }

}
