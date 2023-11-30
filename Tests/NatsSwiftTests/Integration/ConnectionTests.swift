//
//  NatsClientTests.swift
//  NatsSwiftTests
//

import XCTest
import NIO
@testable import NatsSwift

class ConnectionTests: XCTestCase {

    static var allTests = [
        ("testClientConnection", testClientConnection),
        ("testClientServerSetWhenConnected", testClientServerSetWhenConnected),
        ("testClientBadConnection", testClientBadConnection)
    ]
    
    var natsServer = NatsServer()
    
    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testClientConnection() {
        natsServer.start()

        let client = NatsClient(natsServer.clientURL)

        try? client.connect()
            
        XCTAssertTrue(client.state == .connected, "Client did not connect")

    }

    func testClientServerSetWhenConnected() {
        natsServer.start()

        let client = NatsClient(natsServer.clientURL)

        try? client.connect()
        guard let _ = client.server else { XCTFail("Client did not connect to server correctly"); return }

    }

    func testClientBadConnection() {
        natsServer.start()

        let client = NatsClient("notnats.net")

        try? client.connect()
        XCTAssertTrue(client.state == .disconnected, "Client should not have connected")

    }

    func testClientConnectionLogging() {
        natsServer.start()

        let client = NatsClient(natsServer.clientURL)
        client.config.loglevel = .trace
        try? client.connect()
        XCTAssertTrue(client.state == .connected, "Client did not connect")

    }

    func testClientConnectDisconnect() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)
        client.config.loglevel = .trace

        try? client.connect()
        XCTAssertTrue(client.state == .connected, "Client did not connect")
        XCTAssertNotNil(client.server)
        XCTAssertTrue(client.channel!.isActive)

        client.disconnect()
        XCTAssertTrue(client.state == .disconnected, "Client should be disconnect")
        XCTAssertNil(client.server)
        XCTAssertFalse(client.channel!.isActive)

        try? client.connect()
        XCTAssertTrue(client.state == .connected, "Client did not connect")
        XCTAssertNotNil(client.server)
        XCTAssertTrue(client.channel!.isActive)

        client.disconnect()
        XCTAssertTrue(client.state == .disconnected, "Client should be disconnect")
        XCTAssertNil(client.server)
        XCTAssertFalse(client.channel!.isActive)
    }

    func testClientReconnectWhenAlreadyConnected() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)
        client.config.loglevel = .trace

        try? client.connect()
        XCTAssertTrue(client.state == .connected, "Client did not connect")
        XCTAssertNotNil(client.server)
        XCTAssertTrue(client.channel!.isActive)

        try? client.reconnect()
        XCTAssertNotNil(client.server)
        XCTAssertTrue(client.channel!.isActive)
    }
}
