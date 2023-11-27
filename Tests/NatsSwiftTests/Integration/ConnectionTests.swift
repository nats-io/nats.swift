//
//  NatsClientTests.swift
//  SwiftyNatsTests
//

import XCTest
import NIO
@testable import SwiftyNats

class ConnectionTests: XCTestCase {

    static var allTests = [
        ("testClientConnection", testClientConnection),
        ("testClientServerSetWhenConnected", testClientServerSetWhenConnected),
        ("testClientBadConnection", testClientBadConnection)
    ]

    func testClientConnection() {

        let client = NatsClient(TestSettings.natsUrl)

        try? client.connect()
        XCTAssertTrue(client.state == .connected, "Client did not connect")

    }

    func testClientServerSetWhenConnected() {

        let client = NatsClient(TestSettings.natsUrl)

        try? client.connect()
        guard let _ = client.server else { XCTFail("Client did not connect to server correctly"); return }

    }

    func testClientBadConnection() {

        let client = NatsClient("notnats.net")

        try? client.connect()
        XCTAssertTrue(client.state == .disconnected, "Client should not have connected")

    }
    
    func testClientConnectionLogging() {

        let client = NatsClient(TestSettings.natsUrl)
        client.config.loglevel = .trace
        try? client.connect()
        XCTAssertTrue(client.state == .connected, "Client did not connect")

    }
    
    func testClientConnectDisconnect() {
        let client = NatsClient(TestSettings.natsUrl)
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
        let client = NatsClient(TestSettings.natsUrl)
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
