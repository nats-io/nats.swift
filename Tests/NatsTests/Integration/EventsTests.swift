//
//  EventsTests.swift
//  NatsTests
//

import Foundation
import Logging
import XCTest

@testable import Nats


class TestNatsEvents: XCTestCase {

    static var allTests = [
        ("testClientConnectedEvent", testClientConnectedEvent),
        ("testClientConnectedEvent", testClientConnectedEvent),
        ("testClientConnectedEvent", testClientConnectedEvent),
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testClientConnectedEvent() async throws {
        natsServer.start()
        logger.logLevel = .debug

        let client = ClientOptions().url(URL(string: natsServer.clientURL)!).build()

        let expectation = XCTestExpectation(
            description: "client was not notified of connection established event")
        client.on(.connected) { event in
            XCTAssertEqual(event.kind(), NatsEventKind.connected)
            expectation.fulfill()
        }
        try await client.connect()

        await fulfillment(of: [expectation], timeout: 1.0)
        try await client.close()
    }

    func testClientClosedEvent() async throws {
        natsServer.start()
        logger.logLevel = .debug

        let client = ClientOptions().url(URL(string: natsServer.clientURL)!).build()

        let expectation = XCTestExpectation(
            description: "client was not notified of connection closed event")
        client.on(.closed) { event in
            XCTAssertEqual(event.kind(), NatsEventKind.closed)
            expectation.fulfill()
        }
        try await client.connect()

        try await client.close()
        await fulfillment(of: [expectation], timeout: 1.0)
    }

    func testClientReconnectEvent() async throws {
        natsServer.start()
        let port = natsServer.port!
        logger.logLevel = .debug

        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .reconnectWait(1)
            .build()

        let disconnected = XCTestExpectation(
            description: "client was not notified of disconnection event")
        client.on(.disconnected) { event in
            XCTAssertEqual(event.kind(), NatsEventKind.disconnected)
            disconnected.fulfill()
        }
        try await client.connect()
        natsServer.stop()

        let reconnected = XCTestExpectation(
            description: "client was not notified of reconnection event")
        client.on(.connected) { event in
            XCTAssertEqual(event.kind(), NatsEventKind.connected)
            reconnected.fulfill()
        }
        await fulfillment(of: [disconnected], timeout: 5.0)

        natsServer.start(port: port)
        await fulfillment(of: [reconnected], timeout: 5.0)

        try await client.close()
    }
}
