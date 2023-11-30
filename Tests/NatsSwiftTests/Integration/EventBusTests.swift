//
//  EventBusTests.swift
//  NatsSwiftTest
//

import XCTest
@testable import NatsSwift

class EventBusTests: XCTestCase {

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testClientConnectedEvent() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)
        
        var isConnected = false
        client.on(.connected) { _ in
            isConnected = true
        }

        try? client.connect()

        XCTAssertTrue(isConnected, "Subscriber was not notified of connection")

        client.disconnect()

    }

    func testClientDisconnectedEvent() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)

        try? client.connect()

        var isConnected = true
        client.on(.disconnected) { _ in
            isConnected = false
        }

        client.disconnect()

        XCTAssertTrue(isConnected == false, "Subscriber was not notified of disconnection")

    }

    func testClientEventOff() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)

        try? client.connect()

        var isConnected = true
        let eid = client.on(.disconnected) { _ in
            isConnected = false
        }

        client.off(eid)

        client.disconnect()

        XCTAssertTrue(isConnected == true, "Subscriber was notified of connection and should not have been")

    }

    func testClientEventMultiple() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)
        
        var counter = 0
        client.on([.connected, .disconnected]) { _ in
            counter += 1
        }

        try? client.connect()
        client.disconnect()

        XCTAssertTrue(counter == 2, "Subscriber was not notified of correct events")
    }

    func testClientEventAutoOff() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)
        
        var counter = 0
        client.on([.connected, .disconnected], autoOff: true) { _ in
            counter += 1
        }

        try? client.connect()
        client.disconnect()

        XCTAssertTrue(counter == 1, "Subscriber was notified of incorrect events after autoOff")
    }

}
