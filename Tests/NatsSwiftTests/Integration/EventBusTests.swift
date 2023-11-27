//
//  EventBusTests.swift
//  SwiftyNatsTest
//

import XCTest
@testable import SwiftyNats

class EventBusTests: XCTestCase {
    
    func testClientConnectedEvent() {
        
        let client = NatsClient(TestSettings.natsUrl)
        
        var isConnected = false
        client.on(.connected) { _ in
            isConnected = true
        }
        
        try? client.connect()
        
        XCTAssertTrue(isConnected, "Subscriber was not notified of connection")
        
        client.disconnect()
        
    }

    func testClientDisconnectedEvent() {
        
        let client = NatsClient(TestSettings.natsUrl)
        try? client.connect()
        
        var isConnected = true
        client.on(.disconnected) { _ in
            isConnected = false
        }
        
        client.disconnect()
        
        XCTAssertTrue(isConnected == false, "Subscriber was not notified of disconnection")
        
    }

    func testClientEventOff() {
        
        let client = NatsClient(TestSettings.natsUrl)
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
        
        let client = NatsClient(TestSettings.natsUrl)
        
        var counter = 0
        client.on([.connected, .disconnected]) { _ in
            counter += 1
        }
        
        try? client.connect()
        client.disconnect()
        
        XCTAssertTrue(counter == 2, "Subscriber was not notified of correct events")
    }

    func testClientEventAutoOff() {
        
        let client = NatsClient(TestSettings.natsUrl)
        
        var counter = 0
        client.on([.connected, .disconnected], autoOff: true) { _ in
            counter += 1
        }
        
        try? client.connect()
        client.disconnect()
        
        XCTAssertTrue(counter == 1, "Subscriber was notified of incorrect events after autoOff")
    }
    
}
