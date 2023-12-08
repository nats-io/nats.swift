//
//  NatsClientTests.swift
//  NatsSwiftTests
//

import XCTest
import NIO
import Logging
@testable import NatsSwift

class ConnectionTests: XCTestCase {
    
    static var allTests = [
        ("testNewClient", testNewClient)
    ]
    
    func testNewClient() async throws {
        logger.logLevel = .debug
        print("Testing new client")
        logger.debug("Testing new client with log")
        let client = Client(url: URL(string: TestSettings.natsUrl)!)
        try await client.connect()
        let sub = try await client.subscribe(to: "test")
        try await client.publish("msg", subject: "test")
        
        if let msg = await sub.next() {
            print("Received on \(msg.subject): \(msg.payload!)")
        }
        await sub.complete()
        
        XCTAssertNotNil(client, "Client should not be nil")
    }
}
