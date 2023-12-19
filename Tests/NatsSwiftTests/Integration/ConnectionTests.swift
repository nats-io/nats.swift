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
   var natsServer = NatsServer()

     override func tearDown() {
         super.tearDown()
         natsServer.stop()
     }

    func testNewClient() async throws {
        natsServer.start()
        logger.logLevel = .debug
        print("Testing new client")
        logger.debug("Testing new client with log")
        let client = Client(url: URL(string: natsServer.clientURL)!)
        try await client.connect()
        let sub = try await client.subscribe(to: "test")
        try client.publish("msg".data(using: .utf8)!, subject: "test")

        if let msg = await sub.next() {
            print("Received on \(msg.subject): \(msg.payload!)")
        }
        await sub.complete()

        XCTAssertNotNil(client, "Client should not be nil")
    }
}
