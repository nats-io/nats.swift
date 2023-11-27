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

        XCTAssertNotNil(client, "Client should not be nil")
    }

}
