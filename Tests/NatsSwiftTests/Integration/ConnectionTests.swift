//
//  NatsClientTests.swift
//  NatsSwiftTests
//

import XCTest
import NIO
import Logging
@testable import NatsSwift

class CoreNatsTests: XCTestCase {

    static var allTests = [
        ("testPublish", testPublish),
        ("testPublishWithReply", testPublishWithReply),
        ("testConnect", testConnect)
    ]
   var natsServer = NatsServer()

     override func tearDown() {
         super.tearDown()
         natsServer.stop()
     }

     func testPublish() async throws {
        natsServer.start()
        logger.logLevel = .debug
        let client = Client(url: URL(string: natsServer.clientURL)!)
        try await client.connect()
        let sub = try await client.subscribe(to: "test")

        try client.publish("msg".data(using: .utf8)!, subject: "test")
        let expectation = XCTestExpectation(description: "Should receive message in 5 seconsd")
        Task {
            if let msg = await sub.next() {
                XCTAssertEqual(msg.subject, "test")
                expectation.fulfill()
            }
        }
        wait(for: [expectation], timeout: 5.0)
        await sub.complete()
     }

     func testPublishWithReply() async throws {
        natsServer.start()
        logger.logLevel = .debug
        let client = Client(url: URL(string: natsServer.clientURL)!)
        try await client.connect()
        let sub = try await client.subscribe(to: "test")

        try client.publish("msg".data(using: .utf8)!, subject: "test", reply: "reply")
        let expectation = XCTestExpectation(description: "Should receive message in 5 seconsd")
        Task {
            if let msg = await sub.next() {
                XCTAssertEqual(msg.subject, "test")
                XCTAssertEqual(msg.replySubject, "reply")
                expectation.fulfill()
            }
        }
        wait(for: [expectation], timeout: 5.0)
        await sub.complete()
     }

    func testConnect() async throws {
        natsServer.start()
        logger.logLevel = .debug
        let client = Client(url: URL(string: natsServer.clientURL)!)
        try await client.connect()
        XCTAssertNotNil(client, "Client should not be nil")
    }
}
