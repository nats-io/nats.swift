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
        ("testSubscribe", testSubscribe),
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
        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
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
        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
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

     func testSubscribe() async throws {
         natsServer.start()
         logger.logLevel = .debug
         let client = ClientOptions().url(URL(string: natsServer.clientURL)!).build()
         try await client.connect()
         let sub = try await client.subscribe(to: "test")
         try client.publish("msg".data(using: .utf8)!, subject: "test")
         let message = await sub.next()
         print( "payload: \(String(data:message!.payload!, encoding: .utf8)!)")
         XCTAssertEqual(message?.payload, "msg".data(using: .utf8)!)
     }

    func testConnect() async throws {
        natsServer.start()
        logger.logLevel = .debug
        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()
        XCTAssertNotNil(client, "Client should not be nil")
    }

    func testReconnect() async throws {
        natsServer.start()
        let port = natsServer.port!
        logger.logLevel = .debug

        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .reconnectWait(1)
            .build()

        try await client.connect()


        // Payload to publish
        let payload = "hello".data(using: .utf8)!

        var messagesReceived = 0
        let sub = try! await client.subscribe(to: "foo")

        // publish some messages
        Task {
            for _ in 0..<10 {
                try client.publish(payload, subject: "foo")
            }
        }

        // make sure sub receives messages
        for _ in 0..<10 {
            let _ = await sub.next()
            messagesReceived += 1
        }

        // restart the server
        natsServer.stop()
        sleep(1)
        natsServer.start(port: port)
        sleep(2)

        // publish more messages, sub should receive them
        Task {
            for _ in 0..<10 {
                try client.publish(payload, subject: "foo")
            }
        }
        for _ in 0..<10 {
            let _ = await sub.next()
            messagesReceived += 1
        }

        // Check if the total number of messages received matches the number sent
        XCTAssertEqual(20, messagesReceived, "Mismatch in the number of messages sent and received")
    }
}
