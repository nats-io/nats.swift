//
//  PublishTests.swift
//  NatsSwiftTest
//


import XCTest
@testable import NatsSwift

class PublishTests: XCTestCase {
    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testClientPublish() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)
        
        try? client.connect()

        var responses = [String]()
        let _ = client.subscribe(to: "swift.test") { m in
            responses.append(m.payload!)
        }

        client.publish("test-1", to: "swift.test")
        client.publish("test-2", to: "swift.test")
        client.publish("test-3", to: "swift.test")

        sleep(1) // give the server 1 second to process the messages and publish them back to our subscription...

        XCTAssertTrue(responses.contains("test-1"))
        XCTAssertTrue(responses.contains("test-2"))
        XCTAssertTrue(responses.contains("test-3"))

        client.disconnect()

    }

    func testClientPublishSync() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)
        
        try? client.connect()

        var responses = [String]()
        let _ = client.subscribe(to: "swift.test") { m in
            responses.append(m.payload!)
        }

        do {
            try client.publishSync("test-1", to: "swift.test")
            try client.publishSync("test-2", to: "swift.test")
            try client.publishSync("test-3", to: "swift.test")
        } catch {
            XCTFail("Client failed to publish message")
        }

        sleep(1) // give the server 1 second to process the messages and publish them back to our subscription...

        XCTAssertTrue(responses.contains("test-1"))
        XCTAssertTrue(responses.contains("test-2"))
        XCTAssertTrue(responses.contains("test-3"))

        client.disconnect()

    }

}

