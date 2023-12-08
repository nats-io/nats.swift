//
//  SubscribeTests.swift
//  NatsSwiftTests
//

import XCTest
@testable import NatsSwift

class SubscribeTests: XCTestCase {
    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testClientSubscription() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)
        
        try? client.connect()

        var subscribed = false
        client.on(.response) { _ in
            subscribed = true
        }

        client.subscribe(to: "swift.test") { m in }

        sleep(1) // subscribe is async so we need about .1 seconds for the server to respond with an OK

        XCTAssertTrue(subscribed == true, "Client did not subscribe")

        client.disconnect()
    }

    func testClientSubscriptionSync() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)
        
        try? client.connect()

        let handler: (OldNatsMessage) -> Void = { message in

        }

        guard let _ = try? client.subscribeSync(to: "swift.test", handler) else {
            XCTFail("Subscription failed");
            return
        }

        client.disconnect()

    }

}
