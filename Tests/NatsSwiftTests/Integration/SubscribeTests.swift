//
//  SubscribeTests.swift
//  SwiftyNatsTests
//

import XCTest
@testable import SwiftyNats

class SubscribeTests: XCTestCase {
    
    func testClientSubscription() {
        
        let client = NatsClient(TestSettings.natsUrl)
        
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

        let client = NatsClient(TestSettings.natsUrl)
        
        try? client.connect()
        
        let handler: (NatsMessage) -> Void = { message in
            
        }
        
        guard let _ = try? client.subscribeSync(to: "swift.test", handler) else {
            XCTFail("Subscription failed");
            return
        }
        
        client.disconnect()
        
    }
    
}
