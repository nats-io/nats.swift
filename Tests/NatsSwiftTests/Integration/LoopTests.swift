//
//  LoopTests.swift
//  NatsSwiftTests
//

import XCTest
@testable import NatsSwift

class LoopTests: XCTestCase {

    func testReadSubscriptionInLoop() {

        let clientPublish = NatsClient(TestSettings.natsUrl)
        let clientSubscribe = NatsClient(TestSettings.natsUrl)

        try? clientPublish.connect()
        try? clientSubscribe.connect()

        let runCountExpectation = expectation(description: "Callback Subscribe")
        let runLoops = 10
        runCountExpectation.expectedFulfillmentCount = runLoops
        clientSubscribe.subscribe(to: "swift.test") { message in
            print("-> \(message.payload!)")
            runCountExpectation.fulfill()

            guard let byteCount = message.byteCount else {
                XCTFail("No message.byteCount")
                return
            }
            guard let payload = message.payload else {
                XCTFail("No message.payload")
                return
            }
            if byteCount != payload.count {
                print("byteCount \(byteCount)")
                print("payload [\(payload.count)] \(payload)")
                XCTFail("ERROR DETECTED")
            }

        }

        for i in 1...runLoops {
            clientPublish.publish("S....................\(i)....................E", to: "swift.test")
        }

        waitForExpectations(timeout: 5.0, handler: nil)

        clientPublish.disconnect()
        clientSubscribe.disconnect()
    }

}
