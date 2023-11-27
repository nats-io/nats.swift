//
//  ServerInformation.swift
//  NatsSwiftTest
//

import Foundation
import XCTest
@testable import NatsSwift

class ServerInformationTest: XCTestCase {

    func testServerInformation() {

        let client = NatsClient(TestSettings.natsUrl)
        XCTAssertNil(client.serverInformation)

        try? client.connect()
        XCTAssertNotNil(client.serverInformation)

        client.disconnect()
        XCTAssertNil(client.serverInformation)
    }

    func testServerInformationPropertiesSet() {
        let client = NatsClient(TestSettings.natsUrl)
        try? client.connect()
        XCTAssertEqual(client.serverInformation?.host, "0.0.0.0")
        XCTAssertEqual(client.serverInformation?.port, 4222)

        XCTAssert(client.serverInformation?.serverName.count ?? 0 > 0)
        XCTAssert(client.serverInformation?.serverId.count ?? 0 > 0)
        XCTAssert(client.serverInformation?.version.count ?? 0 >= 5)

        XCTAssertGreaterThan(client.serverInformation?.maxPayload ?? 0, 0)

        client.disconnect()
    }
}
