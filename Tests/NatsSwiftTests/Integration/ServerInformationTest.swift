//
//  ServerInformation.swift
//  NatsSwiftTest
//

import Foundation
import XCTest
@testable import NatsSwift

class ServerInformationTest: XCTestCase {
    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testServerInformation() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)

        XCTAssertNil(client.serverInformation)

        try? client.connect()
        XCTAssertNotNil(client.serverInformation)

        client.disconnect()
        XCTAssertNil(client.serverInformation)
    }

    func testServerInformationPropertiesSet() {
        natsServer.start()
        let client = NatsClient(natsServer.clientURL)

        try? client.connect()
        XCTAssertEqual(client.serverInformation?.host, "0.0.0.0")
        XCTAssertEqual(client.serverInformation?.port, natsServer.port!)
        
        XCTAssert(client.serverInformation?.serverName.count ?? 0 > 0)
        XCTAssert(client.serverInformation?.serverId.count ?? 0 > 0)
        XCTAssert(client.serverInformation?.version.count ?? 0 >= 5)

        XCTAssertGreaterThan(client.serverInformation?.maxPayload ?? 0, 0)

        client.disconnect()
    }
}
