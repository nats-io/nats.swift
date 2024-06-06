//
//  File.swift
//
//
//  Created by Piotr Piotrowski on 05/06/2024.
//

import Logging
import NIO
import Nats
import NatsServer
import XCTest

@testable import JetStream

class RequestTests: XCTestCase {

    static var allTests = [
        ("testRequest", testRequest)
    ]

    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testRequest() async throws {

        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let stream = """
            {
                "name": "FOO",
                "subjects": ["foo"]
            }
            """
        let data = stream.data(using: .utf8)!

        _ = try await client.request(data, subject: "$JS.API.STREAM.CREATE.FOO")

        let info: Response<AccountInfo> = try await ctx.request("INFO", message: Data())

        guard case .success(let info) = info else {
            XCTFail("request should be successful")
            return
        }

        XCTAssertEqual(info.streams, 1)
        let badInfo: Response<AccountInfo> = try await ctx.request(
            "STREAM.INFO.BAD", message: Data())
        guard case .error(let jetStreamAPIResponse) = badInfo else {
            XCTFail("should get error")
            return
        }

        XCTAssertEqual(ErrorCode.streamNotFound, jetStreamAPIResponse.error.errorCode)

    }
}
