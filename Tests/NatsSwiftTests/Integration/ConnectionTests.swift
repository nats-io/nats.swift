//
//  NatsClientTests.swift
//  NatsSwiftTests
//

import Logging
import NIO
import XCTest

@testable import NatsSwift

class CoreNatsTests: XCTestCase {

    static var allTests = [
        ("testRtt", testRtt),
        ("testPublish", testPublish),
        ("testPublishWithReply", testPublishWithReply),
        ("testSubscribe", testSubscribe),
        ("testConnect", testConnect),
        ("testReconnect", testReconnect),
        ("testUsernameAndPassword", testUsernameAndPassword),
        ("testTokenAuth", testTokenAuth),
        ("testCredentialsAuth", testCredentialsAuth),
        ("testMutualTls", testMutualTls),
        ("testTlsFirst", testTlsFirst),
        ("testInvalidCertificate", testInvalidCertificate),
        ("testLameDuckMode", testLameDuckMode),
        ("testRequest", testRequest),
    ]
    var natsServer = NatsServer()

    override func tearDown() {
        super.tearDown()
        natsServer.stop()
    }

    func testRtt() async throws {
        natsServer.start()
        logger.logLevel = .debug
        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()

        let rtt: Duration = try await client.rtt()
        XCTAssertGreaterThan(rtt, Duration.zero, "should have RTT")

        try await client.close()
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
        let iter = sub.makeAsyncIterator()
        Task {
            if let msg = await iter.next() {
                XCTAssertEqual(msg.subject, "test")
                expectation.fulfill()
            }
        }
        await fulfillment(of: [expectation], timeout: 5.0)
        await sub.complete()
        try await client.close()
    }

    func testConnectMultipleURLsOneIsValid() async throws {
        natsServer.start()
        logger.logLevel = .debug
        let client = ClientOptions()
            .urls([
                URL(string: natsServer.clientURL)!, URL(string: "nats://localhost:4344")!,
                URL(string: "nats://localhost:4343")!,
            ])
            .build()
        try await client.connect()
        let sub = try await client.subscribe(to: "test")

        try client.publish("msg".data(using: .utf8)!, subject: "test")
        let expectation = XCTestExpectation(description: "Should receive message in 5 seconsd")
        let iter = sub.makeAsyncIterator()
        Task {
            if let msg = await iter.next() {
                XCTAssertEqual(msg.subject, "test")
                expectation.fulfill()
            }
        }
        await fulfillment(of: [expectation], timeout: 5.0)
        await sub.complete()
        try await client.close()
    }

    func testConnectMultipleURLsRetainOrder() async throws {
        natsServer.start()
        let natsServer2 = NatsServer()
        natsServer2.start()
        logger.logLevel = .debug
        for _ in 0..<10 {
            let client = ClientOptions()
                .urls([URL(string: natsServer2.clientURL)!, URL(string: natsServer.clientURL)!])
                .retainServersOrder()
                .build()
            try await client.connect()
            XCTAssertEqual(client.connectedUrl, URL(string: natsServer2.clientURL))
            try await client.close()
        }
    }

    func testRetryOnFailedConnect() async throws {
        let client = ClientOptions()
            .url(URL(string: "nats://localhost:4321")!)
            .reconnectWait(1)
            .retryOnfailedConnect()
            .build()

        let expectation = XCTestExpectation(
            description: "client was not notified of connection established event")
        client.on(.connected) { event in
            expectation.fulfill()
        }

        try await client.connect()
        natsServer.start(port: 4321)

        await fulfillment(of: [expectation], timeout: 5.0)

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
        let iter = sub.makeAsyncIterator()
        Task {
            if let msg = await iter.next() {
                XCTAssertEqual(msg.subject, "test")
                XCTAssertEqual(msg.replySubject, "reply")
                expectation.fulfill()
            }
        }
        await fulfillment(of: [expectation], timeout: 5.0)
        await sub.complete()
    }

    func testSubscribe() async throws {
        natsServer.start()
        logger.logLevel = .debug
        let client = ClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let sub = try await client.subscribe(to: "test")
        try client.publish("msg".data(using: .utf8)!, subject: "test")
        let iter = sub.makeAsyncIterator()
        let message = await iter.next()
        print("payload: \(String(data:message!.payload!, encoding: .utf8)!)")
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
        for await _ in sub {
            messagesReceived += 1
            if messagesReceived == 10 {
                break
            }
        }
        let expectation = XCTestExpectation(
            description: "client was not notified of connection established event")
        client.on(.connected) { event in
            expectation.fulfill()
        }

        // restart the server
        natsServer.stop()
        sleep(1)
        natsServer.start(port: port)
        await fulfillment(of: [expectation], timeout: 10.0)

        // publish more messages, sub should receive them
        Task {
            for _ in 0..<10 {
                try client.publish(payload, subject: "foo")
            }
        }

        for await _ in sub {
            messagesReceived += 1
            if messagesReceived == 20 {
                break
            }
        }

        // Check if the total number of messages received matches the number sent
        XCTAssertEqual(20, messagesReceived, "Mismatch in the number of messages sent and received")
        try await client.close()
    }

    func testUsernameAndPassword() async throws {
        logger.logLevel = .debug
        let currentFile = URL(fileURLWithPath: #file)
        // Navigate up to the Tests directory
        let testsDir = currentFile.deletingLastPathComponent().deletingLastPathComponent()
        // Construct the path to the resource
        let resourceURL =
            testsDir
            .appendingPathComponent("Integration/Resources/creds.conf", isDirectory: false)
        natsServer.start(cfg: resourceURL.path)
        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .usernameAndPassword("derek", "s3cr3t")
            .maxReconnects(5)
            .build()
        try await client.connect()
        try client.publish("msg".data(using: .utf8)!, subject: "test")
        try await client.flush()
        _ = try await client.subscribe(to: "test")
        XCTAssertNotNil(client, "Client should not be nil")

        // Test if client with bad credentials throws an error
        let badCertsClient = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .usernameAndPassword("derek", "badpassword")
            .maxReconnects(5)
            .build()

        do {
            try await badCertsClient.connect()
            XCTFail("Should have thrown an error")
        } catch {
            XCTAssertNotNil(error, "Error should not be nil")
        }

    }

    func testTokenAuth() async throws {
        logger.logLevel = .debug
        let currentFile = URL(fileURLWithPath: #file)
        // Navigate up to the Tests directory
        let testsDir = currentFile.deletingLastPathComponent().deletingLastPathComponent()
        // Construct the path to the resource
        let resourceURL =
            testsDir
            .appendingPathComponent("Integration/Resources/token.conf", isDirectory: false)
        natsServer.start(cfg: resourceURL.path)
        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .token("s3cr3t")
            .maxReconnects(5)
            .build()
        try await client.connect()
        try client.publish("msg".data(using: .utf8)!, subject: "test")
        try await client.flush()
        _ = try await client.subscribe(to: "test")
        XCTAssertNotNil(client, "Client should not be nil")

        // Test if client with bad credentials throws an error
        let badCertsClient = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .token("badtoken")
            .maxReconnects(5)
            .build()

        do {
            try await badCertsClient.connect()
            XCTFail("Should have thrown an error")
        } catch {
            XCTAssertNotNil(error, "Error should not be nil")
        }
    }

    func testCredentialsAuth() async throws {
        logger.logLevel = .debug
        let currentFile = URL(fileURLWithPath: #file)
        // Navigate up to the Tests directory
        let testsDir = currentFile.deletingLastPathComponent().deletingLastPathComponent()
        // Construct the path to the resource
        let resourceURL =
            testsDir
            .appendingPathComponent("Integration/Resources/jwt.conf", isDirectory: false)
        natsServer.start(cfg: resourceURL.path)
        print("server started with file: \(resourceURL.path)")

        let credsURL = testsDir.appendingPathComponent(
            "Integration/Resources/TestUser.creds", isDirectory: false)

        let client = ClientOptions().url(URL(string: natsServer.clientURL)!).credentialsFile(
            credsURL
        ).build()
        try await client.connect()
        let subscribe = try await client.subscribe(to: "foo").makeAsyncIterator()
        try client.publish("data".data(using: .utf8)!, subject: "foo")
        let message = await subscribe.next()
        print("message: \(message!.subject)")
    }

    func testMutualTls() async throws {
        logger.logLevel = .debug
        let currentFile = URL(fileURLWithPath: #file)
        // Navigate up to the Tests directory
        let testsDir = currentFile.deletingLastPathComponent().deletingLastPathComponent()
        // Construct the path to the resource
        let resourceURL =
            testsDir
            .appendingPathComponent("Integration/Resources/tls.conf", isDirectory: false)
        natsServer.start(cfg: resourceURL.path)
        let certsURL = testsDir.appendingPathComponent(
            "Integration/Resources/certs/rootCA.pem", isDirectory: false)
        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .requireTls()
            .rootCertificates(certsURL)
            .clientCertificate(
                testsDir.appendingPathComponent(
                    "Integration/Resources/certs/client-cert.pem", isDirectory: false),
                testsDir.appendingPathComponent(
                    "Integration/Resources/certs/client-key.pem", isDirectory: false)
            )
            .build()
        try await client.connect()
        try client.publish("msg".data(using: .utf8)!, subject: "test")
        try await client.flush()
        _ = try await client.subscribe(to: "test")
        XCTAssertNotNil(client, "Client should not be nil")
    }

    func testTlsFirst() async throws {
        logger.logLevel = .debug
        let currentFile = URL(fileURLWithPath: #file)
        // Navigate up to the Tests directory
        let testsDir = currentFile.deletingLastPathComponent().deletingLastPathComponent()
        // Construct the path to the resource
        let resourceURL =
            testsDir
            .appendingPathComponent("Integration/Resources/tls_first.conf", isDirectory: false)
        natsServer.start(cfg: resourceURL.path)
        let certsURL = testsDir.appendingPathComponent(
            "Integration/Resources/certs/rootCA.pem", isDirectory: false)
        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .requireTls()
            .rootCertificates(certsURL)
            .clientCertificate(
                testsDir.appendingPathComponent(
                    "Integration/Resources/certs/client-cert.pem", isDirectory: false),
                testsDir.appendingPathComponent(
                    "Integration/Resources/certs/client-key.pem", isDirectory: false)
            )
            .withTlsFirst()
            .build()
        try await client.connect()
        try client.publish("msg".data(using: .utf8)!, subject: "test")
        try await client.flush()
        _ = try await client.subscribe(to: "test")
        XCTAssertNotNil(client, "Client should not be nil")
    }

    func testInvalidCertificate() async throws {
        logger.logLevel = .debug
        let currentFile = URL(fileURLWithPath: #file)
        // Navigate up to the Tests directory
        let testsDir = currentFile.deletingLastPathComponent().deletingLastPathComponent()
        // Construct the path to the resource
        let resourceURL =
            testsDir
            .appendingPathComponent("Integration/Resources/tls.conf", isDirectory: false)
        natsServer.start(cfg: resourceURL.path)
        let certsURL = testsDir.appendingPathComponent(
            "Integration/Resources/certs/rootCA.pem", isDirectory: false)
        let client = ClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .requireTls()
            .rootCertificates(certsURL)
            .clientCertificate(
                testsDir.appendingPathComponent(
                    "Integration/Resources/certs/client-cert-invalid.pem", isDirectory: false),
                testsDir.appendingPathComponent(
                    "Integration/Resources/certs/client-key-invalid.pem", isDirectory: false)
            )
            .build()
        do {
            try await client.connect()
        } catch {
            return
        }
        XCTFail("Expected error from connect")
    }

     func testLameDuckMode() async throws {
        natsServer.start()
        logger.logLevel = .debug

        let client = ClientOptions().url(URL(string: natsServer.clientURL)!).build()

        let expectation = XCTestExpectation(
            description: "client was not notified of connection established event")
        client.on(.lameDuckMode) { event in
            XCTAssertEqual(event.kind(), NatsEventKind.lameDuckMode)
            expectation.fulfill()
        }
        try await client.connect()

        natsServer.setLameDuckMode()
        await fulfillment(of: [expectation], timeout: 1.0)
        try await client.close()
    }

    func testRequest() async throws {
        natsServer.start()
        logger.logLevel = .debug

        let client = ClientOptions().url(URL(string: natsServer.clientURL)!).build()

        Task {
        let service = try await client.subscribe(to: "service")
        for await message in service {
            try client.publish("reply".data(using: .utf8)!, subject: message.replySubject!)
        }

        let response = try await client.request("request".data(using: .utf8)!, to: "service")
        XCTAssertEqual(response.payload, "reply".data(using: .utf8)!)
        }
    }
}
