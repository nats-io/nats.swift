// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import XCTest
@testable import Nats
import NatsServer

class TestConcurrency: XCTestCase {
    
    func testRapidMessagesNoCrash() async throws {
        let nats = NatsServer()
        nats.start()
        guard let port = nats.port else {
            XCTFail("Failed to start server")
            return
        }
        
        // Create client
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(port)")!)
            .build()
        
        try await client.connect()
        
        // Subscribe to a subject
        let subscription = try await client.subscribe(subject: "test.>")
        
        // Task to receive messages
        let receiveTask = Task {
            var count = 0
            for try await _ in subscription {
                count += 1
                if count >= 100 {
                    break
                }
            }
            return count
        }
        
        // Rapidly publish messages from multiple tasks
        await withTaskGroup(of: Void.self) { group in
            for i in 0..<10 {
                group.addTask {
                    for j in 0..<10 {
                        let data = "Message \(i)-\(j)".data(using: .utf8)!
                        try? await client.publish(data, subject: "test.msg.\(i).\(j)")
                    }
                }
            }
        }
        
        // Wait for messages to be received
        let receivedCount = try await receiveTask.value
        XCTAssertEqual(receivedCount, 100)
        
        try await client.close()
        nats.stop()
    }
    
    func testReconnectDuringMessages() async throws {
        let nats = NatsServer()
        nats.start()
        guard let port = nats.port else {
            XCTFail("Failed to start server")
            return
        }
        
        // Create client with short reconnect wait
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:\(port)")!)
            .reconnectWait(0.1)
            .maxReconnects(5)
            .build()
        
        try await client.connect()
        
        // Subscribe to a subject
        let subscription = try await client.subscribe(subject: "test.>")
        
        // Task to receive messages
        let receiveTask = Task {
            var count = 0
            for try await _ in subscription {
                count += 1
                if count >= 50 {
                    break
                }
            }
            return count
        }
        
        // Publish messages while simulating disconnects
        for i in 0..<50 {
            let data = "Message \(i)".data(using: .utf8)!
            try? await client.publish(data, subject: "test.msg.\(i)")
            
            // Simulate network issues on some iterations
            if i == 10 || i == 30 {
                // Force a reconnect by stopping and restarting server
                nats.stop()
                try await Task.sleep(nanoseconds: 100_000_000) // 0.1 seconds
                nats.start(port: port)
                try await Task.sleep(nanoseconds: 200_000_000) // 0.2 seconds for reconnect
            }
        }
        
        // Give some time for remaining messages
        try await Task.sleep(nanoseconds: 500_000_000) // 0.5 seconds
        
        // Cancel the receive task if it's still running
        receiveTask.cancel()
        let receivedCount = (try? await receiveTask.value) ?? 0
        
        // We may not receive all 50 due to reconnects, but should receive some
        XCTAssertGreaterThan(receivedCount, 0)
        
        try await client.close()
        nats.stop()
    }
}