//
//  NatsServer.swift
//  NatsSwift
//

import Foundation
import XCTest

class NatsServer {
    var port: UInt? { return natsServerPort }
    var clientURL: String {
        if let natsServerPort {
            return "nats://localhost:\(natsServerPort)"
        } else {
            return ""
        }
    }
    
    private var process: Process?
    private var natsServerPort: UInt?
    
    // TODO: When implementing JetStream, creating and deleting store dir should be handled in start/stop methods
    func start(port: Int = -1, cfg: String? = nil, file: StaticString = #file, line: UInt = #line) {
        XCTAssertNil(self.process, "nats-server is already running on port \(port)", file: file, line: line)
        let process = Process()
        let pipe = Pipe()
                
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["nats-server", "-p", "\(port)"]
        if let cfg {
            process.arguments?.append(contentsOf: ["-c", cfg])
        }
        process.standardError = pipe
        process.standardOutput = pipe
        
        let outputHandle = pipe.fileHandleForReading
        let semaphore = DispatchSemaphore(value: 0)
        var lineCount = 0
        let maxLines = 100
        var serverPort: UInt?
        var serverError: String?
        
        outputHandle.readabilityHandler = { fileHandle in
            let data = fileHandle.availableData
            if let line = String(data: data, encoding: .utf8) {
                lineCount += 1

                serverError = self.extracErrorMessage(from: line)
                serverPort = self.extractPort(from: line)
                if serverPort != nil || serverError != nil || lineCount >= maxLines {
                    serverError = serverError
                    semaphore.signal()
                    outputHandle.readabilityHandler = nil
                }
            }
        }
        
        XCTAssertNoThrow(try process.run(), "error starting nats-server on port \(port)", file: file, line: line)
        
        let result = semaphore.wait(timeout: .now() + .seconds(10))
        
        XCTAssertFalse(result == .timedOut, "timeout waiting for server to be ready", file: file, line: line)
        XCTAssertNil(serverError, "error starting nats-server: \(serverError!)", file: file, line: line)
        
        self.process = process
        self.natsServerPort = serverPort
    }
    
    func stop(file: StaticString = #file, line: UInt = #line) {
        XCTAssertNotNil(self.process, "nats-server is not running", file: file, line: line)
        
        self.process?.terminate()
        process?.waitUntilExit()
        process = nil
        natsServerPort = port
    }
    
    private func extractPort(from string: String) -> UInt? {
        let pattern = "Listening for client connections on [^:]+:(\\d+)"
        
        let regex = try! NSRegularExpression(pattern: pattern)
        let nsrange = NSRange(string.startIndex..<string.endIndex, in: string)
        
        if let match = regex.firstMatch(in: string, options: [], range: nsrange) {
            let portRange = match.range(at: 1)
            if let swiftRange = Range(portRange, in: string) {
                let portString = String(string[swiftRange])
                return UInt(portString)
            }
        }
        
        return nil
    }
    
    private func extracErrorMessage(from logLine: String) -> String? {
        if logLine.contains("nats-server: No such file or directory") {
            return "nats-server not found - make sure nats-server can be found in PATH"
        }
        guard let range = logLine.range(of: "[FTL]") else {
            return nil
        }

        let messageStartIndex = range.upperBound
        let message = logLine[messageStartIndex...]

        return String(message).trimmingCharacters(in: .whitespaces)
    }
    
    deinit{
        stop()
    }
}
