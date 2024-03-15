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

import Foundation
import XCTest

class NatsServer {
    var port: Int? { return natsServerPort }
    var clientURL: String {
        let scheme = tlsEnabled ? "tls://" : "nats://"
        if let natsServerPort {
            return "\(scheme)localhost:\(natsServerPort)"
        } else {
            return ""
        }
    }

    private var process: Process?
    private var natsServerPort: Int?
    private var tlsEnabled = false
    private var pidFile: URL?

    // TODO: When implementing JetStream, creating and deleting store dir should be handled in start/stop methods
    func start(port: Int = -1, cfg: String? = nil, file: StaticString = #file, line: UInt = #line) {
        XCTAssertNil(
            self.process, "nats-server is already running on port \(port)", file: file, line: line)
        let process = Process()
        let pipe = Pipe()

        let fileManager = FileManager.default
        pidFile = fileManager.temporaryDirectory.appendingPathComponent("nats-server.pid")

        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["nats-server", "-p", "\(port)", "-P", pidFile!.path]
        if let cfg {
            process.arguments?.append(contentsOf: ["-c", cfg])
        }
        process.standardError = pipe
        process.standardOutput = pipe

        let outputHandle = pipe.fileHandleForReading
        let semaphore = DispatchSemaphore(value: 0)
        var lineCount = 0
        let maxLines = 100
        var serverPort: Int?
        var serverError: String?

        outputHandle.readabilityHandler = { fileHandle in
            let data = fileHandle.availableData
            if let line = String(data: data, encoding: .utf8) {
                lineCount += 1

                serverError = self.extracErrorMessage(from: line)
                serverPort = self.extractPort(from: line)
                if !self.tlsEnabled && self.isTLS(from: line) {
                    self.tlsEnabled = true
                }
                if serverPort != nil || serverError != nil || lineCount >= maxLines {
                    serverError = serverError
                    semaphore.signal()
                    outputHandle.readabilityHandler = nil
                }
            }
        }

        XCTAssertNoThrow(
            try process.run(), "error starting nats-server on port \(port)", file: file, line: line)

        let result = semaphore.wait(timeout: .now() + .seconds(10))

        XCTAssertFalse(
            result == .timedOut, "timeout waiting for server to be ready", file: file, line: line)
        XCTAssertNil(
            serverError, "error starting nats-server: \(serverError!)", file: file, line: line)

        self.process = process
        self.natsServerPort = serverPort
    }

    func stop() {
        if process == nil {
            return
        }

        self.process?.terminate()
        process?.waitUntilExit()
        process = nil
        natsServerPort = port
        tlsEnabled = false
    }

    func setLameDuckMode(file: StaticString = #file, line: UInt = #line) {
        let process = Process()

        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["nats-server", "--signal", "ldm=\(self.pidFile!.path)"]

        XCTAssertNoThrow(
            try process.run(), "error setting lame duck mode", file: file, line: line)
        self.process = nil
    }

    private func extractPort(from string: String) -> Int? {
        let pattern = "Listening for client connections on [^:]+:(\\d+)"

        let regex = try! NSRegularExpression(pattern: pattern)
        let nsrange = NSRange(string.startIndex..<string.endIndex, in: string)

        if let match = regex.firstMatch(in: string, options: [], range: nsrange) {
            let portRange = match.range(at: 1)
            if let swiftRange = Range(portRange, in: string) {
                let portString = String(string[swiftRange])
                return Int(portString)
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

    private func isTLS(from logLine: String) -> Bool {
        return logLine.contains("TLS required for client connections")
    }

    deinit {
        stop()
    }
}