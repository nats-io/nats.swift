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

public class NatsServer {
    public var port: Int? { return natsServerPort }
    public var clientURL: String {
        let scheme = tlsEnabled ? "tls://" : "nats://"
        if let natsServerPort {
            return "\(scheme)localhost:\(natsServerPort)"
        } else {
            return ""
        }
    }

    public var clientWebsocketURL: String {
        let scheme = tlsEnabled ? "wss://" : "ws://"
        if let natsWebsocketPort {
            return "\(scheme)localhost:\(natsWebsocketPort)"
        } else {
            return ""
        }
    }

    private var process: Process?
    private var natsServerPort: Int?
    private var natsWebsocketPort: Int?
    private var tlsEnabled = false
    private var pidFile: URL?

    public init() {}

    // TODO: When implementing JetStream, creating and deleting store dir should be handled in start/stop methods
    public func start(
        port: Int = -1, cfg: String? = nil, file: StaticString = #file, line: UInt = #line
    ) {
        XCTAssertNil(
            self.process, "nats-server is already running on port \(port)", file: file, line: line)
        let process = Process()
        let pipe = Pipe()

        let fileManager = FileManager.default
        pidFile = fileManager.temporaryDirectory.appendingPathComponent("nats-server.pid")

        let tempDir = FileManager.default.temporaryDirectory.appending(component: UUID().uuidString)

        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [
            "nats-server", "-p", "\(port)", "-P", pidFile!.path, "--store_dir",
            "\(tempDir.absoluteString)",
        ]
        if let cfg {
            process.arguments?.append(contentsOf: ["-c", cfg])
        }
        process.standardError = pipe
        process.standardOutput = pipe

        let outputHandle = pipe.fileHandleForReading
        let semaphore = DispatchSemaphore(value: 0)
        var lineCount = 0
        let maxLines = 100
        var serverError: String?
        var outputBuffer = Data()

        outputHandle.readabilityHandler = { fileHandle in
            let data = fileHandle.availableData
            guard data.count > 0 else { return }
            outputBuffer.append(data)

            guard let output = String(data: outputBuffer, encoding: .utf8) else { return }

            let lines = output.split(separator: "\n", omittingEmptySubsequences: false)
            let completedLines = lines.dropLast()

            for lineSequence in completedLines {
                let line = String(lineSequence)
                lineCount += 1

                let errorLine = self.extracErrorMessage(from: line)

                if let port = self.extractPort(from: line, for: "client connections") {
                    self.natsServerPort = port
                }

                if let port = self.extractPort(from: line, for: "websocket clients") {
                    self.natsWebsocketPort = port
                }

                let ready = line.contains("Server is ready")

                if !self.tlsEnabled && self.isTLS(from: line) {
                    self.tlsEnabled = true
                }

                if ready || errorLine != nil || lineCount >= maxLines {
                    serverError = errorLine
                    semaphore.signal()
                    outputHandle.readabilityHandler = nil
                    return
                }
            }

            if output.hasSuffix("\n") {
                outputBuffer.removeAll()
            } else {
                if let lastLine = lines.last, let incompleteLine = lastLine.data(using: .utf8) {
                    outputBuffer = incompleteLine
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
    }

    public func stop() {
        if process == nil {
            return
        }

        self.process?.terminate()
        process?.waitUntilExit()
        process = nil
        natsServerPort = port
        tlsEnabled = false
    }

    public func setLameDuckMode(file: StaticString = #file, line: UInt = #line) {
        let process = Process()

        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["nats-server", "--signal", "ldm=\(self.pidFile!.path)"]

        XCTAssertNoThrow(
            try process.run(), "error setting lame duck mode", file: file, line: line)
        self.process = nil
    }

    private func extractPort(from string: String, for phrase: String) -> Int? {
        // Listening for websocket clients on
        // Listening for client connections on
        let pattern = "Listening for \(phrase) on .*?:(\\d+)$"

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
            || logLine.contains("websocket clients on wss://")
    }

    deinit {
        stop()
    }
}
