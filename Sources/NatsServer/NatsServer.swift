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

/// Runs a `nats-server` process for integration tests.
///
/// The `nats-server` binary must be available on the `PATH`. Each started server uses its own
/// temporary JetStream store directory, which is removed when the server is stopped.
///
/// ```swift
/// let server = NatsServer()
/// server.start(cfg: configPath)
/// defer { server.stop() }
/// let client = NatsClientOptions().url(URL(string: server.clientURL)!).build()
/// ```
public class NatsServer {
    /// The client port of the running server.
    public var port: Int? { return natsServerPort }
    /// The URL clients connect to, using `tls://` when TLS is configured.
    public var clientURL: String {
        let scheme = tlsEnabled ? "tls://" : "nats://"
        if let natsServerPort {
            return "\(scheme)localhost:\(natsServerPort)"
        } else {
            return ""
        }
    }

    /// The WebSocket URL clients connect to, when WebSocket is configured.
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
    private var storeDir: URL?

    /// Creates a server which is not running yet.
    public init() {}

    /// Starts the server and waits until it accepts connections.
    ///
    /// - Parameters:
    ///   - port: client port, `-1` picks a random free port.
    ///   - cfg: path of a server configuration file.
    ///   - file: source file reported by failed assertions.
    ///   - line: source line reported by failed assertions.
    public func start(
        port: Int = -1, cfg: String? = nil, file: StaticString = #filePath, line: UInt = #line
    ) {
        XCTAssertNil(
            self.process, "nats-server is already running on port \(port)", file: file, line: line)
        let process = Process()
        let pipe = Pipe()

        let fileManager = FileManager.default
        pidFile = fileManager.temporaryDirectory.appendingPathComponent("nats-server.pid")

        let tempDir = FileManager.default.temporaryDirectory.appending(component: UUID().uuidString)
        storeDir = tempDir

        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [
            "nats-server", "-p", "\(port)", "-P", pidFile!.path, "--store_dir",
            tempDir.path,
        ]
        if let cfg {
            process.arguments?.append(contentsOf: ["-c", cfg])
        }
        process.standardError = pipe
        process.standardOutput = pipe

        let outputHandle = pipe.fileHandleForReading
        let semaphore = DispatchSemaphore(value: 0)
        let startup = StartupOutput()

        outputHandle.readabilityHandler = { fileHandle in
            let data = fileHandle.availableData
            guard data.count > 0 else { return }
            if startup.process(data) {
                fileHandle.readabilityHandler = nil
                semaphore.signal()
            }
        }

        XCTAssertNoThrow(
            try process.run(), "error starting nats-server on port \(port)", file: file, line: line)

        let result = semaphore.wait(timeout: .now() + .seconds(10))

        XCTAssertFalse(
            result == .timedOut, "timeout waiting for server to be ready", file: file, line: line)
        let state = startup.state
        XCTAssertNil(
            state.serverError, "error starting nats-server: \(state.serverError ?? "")", file: file,
            line: line)

        natsServerPort = state.port
        natsWebsocketPort = state.websocketPort
        tlsEnabled = state.tlsEnabled
        self.process = process
    }

    /// Stops the server and removes its JetStream store directory.
    public func stop() {
        if process == nil {
            return
        }

        self.process?.terminate()
        if let process {
            Self.waitForExit(process)
        }
        process = nil
        natsServerPort = port
        tlsEnabled = false
        if let storeDir {
            try? FileManager.default.removeItem(at: storeDir)
            self.storeDir = nil
        }
    }

    /// Sends a signal to the running server, e.g. to enter lame duck mode.
    public func sendSignal(_ signal: Signal, file: StaticString = #filePath, line: UInt = #line) {
        let process = Process()

        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["nats-server", "--signal", "\(signal.rawValue)=\(self.pidFile!.path)"]

        XCTAssertNoThrow(
            try process.run(), "error setting signal", file: file, line: line)
        self.process = nil
    }

    /// Waits until the process exits (at most 10 seconds).
    /// Unlike `Process.waitUntilExit()`, this does not rely on the run loop of the current thread,
    /// which is not running on threads of the Swift concurrency pool.
    private static func waitForExit(_ process: Process) {
        let deadline = Date().addingTimeInterval(10)
        while process.isRunning && Date() < deadline {
            usleep(10_000)
        }
    }

    fileprivate static func extractPort(from string: String, for phrase: String) -> Int? {
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

    fileprivate static func extracErrorMessage(from logLine: String) -> String? {
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

    fileprivate static func isTLS(from logLine: String) -> Bool {
        return logLine.contains("TLS required for client connections")
            || logLine.contains("websocket clients on wss://")
    }

    deinit {
        stop()
    }

    /// Signals which can be sent to the server.
    public enum Signal: String {
        /// Puts the server in lame duck mode, asking clients to move to other servers.
        case lameDuckMode = "ldm"
        /// Reloads the server configuration.
        case reload = "reload"
    }
}

/// Parses the startup output of a server, which is read on a background queue.
private final class StartupOutput: @unchecked Sendable {
    struct State {
        var port: Int?
        var websocketPort: Int?
        var tlsEnabled = false
        var serverError: String?
    }

    private let lock = NSLock()
    private var buffer = Data()
    private var lineCount = 0
    private var current = State()
    private static let maxLines = 100

    var state: State {
        lock.lock()
        defer { lock.unlock() }
        return current
    }

    /// Processes output, returning true once the server is ready or failed to start.
    func process(_ data: Data) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        buffer.append(data)
        guard let output = String(data: buffer, encoding: .utf8) else { return false }

        let lines = output.split(separator: "\n", omittingEmptySubsequences: false)
        for lineSequence in lines.dropLast() {
            let line = String(lineSequence)
            lineCount += 1

            let errorLine = NatsServer.extracErrorMessage(from: line)
            if let port = NatsServer.extractPort(from: line, for: "client connections") {
                current.port = port
            }
            if let port = NatsServer.extractPort(from: line, for: "websocket clients") {
                current.websocketPort = port
            }
            if NatsServer.isTLS(from: line) {
                current.tlsEnabled = true
            }
            if line.contains("Server is ready") || errorLine != nil
                || lineCount >= Self.maxLines
            {
                current.serverError = errorLine
                return true
            }
        }

        if output.hasSuffix("\n") {
            buffer.removeAll()
        } else if let lastLine = lines.last {
            buffer = Data(lastLine.utf8)
        }
        return false
    }
}
