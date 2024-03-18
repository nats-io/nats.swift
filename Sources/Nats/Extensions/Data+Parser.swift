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

extension Data {
    private static let cr = UInt8(ascii: "\r")
    private static let lf = UInt8(ascii: "\n")
    private static let crlf = Data([cr, lf])
    private static var currentNum = 0
    private static var errored = false
    private static let versionLinePrefix = "NATS/1.0"

    func removePrefix(_ prefix: Data) -> Data {
        guard self.starts(with: prefix) else { return self }
        return self.dropFirst(prefix.count)
    }

    func split(
        separator: Data, maxSplits: Int = .max, omittingEmptySubsequences: Bool = true
    )
        -> [Data]
    {
        var chunks: [Data] = []
        var start = startIndex
        var end = startIndex
        var splitsCount = 0

        while end < count {
            if splitsCount >= maxSplits {
                break
            }
            if self[start..<end].elementsEqual(separator) {
                if !omittingEmptySubsequences || start != end {
                    chunks.append(self[start..<end])
                }
                start = index(end, offsetBy: separator.count)
                end = start
                splitsCount += 1
                continue
            }
            end = index(after: end)
        }

        if start <= endIndex {
            if !omittingEmptySubsequences || start != endIndex {
                chunks.append(self[start..<endIndex])
            }
        }

        return chunks
    }

    func getMessageType() -> NatsOperation? {
        guard self.count > 2 else { return nil }
        for operation in NatsOperation.allOperations() {
            if self.starts(with: operation.rawBytes) {
                return operation
            }
        }
        return nil
    }

    func starts(with bytes: [UInt8]) -> Bool {
        guard self.count >= bytes.count else { return false }
        return self.prefix(bytes.count).elementsEqual(bytes)
    }

    internal mutating func prepend(_ other: Data) {
        self = other + self
    }

    internal func parseOutMessages() throws -> (ops: [ServerOp], remainder: Data?) {
        var serverOps = [ServerOp]()
        var startIndex = self.startIndex
        var remainder: Data?

        while startIndex < self.endIndex {
            var nextLineStartIndex: Int
            var lineData: Data
            if let range = self[startIndex...].range(of: Data.crlf) {
                let lineEndIndex = range.lowerBound
                nextLineStartIndex =
                    self.index(range.upperBound, offsetBy: 0, limitedBy: self.endIndex)
                    ?? self.endIndex
                lineData = self[startIndex..<lineEndIndex]
            } else {
                remainder = self[startIndex..<self.endIndex]
                break
            }
            if lineData.count == 0 {
                startIndex = nextLineStartIndex
                continue
            }

            let serverOp = try ServerOp.parse(from: lineData)

            // if it's a message, get the full payload and add to returned data
            if case .message(var msg) = serverOp {
                if msg.length == 0 {
                    serverOps.append(serverOp)
                } else {
                    var payload = Data()
                    let payloadEndIndex = nextLineStartIndex + msg.length
                    let payloadStartIndex = nextLineStartIndex
                    // include crlf in the expected payload leangth
                    if payloadEndIndex + Data.crlf.count > endIndex {
                        remainder = self[startIndex..<self.endIndex]
                        break
                    }
                    payload.append(self[payloadStartIndex..<payloadEndIndex])
                    msg.payload = payload
                    startIndex =
                        self.index(
                            payloadEndIndex, offsetBy: Data.crlf.count, limitedBy: self.endIndex)
                        ?? self.endIndex
                    serverOps.append(.message(msg))
                    continue
                }
                //TODO(jrm): Add HMSG handling here too.
            } else if case .hMessage(var msg) = serverOp {
                if msg.length == 0 {
                    serverOps.append(serverOp)
                } else {
                    let headersStartIndex = nextLineStartIndex
                    let headersEndIndex = nextLineStartIndex + msg.headersLength
                    let payloadStartIndex = headersEndIndex
                    let payloadEndIndex = nextLineStartIndex + msg.length

                    var payload: Data?
                    if msg.length > msg.headersLength {
                        payload = Data()
                    }
                    var headers = NatsHeaderMap()

                    // if the whole msg length (including training crlf) is longer
                    // than the remaining chunk, break and return the remainder
                    if payloadEndIndex + Data.crlf.count > endIndex {
                        remainder = self[startIndex..<self.endIndex]
                        break
                    }

                    let headersData = self[headersStartIndex..<headersEndIndex]
                    if let headersString = String(data: headersData, encoding: .utf8) {
                        let headersArray = headersString.split(separator: "\r\n")
                        let versionLine = headersArray[0]
                        guard versionLine.hasPrefix(Data.versionLinePrefix) else {
                            throw NatsParserError(
                                "header version line does not begin with `NATS/1.0`")
                        }
                        let versionLineSuffix =
                            versionLine
                            .dropFirst(Data.versionLinePrefix.count)
                            .trimmingCharacters(in: .whitespacesAndNewlines)

                        // handle inlines status and description
                        if versionLineSuffix.count > 0 {
                            let statusAndDesc = versionLineSuffix.split(
                                separator: " ", maxSplits: 1)
                            guard let status = StatusCode(statusAndDesc[0]) else {
                                throw NatsParserError("could not parse status parameter")
                            }
                            msg.status = status
                            if statusAndDesc.count > 1 {
                                msg.description = String(statusAndDesc[1])
                            }
                        }

                        for header in headersArray.dropFirst() {
                            let headerParts = header.split(separator: ":")
                            if headerParts.count == 2 {
                                headers.append(
                                    try NatsHeaderName(String(headerParts[0])),
                                    NatsHeaderValue(String(headerParts[1])))
                            } else {
                                logger.error("Error parsing header: \(header)")
                            }
                        }
                    }
                    msg.headers = headers

                    if var payload = payload {
                        payload.append(self[payloadStartIndex..<payloadEndIndex])
                        msg.payload = payload
                    }

                    startIndex =
                        self.index(
                            payloadEndIndex, offsetBy: Data.crlf.count, limitedBy: self.endIndex)
                        ?? self.endIndex
                    serverOps.append(.hMessage(msg))
                    continue
                }

            } else {
                // otherwise, just add this server op to the result
                serverOps.append(serverOp)
            }
            startIndex = nextLineStartIndex

        }

        return (serverOps, remainder)
    }
}
