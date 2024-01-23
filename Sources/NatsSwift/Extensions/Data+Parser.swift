//
//  Data+Parser.swift
//  NatsSwift
//

import Foundation

extension Data {
    private static let cr = UInt8(ascii: "\r")
    private static let lf = UInt8(ascii: "\n")
    private static let crlf = Data([cr, lf])
    private static var currentNum = 0
    private static var errored = false

    func removePrefix(_ prefix: Data) -> Data {
        guard self.starts(with: prefix) else { return self }
        return self.dropFirst(prefix.count)
    }

    func split(separator: Data, maxSplits: Int = .max, omittingEmptySubsequences: Bool = true) -> [Data] {
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
                nextLineStartIndex = self.index(range.upperBound, offsetBy: 0, limitedBy: self.endIndex) ?? self.endIndex
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
            if case .Message(var msg) = serverOp {
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
                    startIndex = self.index(payloadEndIndex, offsetBy: Data.crlf.count, limitedBy: self.endIndex) ?? self.endIndex
                    serverOps.append(.Message(msg))
                    continue
                }
            //TODO(jrm): Add HMSG handling here too.
            } else if case .HMessage(var msg) = serverOp  {
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
                    var headers = HeaderMap()
                    
                    // if the whole msg length (including training crlf) is longer
                    // than the remaining chunk, break and return the remainder
                    if payloadEndIndex + Data.crlf.count > endIndex {
                        remainder = self[startIndex..<self.endIndex]
                        break
                    }

                   let headersData = self[headersStartIndex..<headersEndIndex]
                    if let headersString = String(data: headersData, encoding: .utf8) {
                        let headersArray = headersString.split(separator: "\r\n")
                        // TODO: unused now, but probably we should validate?
                        let versionLine = headersArray[0]

                        for header in headersArray.dropFirst() {
                            let headerParts = header.split(separator: ":")
                            if headerParts.count == 2 {
                                headers.append(try! HeaderName(String(headerParts[0])), HeaderValue(String(headerParts[1])))
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
                    
                    startIndex = self.index(payloadEndIndex, offsetBy: Data.crlf.count, limitedBy: self.endIndex) ?? self.endIndex
                    serverOps.append(.HMessage(msg))
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
