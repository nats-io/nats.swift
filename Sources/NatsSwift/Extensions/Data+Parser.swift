//
//  Data+Parser.swift
//  NatsSwift
//

import Foundation

extension Data {
    private static let crlf = Data([UInt8(ascii: "\r"), UInt8(ascii: "\n")])
    
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
    
    func parseOutMessages() -> [ServerOp] {
        var serverOps = [ServerOp]()
        var startIndex = self.startIndex
        
        while startIndex < self.endIndex, let range = self[startIndex...].range(of: Data.crlf) {
            let lineEndIndex = range.lowerBound
            let nextLineStartIndex = self.index(range.upperBound, offsetBy: 0, limitedBy: self.endIndex) ?? self.endIndex
            
            let lineData = self[startIndex..<lineEndIndex]
            var serverOp: ServerOp
            do {
                serverOp = try ServerOp.parse(from: lineData)
            } catch {
                // TODO(pp): handle this error properly (maybe surface in throw)
                logger.error("Error parsing message: \(error)")
                return serverOps
            }
            
            // if it's a message, get the next line as well and include as payload
            if case .Message(var msg) = serverOp {
                if msg.length == 0 {
                    serverOps.append(serverOp)
                } else if nextLineStartIndex < self.endIndex, let nextLineRange = self[nextLineStartIndex...].range(of: Data.crlf) {
                    let nextLineEndIndex = nextLineRange.lowerBound
                    msg.payload = self[nextLineStartIndex..<nextLineEndIndex]
                    serverOps.append(.Message(msg))
                    startIndex = self.index(nextLineRange.upperBound, offsetBy: 0, limitedBy: self.endIndex) ?? self.endIndex
                    continue
                }
                
            } else {
                // otherwise, just add this server op to the result
                serverOps.append(serverOp)
            }
            
            // Move to the start of the next line
            startIndex = nextLineStartIndex
        }
        
        return serverOps
    }
}
