//
//  Data+Parser.swift
//  NatsSwift
//

import Foundation

extension Data {
    func removePrefix(_ prefix: Data) -> Data {
        guard self.starts(with: prefix) else { return self }
        return self.dropFirst(prefix.count)
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
    
    func parseOutMessages() -> [Data] {
        var messages = [Data]()
        let crlf = Data([UInt8(ascii: "\r"), UInt8(ascii: "\n")])
        var startIndex = self.startIndex
        
        while startIndex < self.endIndex, let range = self[startIndex...].range(of: crlf) {
            let lineEndIndex = range.lowerBound
            let nextLineStartIndex = self.index(range.upperBound, offsetBy: 0, limitedBy: self.endIndex) ?? self.endIndex
            
            let lineData = self[startIndex..<lineEndIndex]
            
            if let messageType = lineData.getMessageType() {
                if messageType == .message {
                    // For .message type, include this line and the following line
                    if nextLineStartIndex < self.endIndex, let nextLineRange = self[nextLineStartIndex...].range(of: crlf) {
                        let nextLineEndIndex = nextLineRange.lowerBound
                        let messageData = self[startIndex..<nextLineEndIndex]
                        messages.append(messageData)
                        startIndex = self.index(nextLineRange.upperBound, offsetBy: 0, limitedBy: self.endIndex) ?? self.endIndex
                        continue
                    }
                } else {
                    // For other types, include only this line
                    messages.append(lineData)
                }
            }
            
            // Move to the start of the next line
            startIndex = nextLineStartIndex
        }
        
        return messages
    }
}
