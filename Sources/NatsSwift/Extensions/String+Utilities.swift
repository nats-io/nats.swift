//
//  String+Utilities.swift
//  NatsSwift
//

import Foundation

extension String {

    static func hash() -> String {
        let uuid = String.uuid()
        return uuid[0...7]
    }

    static func uuid() -> String {
        return UUID().uuidString.trimmingCharacters(in: .punctuationCharacters)
    }

    subscript (bounds: CountableClosedRange<Int>) -> String {
        let start = index(startIndex, offsetBy: bounds.lowerBound)
        let end = index(startIndex, offsetBy: bounds.upperBound)
        return String(self[start...end])
    }

    func removeNewlines() -> String {
        return self.components(separatedBy: CharacterSet.newlines).reduce("", {$0 + $1})
    }

    func removePrefix(_ prefix: String) -> String {
        let index = self.index(self.startIndex, offsetBy: prefix.count)
        return String(self[index...])
    }

    func toJsonDicitonary() -> [String: AnyObject]? {

        guard let data = self.data(using: String.Encoding.utf8) else { return nil }

        guard let obj = try? JSONSerialization.jsonObject(with: data, options: []) else { return nil }

        return obj as? [String: AnyObject]
    }

    func getMessageType() -> NatsOperation? {

        guard self.count > 2 else { return nil }

        let isOperation: ((NatsOperation) -> Bool) = { no in
            let l = no.rawValue.count - 1
            guard self.count > l else { return false }
            let operation = String(self[0...l]).uppercased()
            guard operation == no.rawValue else { return false }
            return true
        }

        let firstCharacter = String(self[0...0]).uppercased()

        switch firstCharacter {
        case "C":
            guard isOperation(.connect) else { return nil }
            return .connect
        case "S":
            guard isOperation(.subscribe) else { return nil }
            return .subscribe
        case "U":
            guard isOperation(.unsubscribe) else { return nil }
            return .unsubscribe
        case "M":
            guard isOperation(.message) else { return nil }
            return .message
        case "I":
            guard isOperation(.info) else { return nil }
            return .info
        case "+":
            guard isOperation(.ok) else { return nil }
            return .ok
        case "-":
            guard isOperation(.error) else { return nil }
            return .error
        case "P":
            if isOperation(.ping) { return .ping }
            if isOperation(.pong) { return .pong }
            if isOperation(.publish) { return .publish }
            return nil
        default:
            return nil
        }

    }

    func parseOutMessages() -> [String] {

        var messages = [String]()
        let lines = self.components(separatedBy: "\n")
        var isMessageFlag = false
        var lastLine = ""

        for line in lines {

            if isMessageFlag {
                messages.append(lastLine + line)
                isMessageFlag = false
                continue
            }

            lastLine = line
            let type = line.getMessageType()
            if type == nil { continue }

            switch (type!) {
            case .message:
                isMessageFlag = true
                break
            default:
                messages.append(line)
            }

        }

        return messages
    }

}

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
