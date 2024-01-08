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
}
