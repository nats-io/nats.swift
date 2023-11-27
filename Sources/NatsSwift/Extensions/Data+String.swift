//
//  Data+String.swift
//  NatsSwift
//

import Foundation
import NIOPosix

extension Data {
    func toString() -> String? {
        if let str = String(data: self, encoding: .utf8) {
            return str
        }
        return nil
    }
}
