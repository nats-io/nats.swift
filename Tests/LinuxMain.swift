//
//  LinuxMain.swift
//  NatsSwift
//

import XCTest

@testable import NatsSwiftTests

XCTMain([
    testCase(StringExtensionTests.allTests),
    testCase(NatsMessageTests.allTests),
])
