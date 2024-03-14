//
//  LinuxMain.swift
//  Nats
//

import XCTest

@testable import NatsTests

XCTMain([
    testCase(StringExtensionTests.allTests),
    testCase(NatsMessageTests.allTests),
])
