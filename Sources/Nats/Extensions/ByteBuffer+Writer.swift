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
import NIO

extension ByteBuffer {
    mutating func writeClientOp(_ op: ClientOp) {
        switch op {
        case .publish((let subject, let reply, let payload, let headers)):
            if let payload = payload {
                self.reserveCapacity(
                    minimumWritableBytes: payload.count + subject.utf8.count
                        + NatsOperation.publish.rawValue.count + 12)
                if headers != nil {
                    self.writeBytes(NatsOperation.hpublish.rawBytes)
                } else {
                    self.writeBytes(NatsOperation.publish.rawBytes)
                }
                self.writeString(" ")
                self.writeString(subject)
                self.writeString(" ")
                if let reply = reply {
                    self.writeString("\(reply) ")
                }
                if let headers = headers {
                    let headers = headers.toBytes()
                    let totalLen = headers.count + payload.count
                    let headersLen = headers.count
                    self.writeString("\(headersLen) \(totalLen)\r\n")
                    self.writeData(headers)
                } else {
                    self.writeString("\(payload.count)\r\n")
                }
                self.writeData(payload)
                self.writeString("\r\n")
            } else {
                self.reserveCapacity(
                    minimumWritableBytes: subject.utf8.count + NatsOperation.publish.rawValue.count
                        + 12)
                self.writeBytes(NatsOperation.publish.rawBytes)
                self.writeString(" ")
                self.writeString(subject)
                if let reply = reply {
                    self.writeString("\(reply) ")
                }
                self.writeString("\r\n")
            }

        case .subscribe((let sid, let subject, let queue)):
            if let queue {
                self.writeString(
                    "\(NatsOperation.subscribe.rawValue) \(subject) \(queue) \(sid)\r\n")
            } else {
                self.writeString("\(NatsOperation.subscribe.rawValue) \(subject) \(sid)\r\n")
            }

        case .unsubscribe((let sid, let max)):
            if let max {
                self.writeString("\(NatsOperation.unsubscribe.rawValue) \(sid) \(max)\r\n")
            } else {
                self.writeString("\(NatsOperation.unsubscribe.rawValue) \(sid)\r\n")
            }
        case .connect(let info):
            // This encode can't actually fail
            let json = try! JSONEncoder().encode(info)
            self.reserveCapacity(minimumWritableBytes: json.count + 5)
            self.writeString("\(NatsOperation.connect.rawValue) ")
            self.writeData(json)
            self.writeString("\r\n")
        case .ping:
            self.writeString("\(NatsOperation.ping.rawValue)\r\n")
        case .pong:
            self.writeString("\(NatsOperation.pong.rawValue)\r\n")
        }
    }
}
