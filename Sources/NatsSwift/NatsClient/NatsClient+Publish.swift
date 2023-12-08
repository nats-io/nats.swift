//
//  NatsClient+Publish.swift
//  NatsSwift
//

import Foundation
import Dispatch

extension NatsClient: NatsPublish {

    // MARK: - Implement NatsPublish Protocol

    open func publish(_ payload: String, to subject: String) {
        logger.info("publish \(payload.count) characters to subject \(subject)")
        sendMessage(OldNatsMessage.publish(payload: payload, subject: subject))
    }

    open func publish(_ payload: String, to subject: NatsSubject) {
        publish(payload, to: subject.subject)
    }

    open func reply(to message: OldNatsMessage, withPayload payload: String) {
        guard let replySubject = message.replySubject else { return }
        logger.info("reply \(payload.count) characters to subject \(replySubject.subject)")
        publish(payload, to: replySubject.subject)
    }

    open func publishSync(_ payload: String, to subject: String) throws {

        let group = DispatchGroup()
        group.enter()

        var response: NatsEvent?

        self.on([.response, .error], autoOff: true) { e in
            response = e
            group.leave()
        }

        publish(payload, to: subject)

        group.wait()

        if response == .error {
            throw NatsPublishError("Error response from server")
        }

    }

    open func publishSync(_ payload: String, to subject: NatsSubject) throws {
        try publishSync(payload, to: subject.subject)
    }

    open func replySync(to message: OldNatsMessage, withPayload payload: String) throws {
        guard let replySubject = message.replySubject else { return }
        try publishSync(payload, to: replySubject.subject)
    }

}
