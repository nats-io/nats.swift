//
//  NatsClient+ServerInfo.swift
//  NatsSwift
//

import Foundation

extension NatsClient {

    /// Information about the connected server
    ///
    /// The struct holfds getters that provide some information about
    /// the server that is currently conneced.
    ///
    ///  Properties detail information can get from `NatsServer`.
    ///  - returns
    ///     - NatsServer
    ///     - `nil` if not connected
    ///
    public var serverInformation: NatsServer? { get { return self.server } }
}
