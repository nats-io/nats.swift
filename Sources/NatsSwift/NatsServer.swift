//
//  NatsServer.swift
//  NatsSwift
//

import Foundation

public struct NatsServer: Codable {

    /// The host  the connected NATS server is listening on
    public let host: String

    /// The port of the connected NATS server the sessionis connected to.
    public let port: UInt

    public let clientIp: String?
    public let clientId: UInt?

    /// The name of the NATS server the session is connected to.
    public let serverName: String

    /// The ID of the server the session is connected to.
    public let serverId: String

    public let maxPayload: UInt

    /// The version of the NATS server the session is connected to.
    public let version: String
    public let go: String
    public let proto: UInt
    public let gitCommit: String?

    public let authRequired: Bool?
    public let sslRequired: Bool?

    public let connectUrls: [String]?
    public let tlsVerify: Bool?

    init(_ data: [String: AnyObject]) {
        self.host = data["host"] as! String
        self.port = data["port"] as! UInt
        self.clientIp = data["client_ip"] as? String
        self.clientId = data["client_id"] as? UInt
        self.serverName = data["server_name"] as! String
        self.serverId = data["server_id"] as! String
        self.maxPayload = data["max_payload"] as! UInt
        self.version = data["version"] as? String ?? "0.0.0"
        self.go = data["go"] as? String ?? ""
        self.proto = data["proto"] as! UInt
        self.gitCommit = data["git_commit"] as? String ?? ""

        self.authRequired = data["auth_required"] as? Bool ?? false
        self.sslRequired = data["ssl_required"] as? Bool ?? false

        self.connectUrls = data["connect_urls"] as? [String]
        self.tlsVerify = data["tls_verify"] as? Bool
    }

}
