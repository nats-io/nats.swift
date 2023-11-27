//
//  NatsResponse.swift
//  SwiftyNats
//

internal enum NatsResponseType {
    case success
    case error
}

struct NatsResponse {
    
    let type: NatsResponseType
    let message: String?
    
    init(_ response: String) {
        if response.hasPrefix(NatsOperation.ok.rawValue) {
            self.type = .success
            self.message = nil
        } else {
            self.type = .error
            self.message = response.removePrefix(NatsOperation.error.rawValue)
        }
    }
    
    internal static func error() -> NatsResponse {
        return NatsResponse("")
    }
    
}
