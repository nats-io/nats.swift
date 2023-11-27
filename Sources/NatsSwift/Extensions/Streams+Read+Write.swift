//
//  Streams+Read+Write.swift
//  NatsSwift
//

import Foundation

extension InputStream {

    func readStream() -> Data? {
        let max_buffer = 4096
        var dataQueue = [Data]()
        var length = max_buffer
        let buf = NSMutableData(capacity: max_buffer)
        let buffer = UnsafeMutablePointer<UInt8>(mutating: buf!.bytes.bindMemory(to: UInt8.self, capacity: buf!.length))

        // read stream per max_buffer
        while length > 0 {
            length = self.read(buffer, maxLength: max_buffer)
            guard length > 0 else { break }
            dataQueue.append(Data(bytes: UnsafePointer<UInt8>(buffer), count: length))
            if length < max_buffer { break }
        }

        guard !dataQueue.isEmpty else { return nil }
        let data = dataQueue.reduce(Data(), {
            var combined = Data(referencing: NSData(data: $0))
            combined.append($1)
            return combined
        })
        return data
    }

    func readStreamWhenReady() -> String? {
        while (true) {
            if self.hasBytesAvailable {
                return self.readStream()?.toString()
            }
            if (self.streamError != nil) { break }
        }
        return nil
    }
}

extension OutputStream {

    func writeStream(_ data: Data) {
        let bytes = NSData(data: data).bytes.bindMemory(to: UInt8.self, capacity: data.count)
        _ = self.write(bytes, maxLength: data.count)
    }

    func writeStreamWhenReady(_ data: Data) {
        while (true) {
            if self.hasSpaceAvailable {
                self.writeStream(data)
                break
            }
            if self.streamError != nil { break }
        }
    }

}
