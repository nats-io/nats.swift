import NIOCore

internal class PingCommand {
    let startTime = ContinuousClock().now
    let promise: EventLoopPromise<Duration>?
    
    static func makeFrom(channel: Channel?) -> PingCommand {
        PingCommand(promise: channel?.eventLoop.makePromise(of: Duration.self))
    }
    
    private init(promise: EventLoopPromise<Duration>?) {
        self.promise = promise
    }
    
    func setRoundTripTime() {
        let now: ContinuousClock.Instant = ContinuousClock().now
        let rtt: Duration = now - startTime
        promise?.succeed(rtt)
    }
    
    func getRoundTripTime() async throws -> Duration {
        try await promise?.futureResult.get() ?? Duration.zero
    }
}
