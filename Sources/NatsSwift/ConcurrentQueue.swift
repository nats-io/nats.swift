import NIOConcurrencyHelpers

internal class ConcurrentQueue<T> {
    private var elements: [T] = []
    private let lock = NIOLock()

    func enqueue(_ element: T) {
        lock.lock()
        defer { lock.unlock() }
        elements.append(element)
    }

    func dequeue() -> T? {
        lock.lock()
        defer { lock.unlock() }
        guard !elements.isEmpty else { return nil }
        return elements.removeFirst()
    }
}
