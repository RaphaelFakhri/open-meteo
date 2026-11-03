import Foundation
import Synchronization

/// Stops future preload I/O without cancelling a download already in progress.
public final class RemotePreloadSession: Sendable {
    private let stopped = Mutex(false)
    public init() {}
    public var isStopped: Bool { stopped.withLock { $0 } }
    public func stop() { stopped.withLock { $0 = true } }
    public func checkActive() throws {
        if isStopped { throw CancellationError() }
    }
    @TaskLocal public static var current: RemotePreloadSession?
}

/// Lazy per-request work, admitted round-robin. A slot covers the complete job,
/// including metadata and payload I/O; stopping a request does not free active slots.
public actor RemotePreloadExecutor {
    private struct Request {
        let id: UUID
        let session: RemotePreloadSession
        let count: Int
        let concurrency: Int
        let operation: @Sendable (Int) async -> Void
        let completion: CheckedContinuation<Void, Never>
        var next = 0
        var active = 0
    }
    private struct Warmup {
        let id: UUID
        let key: String
        let session: RemotePreloadSession
        var next: () -> (@Sendable () async -> Void)?
        var ready = false
        var active = false
    }
    public struct Snapshot: Sendable {
        public let activeRequests: Int
        public let activeWarmups: Int
        public let queuedRequests: Int
        public let queuedWarmups: Int
        public let stoppedRequests: Int
        public let stoppedWarmups: Int
    }
    private var warmups: [Warmup] = []
    private var tasks: [UUID: Task<Void, Never>] = [:]
    private var activeWarmups = 0
    private var stoppedRequests = 0
    private var stoppedWarmups = 0
    private var stopped = false
    private let warmingConcurrency: Int
    private let warmingCapacity: Int
    private let concurrency: Int
    private var active = 0
    private var requests: [Request] = []

    public init(concurrency: Int, warmingConcurrency: Int = 4, warmingCapacity: Int = 256) {
        precondition(concurrency > 0 && warmingConcurrency > 0 && warmingCapacity > 0)
        self.concurrency = concurrency
        self.warmingConcurrency = min(warmingConcurrency, concurrency)
        self.warmingCapacity = warmingCapacity
    }

    public var snapshot: Snapshot {
        Snapshot(activeRequests: active - activeWarmups, activeWarmups: activeWarmups,
                 queuedRequests: requests.filter { $0.next < $0.count }.count,
                 queuedWarmups: warmups.filter { !$0.active && !$0.session.isStopped }.count,
                 stoppedRequests: stoppedRequests, stoppedWarmups: stoppedWarmups)
    }

    /// Stage a lazy sequence of bounded chunks. Publication must precede activation.
    public func stage<S: Sequence & Sendable>(key: String, jobs: S,
        operation: @escaping @Sendable (S.Element) async -> Void
    ) -> UUID where S.Element: Sendable {
        cancel(key: key)
        let id = UUID()
        guard !stopped else { return id }
        if warmups.filter({ !$0.active }).count >= warmingCapacity,
           let oldest = warmups.first(where: { !$0.active }) {
            cancel(key: oldest.key)
        }
        var iterator = jobs.makeIterator()
        warmups.append(Warmup(id: id, key: key, session: RemotePreloadSession(), next: {
            guard let job = iterator.next() else { return nil }
            return { await operation(job) }
        }))
        return id
    }

    public func activate(_ id: UUID) {
        if let index = warmups.firstIndex(where: { $0.id == id }) { warmups[index].ready = true }
        drain()
    }

    public func cancel(key: String) {
        for work in warmups where work.key == key {
            if !work.session.isStopped { stoppedWarmups += 1 }
            work.session.stop()
        }
        warmups.removeAll { !$0.active && $0.session.isStopped }
    }

    public func shutdown() async {
        stopped = true
        for request in requests { request.session.stop() }
        for work in warmups { work.session.stop() }
        let running = Array(tasks.values)
        for task in running { task.cancel() }
        drain()
        for task in running { await task.value }
    }

    public func run(count: Int, concurrency: Int, operation: @escaping @Sendable (Int) async -> Void) async throws {
        precondition(count >= 0 && concurrency > 0)
        guard !stopped else { throw CancellationError() }
        let session = RemotePreloadSession()
        let id = UUID()
        await withTaskCancellationHandler {
            await withCheckedContinuation { completion in
                requests.append(Request(id: id, session: session, count: count, concurrency: concurrency, operation: operation, completion: completion))
                drain()
            }
        } onCancel: {
            session.stop()
            Task { await self.stop(id) }
        }
        try Task.checkCancellation()
        try session.checkActive()
    }

    private func stop(_ id: UUID) {
        if let index = requests.firstIndex(where: { $0.id == id }) {
            requests[index].next = requests[index].count
            stoppedRequests += 1
        }
        drain()
    }

    private func completed(_ id: UUID, task: UUID) {
        tasks.removeValue(forKey: task)
        active -= 1
        if let index = requests.firstIndex(where: { $0.id == id }) {
            requests[index].active -= 1
        }
        drain()
    }

    private func drain() {
        // Retain stopped requests until active I/O finishes.
        for index in requests.indices.reversed() {
            if requests[index].session.isStopped {
                requests[index].next = requests[index].count
            }
            if requests[index].next == requests[index].count && requests[index].active == 0 {
                requests.remove(at: index).completion.resume()
            }
        }
        warmups.removeAll { !$0.active && $0.session.isStopped }
        guard !stopped else { return }
        while active < concurrency,
              let index = requests.firstIndex(where: { $0.next < $0.count && $0.active < $0.concurrency }) {
            var request = requests.remove(at: index)
            let job = request.next
            request.next += 1
            request.active += 1
            requests.append(request)
            active += 1
            let operation = request.operation
            let session = request.session
            let id = request.id
            let task = UUID()
            tasks[task] = Task {
                await RemotePreloadSession.$current.withValue(session) {
                    if !session.isStopped { await operation(job) }
                }
                completed(id, task: task)
            }
        }
        // Request work always gets first admission. Each warming job is one chunk.
        while active < concurrency && activeWarmups < warmingConcurrency,
              let index = warmups.firstIndex(where: { $0.ready && !$0.active }) {
            var work = warmups.remove(at: index)
            guard let operation = work.next() else { continue }
            work.active = true
            warmups.append(work)
            active += 1
            activeWarmups += 1
            let id = work.id
            let session = work.session
            let task = UUID()
            tasks[task] = Task {
                await RemotePreloadSession.$current.withValue(session) {
                    if !session.isStopped { await operation() }
                }
                tasks.removeValue(forKey: task)
                active -= 1
                activeWarmups -= 1
                if let index = warmups.firstIndex(where: { $0.id == id }) {
                    // Move the completed file behind other files for chunk-level fairness.
                    var work = warmups.remove(at: index)
                    work.active = false
                    warmups.append(work)
                }
                drain()
            }
        }
    }
}
