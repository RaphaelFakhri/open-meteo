import Foundation
import Testing
import OmFileFormat
import Synchronization
@testable import OmFileIO

private actor DownloadGate {
    private var opened = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    func wait() async {
        if opened { return }
        await withCheckedContinuation { waiters.append($0) }
    }
    func open() {
        opened = true
        for waiter in waiters { waiter.resume() }
        waiters.removeAll()
    }
}

private func eventually(_ condition: () async -> Bool) async throws {
    let deadline = ContinuousClock.now.advanced(by: .seconds(5))
    while !(await condition()) {
        try #require(ContinuousClock.now < deadline, "Timed out waiting for scheduler state")
        await Task.yield()
    }
}

private final class PrefetchRecordingStorage: AtomicBlockCacheStorable {
    let storage = DataAsClass(data: Data(count: (64 + 16) * 16))
    let ranges = Mutex<[Range<Int>]>([])
    var count: Int { storage.count }
    func withMutableUnsafeBytes<R>(_ body: (UnsafeMutableRawBufferPointer) throws -> R) rethrows -> R {
        try storage.withMutableUnsafeBytes(body)
    }
    func prefetchData(offset: Int, count: Int) {
        ranges.withLock { $0.append(offset..<offset + count) }
    }
}

@Suite struct AtomicCacheCoordinatorTests {
    @Test(.timeLimit(.minutes(1)))
    func abandonedDownloadCanBeRetried() async throws {
        let coordinator = AtomicCacheCoordinator(cache: AtomicBlockCache(data: PrefetchRecordingStorage(), blockSize: 64))
        let started = Mutex(false)
        let reader = Task {
            try await coordinator.get(key: 1, count: 1, provider: { _, _ in
                started.withLock { $0 = true }
                try await Task.sleep(for: .seconds(60))
                return Data(repeating: 0, count: 64)
            }, dataCallback: { _, _ in Issue.record("Cancelled download returned data") })
        }
        do {
            try await eventually { started.withLock { $0 } }
            reader.cancel()
            do { try await reader.value; Issue.record("Expected cancellation") } catch is CancellationError { }
            try await coordinator.get(key: 1, count: 1, provider: { _, _ in Data(repeating: 7, count: 64) },
                                      dataCallback: { _, bytes in #expect(Data(bytes) == Data(repeating: 7, count: 64)) })
        } catch {
            reader.cancel()
            _ = await reader.result
            throw error
        }
    }

    @Test
    func failedDownloadCanBeRetried() async throws {
        let coordinator = AtomicCacheCoordinator(cache: AtomicBlockCache(data: PrefetchRecordingStorage(), blockSize: 64))
        do {
            try await coordinator.get(key: 1, count: 2, provider: { _, _ -> Data in
                throw CurlErrorNonRetry.fileModifiedOrPrevalidationFailed
            }, dataCallback: { _, _ in Issue.record("Failed download returned data") })
            Issue.record("Expected failure")
        } catch CurlErrorNonRetry.fileModifiedOrPrevalidationFailed { }
        try await coordinator.get(key: 1, count: 2, provider: { _, count in Data(repeating: 7, count: count * 64) },
                                  dataCallback: { _, bytes in #expect(Data(bytes) == Data(repeating: 7, count: 64)) })
    }

    @Test
    func warmingChunksRespectFileEndAndSuperBlocks() {
        let chunks = OmPreloadBlockChunks(ranges: [120..<160, 260..<280, 400..<410],
                                         blockCount: 270, superBlockLength: 128, chunkLength: 16)
        #expect(Array(chunks) == [120..<128, 128..<144, 144..<160, 260..<270])
    }

    @Test(.timeLimit(.minutes(1)))
    func demandBypassesPendingPreloadsAndSharesActiveDownload() async throws {
        let cache = AtomicBlockCache(data: PrefetchRecordingStorage(), blockSize: 64)
        let coordinator = AtomicCacheCoordinator(cache: cache)
        let executor = RemotePreloadExecutor(concurrency: 1)
        let gate = DownloadGate()
        let state = Mutex((started: false, downloads: 0))
        let preload = Task {
            try await executor.run(count: 2, concurrency: 1) { index in
                do {
                    try await coordinator.get(key: UInt64(100 + index), count: 1, provider: { _, _ in
                        state.withLock { $0.started = true; $0.downloads += 1 }
                        if index == 0 { await gate.wait() }
                        return Data(repeating: 7, count: 64)
                    }, dataCallback: { _, _ in })
                } catch { Issue.record("Unexpected preload failure: \(error)") }
            }
        }
        do {
            try await eventually { state.withLock { $0.started } }
            // The queued second job must not reserve its cache key or block demand.
            try await coordinator.get(key: 101, count: 1, provider: { _, _ in
                state.withLock { $0.downloads += 1 }
                return Data(repeating: 7, count: 64)
            }, dataCallback: { _, bytes in #expect(Data(bytes) == Data(repeating: 7, count: 64)) })
            let demand = Task {
                try await coordinator.get(key: 100, count: 1, provider: { _, _ in
                    Issue.record("Active download must be shared")
                    return Data(repeating: 0, count: 64)
                }, dataCallback: { _, bytes in #expect(Data(bytes) == Data(repeating: 7, count: 64)) })
            }
            preload.cancel()
            await gate.open()
            try await demand.value
            do { try await preload.value; Issue.record("Expected stopped request") } catch is CancellationError { }
            #expect(state.withLock { $0.downloads } == 2)
        } catch {
            preload.cancel()
            await gate.open()
            _ = await preload.result
            throw error
        }
    }

    @Test func awaitedPrefetchCompletesDownloadsAndPagesPayloads() async throws {
        let storage = PrefetchRecordingStorage()
        let cache = AtomicBlockCache(data: storage, blockSize: 64)
        let reader = OmReaderBlockCache(backend: DataAsClass(data: Data(repeating: 7, count: 128)), cache: AtomicCacheCoordinator(cache: cache), cacheKey: 123)
        try await OmReaderPrefetch.$waitForCompletion.withValue(true) {
            try await reader.prefetchData(offset: 0, count: 128)
            #expect(cache.get(key: reader.calculateCacheKey(block: 0), count: 2) != nil)
            #expect(storage.ranges.withLock { $0.count } == 2)
            try await reader.prefetchData(offset: 0, count: 128)
            #expect(storage.ranges.withLock { $0.count } == 3)
        }
    }

    @Test(.timeLimit(.minutes(1))) func fileModificationCompletionRacesWithWaiterRegistration() async throws {
        let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let cache = try AtomicBlockCache(file: path, blockSize: 64, blockCount: 16)
        let coordinator = AtomicCacheCoordinator(cache: cache)
        for iteration in 0..<200 {
            let readers = (0..<32).map { index in
                Task {
                    do {
                        try await coordinator.get(key: UInt64(index % 3), count: 3, provider: { _, _ -> Data in
                            await Task.yield()
                            if iteration % 2 == 0 { await Task.yield() }
                            throw CurlErrorNonRetry.fileModifiedOrPrevalidationFailed
                        }, dataCallback: { _, _ in Issue.record("Failed fetch produced data") })
                        Issue.record("Expected file modification or cancellation")
                    } catch is CancellationError {
                    } catch CurlErrorNonRetry.fileModifiedOrPrevalidationFailed {
                    } catch {
                        Issue.record("Unexpected error: \(error)")
                    }
                }
            }
            for (index, reader) in readers.enumerated() where index % 3 == 0 { reader.cancel() }
            for reader in readers { await reader.value }
        }
    }

    @Test func prefetchMissUsesCacheStorageOffsets() async throws {
        let storage = PrefetchRecordingStorage()
        let cache = AtomicBlockCache(data: storage, blockSize: 64)
        let reader = OmReaderBlockCache(backend: DataAsClass(data: Data(repeating: 7, count: 128)), cache: AtomicCacheCoordinator(cache: cache), cacheKey: 123)
        try await reader.prefetchData(offset: 0, count: 128)
        try await eventually { storage.ranges.withLock { $0.count == 2 } }
        for range in storage.ranges.withLock({ $0 }) {
            #expect(range.lowerBound >= 0)
            #expect(range.upperBound <= storage.count)
        }
        #expect(try await reader.getData(offset: 0, count: 128) == Data(repeating: 7, count: 128))
    }
}
