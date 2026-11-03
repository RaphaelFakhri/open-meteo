import Foundation
import Synchronization
import Testing
@testable import OmFileIO

private actor PreloadGate {
    private var open = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    func wait() async {
        if open { return }
        await withCheckedContinuation { waiters.append($0) }
    }
    func release() {
        open = true
        for waiter in waiters { waiter.resume() }
        waiters.removeAll()
    }
}

private func waitForPreload(_ condition: () -> Bool) async throws {
    let deadline = ContinuousClock.now.advanced(by: .seconds(5))
    while !condition() {
        try #require(ContinuousClock.now < deadline)
        try await Task.sleep(for: .milliseconds(1))
    }
}

@Suite struct RemotePreloadExecutorTests {
    @Test(.timeLimit(.minutes(1)))
    func rotationYieldsBetweenChunksToRequestWork() async throws {
        let executor = RemotePreloadExecutor(concurrency: 1)
        let gate = PreloadGate()
        let log = Mutex<[String]>([])
        let warming = await executor.stage(key: "file", jobs: 0..<2) { chunk in
            log.withLock { $0.append("warm\(chunk)") }
            if chunk == 0 { await gate.wait() }
        }
        #expect(await executor.snapshot.activeWarmups == 0)
        await executor.activate(warming)
        let request = Task {
            try await waitForPreload { log.withLock { $0 == ["warm0"] } }
            try await executor.run(count: 1, concurrency: 1) { _ in
                log.withLock { $0.append("request") }
            }
        }
        do {
            let deadline = ContinuousClock.now.advanced(by: .seconds(5))
            while await executor.snapshot.queuedRequests == 0 {
                try #require(ContinuousClock.now < deadline)
                try await Task.sleep(for: .milliseconds(1))
            }
            #expect(await executor.snapshot.activeWarmups == 1)
            await gate.release()
            try await request.value
            try await waitForPreload { log.withLock { $0.count == 3 } }
            #expect(log.withLock { $0 } == ["warm0", "request", "warm1"])
            await executor.shutdown()
        } catch {
            request.cancel()
            await gate.release()
            await executor.shutdown()
            throw error
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func replacementStopsFutureChunksButFinishesActiveChunk() async throws {
        let executor = RemotePreloadExecutor(concurrency: 1)
        let gate = PreloadGate()
        let log = Mutex<[String]>([])
        let old = await executor.stage(key: "file", jobs: 0..<2) { chunk in
            log.withLock { $0.append("old\(chunk)") }
            await gate.wait()
            #expect(!Task.isCancelled)
            log.withLock { $0.append("finished") }
        }
        await executor.activate(old)
        do {
            try await waitForPreload { log.withLock { $0 == ["old0"] } }
            let latest = await executor.stage(key: "file", jobs: 0..<1) { _ in
                log.withLock { $0.append("latest") }
            }
            await executor.activate(latest)
            #expect(await executor.snapshot.activeWarmups == 1)
            #expect(await executor.snapshot.stoppedWarmups == 1)
            await gate.release()
            try await waitForPreload { log.withLock { $0.count == 3 } }
            #expect(log.withLock { $0 } == ["old0", "finished", "latest"])
            await executor.shutdown()
        } catch {
            await gate.release()
            await executor.shutdown()
            throw error
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func stagingReplacesUnpublishedWorkAndBoundsPendingFiles() async throws {
        let executor = RemotePreloadExecutor(concurrency: 1, warmingCapacity: 1)
        let log = Mutex<[String]>([])
        let obsolete = await executor.stage(key: "file", jobs: 0..<1) { _ in log.withLock { $0.append("obsolete") } }
        let replaced = await executor.stage(key: "file", jobs: 0..<1) { _ in log.withLock { $0.append("replaced") } }
        let latest = await executor.stage(key: "another", jobs: 0..<1) { _ in log.withLock { $0.append("latest") } }
        await executor.activate(obsolete)
        await executor.activate(replaced)
        #expect(log.withLock { $0.isEmpty })
        #expect(await executor.snapshot.queuedWarmups == 1)
        #expect(await executor.snapshot.stoppedWarmups == 2)
        await executor.activate(latest)
        try await waitForPreload { log.withLock { !$0.isEmpty } }
        #expect(log.withLock { $0 } == ["latest"])
        await executor.shutdown()
    }

    @Test(.timeLimit(.minutes(1)))
    func requestsShareGlobalCapacity() async throws {
        let executor = RemotePreloadExecutor(concurrency: 2)
        let gate = PreloadGate()
        let state = Mutex((active: 0, maximum: 0, calls: Set<Int>()))
        let tasks = (0..<2).map { call in
            Task {
                try await executor.run(count: 20, concurrency: 1) { _ in
                    state.withLock {
                        $0.active += 1
                        $0.maximum = max($0.maximum, $0.active)
                        $0.calls.insert(call)
                    }
                    await gate.wait()
                    state.withLock { $0.active -= 1 }
                }
            }
        }
        do {
            try await waitForPreload { state.withLock { $0.calls.count == 2 } }
            #expect(state.withLock { $0.active } == 2)
            await gate.release()
            for task in tasks { try await task.value }
            #expect(state.withLock { $0.maximum } == 2)
        } catch {
            for task in tasks { task.cancel() }
            await gate.release()
            for task in tasks { _ = await task.result }
            throw error
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func stoppedRequestKeepsActiveSlotUntilDownloadFinishes() async throws {
        let executor = RemotePreloadExecutor(concurrency: 1)
        let gate = PreloadGate()
        let state = Mutex((started: 0, finished: false, nextStarted: false, stopped: false))
        let first = Task {
            try await executor.run(count: 100, concurrency: 4) { _ in
                state.withLock { $0.started += 1 }
                await gate.wait()
                state.withLock {
                    $0.finished = true
                    $0.stopped = (try? RemotePreloadSession.current?.checkActive()) == nil
                }
            }
        }
        do {
            try await waitForPreload { state.withLock { $0.started == 1 } }
            first.cancel()
            let second = Task {
                try await executor.run(count: 1, concurrency: 1) { _ in
                    state.withLock {
                        #expect($0.finished)
                        $0.nextStarted = true
                    }
                }
            }
            #expect(!state.withLock { $0.finished })
            await gate.release()
            do { try await first.value; Issue.record("Expected cancelled request") }
            catch is CancellationError { }
            try await second.value
            #expect(state.withLock { $0.started } == 1)
            #expect(state.withLock { $0.stopped && $0.nextStarted })
        } catch {
            first.cancel()
            await gate.release()
            _ = await first.result
            throw error
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func slowJobDoesNotBlockLaterJobsAndPerCallLimitIsRespected() async throws {
        let executor = RemotePreloadExecutor(concurrency: 8)
        let gate = PreloadGate()
        let state = Mutex((active: 0, maximum: 0, finished: 0))
        let request = Task {
            try await executor.run(count: 30, concurrency: 2) { index in
                state.withLock { $0.active += 1; $0.maximum = max($0.maximum, $0.active) }
                if index == 0 { await gate.wait() }
                state.withLock { $0.active -= 1; $0.finished += 1 }
            }
        }
        do {
            try await waitForPreload { state.withLock { $0.finished == 29 } }
            #expect(state.withLock { $0.maximum } <= 2)
            await gate.release()
            try await request.value
            #expect(state.withLock { $0.finished } == 30)
        } catch {
            request.cancel()
            await gate.release()
            _ = await request.result
            throw error
        }
    }
}
