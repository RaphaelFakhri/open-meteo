import OmFileIO

/// Keep normal paging separate from early remote block-cache warming.
/// Applied at the resolved file reader so derived variables and local fallbacks
/// follow the same policy as directly requested variables.
enum ReaderPrefetchPolicy: Sendable {
    case all
    case remoteOnly
    case localOnly

    func includes(isRemote: Bool) -> Bool {
        switch self {
        case .all: return true
        case .remoteOnly: return isRemote
        case .localOnly: return !isRemote
        }
    }

    static let concurrency = RemoteDataHttpClientLifecycle.positiveLimit("REMOTE_DATA_PREFETCH_CONCURRENCY", default: 10)

    static let executor = RemotePreloadExecutor(
        concurrency: RemoteDataHttpClientLifecycle.positiveLimit("REMOTE_DATA_PREFETCH_GLOBAL_CONCURRENCY", default: 32),
        warmingConcurrency: RemoteDataHttpClientLifecycle.positiveLimit("REMOTE_DATA_BACKGROUND_DOWNLOADS", default: 4)
    )

    @TaskLocal static var current: ReaderPrefetchPolicy = .all
}

/// The response owns this handle; the task captures the result without the handle.
/// Dropping an unconsumed response also stops its pending preload work.
final class ReaderPreload: Sendable {
    private let task: Task<Void, Never>

    init(operation: @escaping @Sendable () async -> Void) {
        task = Task { await operation() }
    }

    func stop() { task.cancel() }
    deinit { task.cancel() }
}
