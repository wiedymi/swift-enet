import Dispatch

/// Native callbacks and Swift jobs enter through the same serial queue.
/// Explicit queue submission also makes their ordering visible to race checks.
final class SocketExecutor: SerialExecutor {
    let queue = DispatchSerialQueue(label: "swift-enet.udp")

    func enqueue(_ job: consuming ExecutorJob) {
        let job = UnownedJob(job)
        queue.async { job.runSynchronously(on: self.asUnownedSerialExecutor()) }
    }

    func checkIsolated() { dispatchPrecondition(condition: .onQueue(queue)) }
}
