import Dispatch
import Foundation

/// One connected UDP peer. Reads and retries do not require application polling.
/// Call close() when finished. One task can wait for receivePacket() at a time.
public actor Client {
    private let socket: DatagramSocket
    private var peer: Peer
    private enum Event: Sendable { case socket(DatagramSocket.Event), timer }
    public nonisolated var unownedExecutor: UnownedSerialExecutor { socket.executor.asUnownedSerialExecutor() }
    public nonisolated var localPort: UInt16 { socket.port }
    public static let maximumMessageSize = Peer.maximumMessageSize
    public var channelCount: Int { peer.channelCount }
    private struct ScheduledTimer: Sendable { var deadline: UInt64; let source: DispatchSourceTimer }
    private var timer: ScheduledTimer?
    private var terminalError: (any Error)?
    private var connectWaiter: CheckedContinuation<Void, any Error>?
    private struct PacketWaiter {
        var id: UUID
        var continuation: CheckedContinuation<Packet?, any Error>
    }
    private var packetWaiter: PacketWaiter?
    private var packets: [Packet] = []
    private var packetIndex = 0
    private var packetBytes = 0
    private static var now: UInt64 { DispatchTime.now().uptimeNanoseconds / 1_000_000 }

    /// Channel count must be 1...255. Connect data is sent in the ENet handshake.
    public static func connect(host: String, port: UInt16, connectData: UInt32 = 0,
                               channelCount: Int = 48) async throws -> Client {
        guard (1...255).contains(channelCount) else { throw ClientError.invalidConnect }
        let socket = try await DatagramSocket.open(host: host, port: port)
        let client = Client(socket: socket, connectData: connectData, channelCount: channelCount)
        do { try await client.start(); return client }
        catch { await client.close(); throw error }
    }

    private init(socket: DatagramSocket, connectData: UInt32, channelCount: Int) {
        self.socket = socket
        peer = Peer(connectID: UInt32.random(in: .min ... .max), connectData: connectData,
                        channels: channelCount, now: Self.now)
    }

    deinit { socket.close(); timer?.source.cancel() }

    private func start() async throws {
        try Task.checkCancellation()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                connectWaiter = continuation
                socket.setReceiveHandler { [weak self] event in self?.deliver(.socket(event)) }
                do { try pump() } catch { finish(error: error) }
            }
        } onCancel: {
            Task { await self.finish(error: CancellationError()) }
        }
    }

    // Both native event sources run on the actor's executor. Older runtimes
    // cannot check actor isolation for non-task callbacks, so they use a task.
    private nonisolated func deliver(_ event: Event) {
        if #available(macOS 15, iOS 18, tvOS 18, visionOS 2, *) {
            assumeIsolated { $0.handle(event) }
        } else {
            Task { await self.handle(event) }
        }
    }

    private func handle(_ event: Event) {
        switch event {
        case .socket(.ready): receive(socket.takePackets())
        case .socket(.closed): finish(error: nil)
        case .socket(.failed(let error)): finish(error: error)
        case .timer:
            guard let timer, Self.now >= timer.deadline else { return }
            do { try pump() } catch { finish(error: error) }
        }
    }

    /// Enqueue a payload. A successful return does not mean the peer has ACKed it.
    /// Out-of-range channels are rejected. Large unreliable payloads use reliable fragments.
    public func send(_ data: Data, channelID: UInt8 = 0, delivery: Delivery = .reliable) throws {
        guard peer.state == .connected else { throw ClientError.notConnected }
        guard Int(channelID) < peer.channelCount else { throw ClientError.invalidChannel }
        try peer.enqueue(data, channel: channelID, delivery: delivery)
        do { try pump() } catch { finish(error: error); throw error }
    }

    /// Return a queued packet, or wait for one. Nil means a clean close.
    public func receivePacket() async throws -> Packet? {
        try Task.checkCancellation()
        if packetIndex < packets.count {
            let packet = packets[packetIndex]
            packets[packetIndex] = .init(data: Data(), channelID: 0)
            packetIndex += 1; packetBytes -= packet.data.count
            if packetIndex == packets.count { packets.removeAll(keepingCapacity: true); packetIndex = 0 }
            return packet
        }
        if case .closed(let error) = peer.state {
            if let terminalError { throw terminalError }
            if let error { throw error }
            return nil
        }
        guard packetWaiter == nil else { throw ClientError.packetReaderInUse }
        let id = UUID()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { packetWaiter = PacketWaiter(id: id, continuation: $0) }
        } onCancel: {
            Task { await self.cancelRead(id: id) }
        }
    }

    private func cancelRead(id: UUID) {
        guard packetWaiter?.id == id else { return }
        packetWaiter?.continuation.resume(throwing: CancellationError()); packetWaiter = nil
    }

    public func snapshotMetrics() -> Metrics { peer.metrics }

    private func receive(_ packets: [Data]) {
        do {
            for packet in packets {
                let wasConnecting = peer.state == .connecting
                try apply(peer.receive(packet, now: Self.now, flushACKs: false))
                if wasConnecting, peer.state == .connected {
                    connectWaiter?.resume(); connectWaiter = nil
                }
            }
            try pump()
        } catch { finish(error: error) }
    }

    private func apply(_ output: Peer.Output) throws {
        for datagram in output.datagrams { try socket.send(datagram) }
        for packet in output.packets {
            if let waiter = packetWaiter {
                packetWaiter = nil; waiter.continuation.resume(returning: packet)
            } else {
                guard packetBytes <= Peer.maximumQueuedBytes - packet.data.count,
                      packets.count - packetIndex < 4096 else { throw ClientError.queueFull }
                if packetIndex >= 256 { packets.removeFirst(packetIndex); packetIndex = 0 }
                packets.append(packet); packetBytes += packet.data.count
            }
        }
    }

    private func pump() throws {
        if case .closed(let error) = peer.state { finish(error: error); return }
        let now = Self.now
        try apply(peer.service(now: now))
        if case .closed(let error) = peer.state { finish(error: error); return }
        let deadline = max(now + 1, peer.nextServiceTime ?? now + 500)
        let instant = deadline.multipliedReportingOverflow(by: 1_000_000)
        guard !instant.overflow else { throw ClientError.timedOut }
        if let timer {
            if timer.deadline != deadline {
                timer.source.schedule(deadline: DispatchTime(uptimeNanoseconds: instant.partialValue))
                self.timer?.deadline = deadline
            }
        } else {
            let source = DispatchSource.makeTimerSource(queue: socket.queue)
            source.setEventHandler { [weak self] in self?.deliver(.timer) }
            source.schedule(deadline: DispatchTime(uptimeNanoseconds: instant.partialValue))
            timer = ScheduledTimer(deadline: deadline, source: source)
            source.resume()
        }
    }

    /// Close locally. An optional failure is returned to a pending or later reader.
    public func close(throwing error: (any Error)? = nil) {
        for datagram in peer.close(now: Self.now).datagrams { try? socket.send(datagram) }
        finish(error: error)
    }

    private func finish(error: (any Error)?) {
        guard !socket.isClosed else { return }
        _ = peer.close(now: Self.now)
        if let error { terminalError = error }
        socket.close()
        timer?.source.cancel(); timer = nil
        if let waiter = connectWaiter { connectWaiter = nil; waiter.resume(throwing: error ?? ClientError.closed) }
        if let waiter = packetWaiter {
            packetWaiter = nil
            if let error { waiter.continuation.resume(throwing: error) } else { waiter.continuation.resume(returning: nil) }
        }
    }
}
