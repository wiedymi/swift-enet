import Darwin
import Dispatch
import Foundation
import os

/// All mutable data is inside a Sendable lock. Descriptor operations and
/// cancellation use that lock; only the source's cancel handler closes it.
/// Callbacks run on queue after unlocking. The actors share this executor.
final class DatagramSocket: Sendable {
    enum Event: Sendable { case ready, closed, failed(any Error) }
    let executor = SocketExecutor()
    var queue: DispatchSerialQueue { executor.queue }
    let port: UInt16
    var isClosed: Bool { source.isCancelled }
    private let descriptor: Int32
    private let source: DispatchSourceRead
    private struct Buffers: Sendable {
        var handler: (@Sendable (Event) -> Void)?
        var pending: [Data] = []
        var receive = [UInt8](repeating: 0, count: 4097)
    }
    private let buffers = OSAllocatedUnfairLock(initialState: Buffers())

    static func open(host: String, port: UInt16) async throws -> DatagramSocket {
        let opening = SocketOpening()
        let socket = try await withTaskCancellationHandler {
            try await opening.start { try DatagramSocket(host: host, port: port) }
        } onCancel: {
            Task { await opening.cancel() }
        }
        if Task.isCancelled { socket.close(); throw CancellationError() }
        return socket
    }

    init(host: String, port: UInt16) throws {
        var hints = addrinfo()
        hints.ai_family = AF_UNSPEC
        hints.ai_socktype = SOCK_DGRAM
        hints.ai_protocol = IPPROTO_UDP
        var addresses: UnsafeMutablePointer<addrinfo>?
        let result = getaddrinfo(host, String(port), &hints, &addresses)
        guard result == 0, let addresses else { throw ClientError.invalidConnect }
        defer { freeaddrinfo(addresses) }
        var candidate: UnsafeMutablePointer<addrinfo>? = addresses
        var candidates: [UnsafeMutablePointer<addrinfo>] = []
        while let address = candidate {
            candidates.append(address); candidate = address.pointee.ai_next
        }
        // Preserve the previous IPv4 hostname path when both families exist.
        // Explicit IPv6 addresses and IPv6-only names still use IPv6.
        candidates.sort { $0.pointee.ai_family == AF_INET && $1.pointee.ai_family != AF_INET }
        var selected: Int32 = -1
        for address in candidates {
            let fd = Darwin.socket(address.pointee.ai_family, SOCK_DGRAM, IPPROTO_UDP)
            if fd >= 0 {
                let flags = fcntl(fd, F_GETFL, 0)
                if flags >= 0, fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0,
                   Darwin.connect(fd, address.pointee.ai_addr, address.pointee.ai_addrlen) == 0 {
                    selected = fd; break
                }
                Darwin.close(fd)
            }
        }
        guard selected >= 0 else { throw ClientError.invalidConnect }
        var local = sockaddr_storage()
        var length = socklen_t(MemoryLayout<sockaddr_storage>.size)
        let query = withUnsafeMutablePointer(to: &local) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { getsockname(selected, $0, &length) }
        }
        guard query == 0 else { Darwin.close(selected); throw ClientError.invalidConnect }
        let localPort = withUnsafePointer(to: local) { pointer -> UInt16 in
            if Int32(local.ss_family) == AF_INET6 {
                return pointer.withMemoryRebound(to: sockaddr_in6.self, capacity: 1) { UInt16(bigEndian: $0.pointee.sin6_port) }
            }
            return pointer.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { UInt16(bigEndian: $0.pointee.sin_port) }
        }
        descriptor = selected
        self.port = localPort
        var bufferSize: Int32 = 256 * 1024
        _ = setsockopt(selected, SOL_SOCKET, SO_RCVBUF, &bufferSize, socklen_t(MemoryLayout<Int32>.size))
        source = DispatchSource.makeReadSource(fileDescriptor: selected, queue: executor.queue)
        source.setEventHandler { [weak self] in self?.readAvailable() }
        source.setCancelHandler { Darwin.close(selected) }
        source.resume()
    }

    deinit { close() }

    func send(_ packet: Data) throws {
        try buffers.withLock { _ in
            guard !source.isCancelled else { throw ClientError.closed }
            let count = packet.withUnsafeBytes { Darwin.send(descriptor, $0.baseAddress, $0.count, 0) }
            if count == packet.count { return }
            if count < 0 && Self.isTransient(errno) { return }
            throw ClientError.socketFailure(code: errno)
        }
    }

    func setReceiveHandler(_ handler: @escaping @Sendable (Event) -> Void) {
        let event: Event? = buffers.withLock {
            $0.handler = handler
            return source.isCancelled ? .closed : $0.pending.isEmpty ? nil : .ready
        }
        if let event { queue.async { handler(event) } }
    }

    func close() {
        let handler = buffers.withLock { value -> (@Sendable (Event) -> Void)? in
            guard !source.isCancelled else { return nil }
            value.pending.removeAll()
            let handler = value.handler
            value.handler = nil
            source.cancel()
            return handler
        }
        if let handler { queue.async { handler(.closed) } }
    }

    func takePackets() -> [Data] {
        buffers.withLock {
            let packets = $0.pending
            $0.pending = []
            return packets
        }
    }

    private func readAvailable() {
        let notification: ((@Sendable (Event) -> Void)?, Event?) = buffers.withLock { value in
            guard !source.isCancelled else { return (nil, nil) }
            var event: Event?
            let wasEmpty = value.pending.isEmpty
            let capacity = value.receive.count
            // A waiting group already has a callback. A task on an older
            // runtime can drain it without another task for each read callback.
            for _ in 0..<64 {
                // The extra byte rejects oversized UDP packets without parsing
                // a truncated packet. No bytes leave the scoped buffer access.
                let count = recv(descriptor, &value.receive, capacity, 0)
                if count >= 0 {
                    if count > 0 && count <= 4096 && value.pending.count < 256 {
                        value.pending.append(value.receive.withUnsafeBytes { Data(bytes: $0.baseAddress!, count: count) })
                        if wasEmpty { event = .ready }
                    }
                    continue
                }
                if errno == EINTR { continue }
                if Self.isTransient(errno) { break }
                event = .failed(ClientError.socketFailure(code: errno))
                break
            }
            return (value.handler, event)
        }
        if let event = notification.1 { notification.0?(event) }
    }

    private static func isTransient(_ code: Int32) -> Bool {
        [EAGAIN, EWOULDBLOCK, EINTR, ECONNREFUSED, ECONNRESET, ENETUNREACH, EHOSTUNREACH, ENOBUFS].contains(code)
    }
}

/// Address lookup itself is a system call. Cancellation releases the caller
/// immediately; a late worker result closes its descriptor instead of escaping.
actor SocketOpening {
    private enum State {
        case idle
        case waiting(CheckedContinuation<DatagramSocket, any Error>)
        case cancelled
        case finished
    }
    private var state: State = .idle

    func start(create: @escaping @Sendable () throws -> DatagramSocket) async throws -> DatagramSocket {
        try Task.checkCancellation()
        return try await withCheckedThrowingContinuation { continuation in
            switch state {
            case .cancelled: continuation.resume(throwing: CancellationError())
            case .finished, .waiting: continuation.resume(throwing: ClientError.closed)
            case .idle:
                state = .waiting(continuation)
                Task.detached { await self.finish(Result { try create() }) }
            }
        }
    }

    func cancel() {
        switch state {
        case .idle: state = .cancelled
        case .waiting(let continuation):
            state = .cancelled
            continuation.resume(throwing: CancellationError())
        case .cancelled, .finished: break
        }
    }

    private func finish(_ result: Result<DatagramSocket, any Error>) {
        switch state {
        case .waiting(let continuation):
            state = .finished
            continuation.resume(with: result)
        case .idle, .cancelled, .finished:
            if case .success(let socket) = result { socket.close() }
            state = .finished
        }
    }
}
