import Darwin
import Dispatch
import Foundation
import Testing
@testable import SwiftENet

@Test func datagramSocketCoalescedReadKeepsEveryPacket() async throws {
    let server = try TestListener(host: "127.0.0.1")
    let socket = try DatagramSocket(host: "127.0.0.1", port: server.port)
    defer { socket.close() }
    try socket.send(Data([0]))
    let address = try await server.receive().address
    let reader = Task {
        var iterator = socketEvents(socket).makeAsyncIterator()
        var packets: [Data] = []
        while packets.count < 32 {
            guard let group = try await iterator.next() else { throw ClientError.closed }
            packets += group
        }
        return packets
    }
    let deadline = Task {
        do { try await Task.sleep(for: .seconds(2)); socket.close() } catch {}
    }
    defer { deadline.cancel(); reader.cancel() }
    for number in 0..<32 { try server.send(Data([UInt8(number)]), to: address) }
    #expect(try await reader.value == (0..<32).map { Data([UInt8($0)]) })
}

@Test(arguments: ["127.0.0.1", "::1"])
func datagramSocketReadinessAndLocalPort(host: String) async throws {
    let server = try TestListener(host: host)
    let socket = try DatagramSocket(host: host, port: server.port)
    defer { socket.close() }
    #expect(socket.port > 0)
    try socket.send(Data([1, 2, 3]))
    let incoming = try await server.receive()
    #expect(incoming.data == Data([1, 2, 3]))
    let receiver = Task { var iterator = socketEvents(socket).makeAsyncIterator(); return try await iterator.next()?.first }
    // Oversized packets must be rejected rather than parsed after truncation.
    try server.send(Data(repeating: 0xFF, count: 5000), to: incoming.address)
    try server.send(Data([4, 5, 6]), to: incoming.address)
    #expect(try await receiver.value == Data([4, 5, 6]))
}

@Test func datagramSocketCloseAndCancelWakeReader() async throws {
    let server = try TestListener(host: "127.0.0.1")
    let socket = try DatagramSocket(host: "127.0.0.1", port: server.port)
    let receiver = Task { var iterator = socketEvents(socket).makeAsyncIterator(); return try await iterator.next()?.first }
    receiver.cancel()
    #expect(try await receiver.value == nil)
    socket.close(); socket.close()
    #expect(throws: ClientError.closed) { try socket.send(Data([1])) }
    let other = try DatagramSocket(host: "127.0.0.1", port: server.port)
    let waiting = Task { var iterator = socketEvents(other).makeAsyncIterator(); return try await iterator.next()?.first }
    other.close()
    #expect(try await waiting.value == nil)
}

@Test func clientCancellationStopsConnectionSetup() async throws {
    let server = try TestListener(host: "127.0.0.1")
    let connecting = Task {
        try await Client.connect(host: "127.0.0.1", port: server.port, connectData: 1)
    }
    _ = try await server.receive()
    connecting.cancel()
    do {
        let session = try await connecting.value
        await session.close()
        Issue.record("Cancelled connection succeeded")
    } catch { #expect(error is CancellationError) }
}

// Tests use a stream adapter; production callbacks enter the connection actor
// directly on its serial queue on supported runtimes.
private func socketEvents(_ socket: DatagramSocket) -> AsyncThrowingStream<[Data], any Error> {
    let pair = AsyncThrowingStream<[Data], any Error>.makeStream(bufferingPolicy: .bufferingOldest(256))
    socket.setReceiveHandler { [weak socket] event in
        if let socket { dispatchPrecondition(condition: .onQueue(socket.queue)) }
        switch event {
        case .ready:
            if let socket { pair.continuation.yield(socket.takePackets()) }
        case .closed: pair.continuation.finish()
        case .failed(let error): pair.continuation.finish(throwing: error)
        }
    }
    return pair.stream
}

// Nonblocking test socket. Every receive has a finite deadline. The server is
// retained by each task that uses it, so descriptor teardown cannot race I/O.
private final class TestListener: Sendable {
    struct Packet: Sendable { let data: Data; let address: Data }
    let port: UInt16
    private let descriptor: Int32

    init(host: String) throws {
        var hints = addrinfo(); hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_DGRAM; hints.ai_protocol = IPPROTO_UDP
        var result: UnsafeMutablePointer<addrinfo>?
        guard getaddrinfo(host, "0", &hints, &result) == 0, let result else { throw ClientError.invalidConnect }
        defer { freeaddrinfo(result) }
        let fd = Darwin.socket(result.pointee.ai_family, SOCK_DGRAM, IPPROTO_UDP)
        guard fd >= 0 else { throw ClientError.invalidConnect }
        guard Darwin.bind(fd, result.pointee.ai_addr, result.pointee.ai_addrlen) == 0,
              fcntl(fd, F_SETFL, O_NONBLOCK) == 0 else { Darwin.close(fd); throw ClientError.invalidConnect }
        var address = sockaddr_storage(); var length = socklen_t(MemoryLayout<sockaddr_storage>.size)
        let query = withUnsafeMutablePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { getsockname(fd, $0, &length) }
        }
        guard query == 0 else { Darwin.close(fd); throw ClientError.invalidConnect }
        port = withUnsafePointer(to: address) {
            if Int32(address.ss_family) == AF_INET6 {
                return $0.withMemoryRebound(to: sockaddr_in6.self, capacity: 1) { UInt16(bigEndian: $0.pointee.sin6_port) }
            }
            return $0.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { UInt16(bigEndian: $0.pointee.sin_port) }
        }
        descriptor = fd
    }
    deinit { Darwin.close(descriptor) }
    func receive() async throws -> Packet {
        let deadline = ContinuousClock.now + .seconds(3)
        while ContinuousClock.now < deadline {
            try Task.checkCancellation()
            var bytes = [UInt8](repeating: 0, count: 65535)
            var address = sockaddr_storage(); var length = socklen_t(MemoryLayout<sockaddr_storage>.size)
            let count = withUnsafeMutablePointer(to: &address) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { recvfrom(descriptor, &bytes, bytes.count, 0, $0, &length) }
            }
            if count >= 0 {
                let addressBytes = withUnsafeBytes(of: address) { Data($0.prefix(Int(length))) }
                return Packet(data: Data(bytes.prefix(count)), address: addressBytes)
            }
            guard errno == EAGAIN || errno == EWOULDBLOCK else { throw ClientError.invalidPacket }
            try await Task.sleep(for: .milliseconds(1))
        }
        throw ClientError.timedOut
    }
    func send(_ packet: Data, to address: Data) throws {
        var storage = sockaddr_storage()
        withUnsafeMutableBytes(of: &storage) { $0.copyBytes(from: address) }
        let count = packet.withUnsafeBytes { bytes in
            withUnsafePointer(to: &storage) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { sendto(descriptor, bytes.baseAddress, bytes.count, 0, $0, socklen_t(address.count)) }
            }
        }
        guard count == packet.count else { throw ClientError.invalidPacket }
    }
}

@Test func hostnameKeepsIPv4Compatibility() async throws {
    let server = try TestListener(host: "127.0.0.1")
    let socket = try DatagramSocket(host: "localhost", port: server.port)
    defer { socket.close() }
    try socket.send(Data([1]))
    #expect(try await server.receive().data == Data([1]))
}

@Test func cancelledAddressLookupClosesLateSocket() async throws {
    let server = try TestListener(host: "127.0.0.1")
    let opening = SocketOpening()
    let gate = DispatchSemaphore(value: 0)
    let started = AsyncStream<Void>.makeStream()
    let created = AsyncStream<DatagramSocket>.makeStream()
    let caller = Task {
        try await opening.start {
            started.continuation.yield(())
            guard gate.wait(timeout: .now() + 2) == .success else { throw ClientError.timedOut }
            let socket = try DatagramSocket(host: "127.0.0.1", port: server.port)
            created.continuation.yield(socket)
            return socket
        }
    }
    var waiting = started.stream.makeAsyncIterator()
    _ = await waiting.next()
    await opening.cancel()
    do { _ = try await caller.value; Issue.record("Cancelled lookup returned a socket") }
    catch { #expect(error is CancellationError) }
    gate.signal()
    var result = created.stream.makeAsyncIterator()
    let late = try #require(await result.next())
    let deadline = ContinuousClock.now + .seconds(1)
    var closed = false
    while ContinuousClock.now < deadline {
        do { try late.send(Data([1])) }
        catch ClientError.closed { closed = true; break }
        try await Task.sleep(for: .milliseconds(1))
    }
    #expect(closed)
    late.close()
    started.continuation.finish(); created.continuation.finish()
}

@Test func clientPreservesChannelsAndRetriesWithoutPolling() async throws {
    let server = try TestListener(host: "127.0.0.1")
    let host = Task {
        let connect = try await server.receive()
        let command = try #require(try Datagram.decode(connect.data).commands.first)
        guard case .connect(var parameters, _) = command.body else { throw ClientError.invalidConnect }
        parameters.peerID = 9; parameters.incomingSession = 1; parameters.outgoingSession = 2
        try server.send(Datagram(peerID: 0, sessionID: 1, sentTime: 0,
                                    commands: [.init(sequence: 1, body: .verify(parameters))]).encoded(), to: connect.address)
        var first: Command?
        var completed = false
        while !completed {
            let packet = try await server.receive()
            let datagram = try Datagram.decode(packet.data)
            for command in datagram.commands {
                if case .reliable = command.body {
                    if let first {
                        #expect(command == first)
                        completed = true
                    } else { first = command; continue }
                    let time = try #require(datagram.sentTime)
                    try server.send(Datagram(peerID: 0, sessionID: 1, sentTime: 1, commands: [
                        .init(channel: command.channel, body: .acknowledge(sequence: command.sequence, time: time)),
                        .init(channel: 3, sequence: 1, body: .reliable(Data([9, 8, 7])))
                    ]).encoded(), to: packet.address)
                }
            }
        }
    }
    let client = try await Client.connect(host: "127.0.0.1", port: server.port, channelCount: 4)
    #expect(client.localPort > 0)
    #expect(await client.channelCount == 4)
    do { try await client.send(Data([1]), channelID: 4); Issue.record("Invalid channel accepted") }
    catch { #expect(error as? ClientError == .invalidChannel) }
    try await client.send(Data([1, 2, 3]), channelID: 3)
    try await host.value
    #expect(try await client.receivePacket() == Packet(data: Data([9, 8, 7]), channelID: 3))
    #expect(await client.snapshotMetrics().isConnected)
    let waiting = Task { try await client.receivePacket() }
    try await Task.sleep(for: .milliseconds(1))
    waiting.cancel()
    do { _ = try await waiting.value; Issue.record("Cancelled receive succeeded") }
    catch { #expect(error is CancellationError) }
    await client.close(); await client.close()
    #expect(try await client.receivePacket() == nil)
    #expect(!(await client.snapshotMetrics().isConnected))
}

@Test(arguments: [0, 256, Int.max]) func clientRejectsInvalidChannelCount(_ count: Int) async {
    do { _ = try await Client.connect(host: "127.0.0.1", port: 1, channelCount: count); Issue.record("Invalid channel count accepted") }
    catch { #expect(error as? ClientError == .invalidConnect) }
}

@Test func clientConcurrentChannelsAndReaderKeepEveryPacket() async throws {
    let server = try TestListener(host: "127.0.0.1")
    let host = Task {
        let connect = try await server.receive()
        let command = try #require(try Datagram.decode(connect.data).commands.first)
        guard case .connect(var parameters, _) = command.body else { throw ClientError.invalidConnect }
        parameters.peerID = 9; parameters.incomingSession = 1; parameters.outgoingSession = 2
        try server.send(Datagram(peerID: 0, sessionID: 1, sentTime: 0,
                                    commands: [.init(sequence: 1, body: .verify(parameters))]).encoded(), to: connect.address)
        var seen: Set<Data> = []
        var sequences: [UInt8: UInt16] = [:]
        while seen.count < 40 {
            let incoming = try await server.receive()
            let datagram = try Datagram.decode(incoming.data)
            for command in datagram.commands {
                guard case .reliable(let payload) = command.body else { continue }
                let time = try #require(datagram.sentTime)
                var reply = [Command(channel: command.channel, body: .acknowledge(sequence: command.sequence, time: time))]
                if seen.insert(payload).inserted {
                    sequences[command.channel, default: 0] += 1
                    reply.append(.init(channel: command.channel, sequence: sequences[command.channel]!, body: .reliable(payload)))
                }
                try server.send(Datagram(peerID: 0, sessionID: 1, sentTime: time, commands: reply).encoded(), to: incoming.address)
            }
        }
    }
    let client = try await Client.connect(host: "127.0.0.1", port: server.port, channelCount: 2)
    let deadline = Task { do { try await Task.sleep(for: .seconds(3)); await client.close() } catch {} }
    defer { deadline.cancel(); host.cancel() }
    func send(_ channel: UInt8) async throws {
        for number in 0..<20 { try await client.send(Data([channel, UInt8(number)]), channelID: channel) }
    }
    async let first: Void = send(0)
    async let second: Void = send(1)
    var received: Set<Data> = []
    for _ in 0..<40 {
        let packet = try #require(try await client.receivePacket())
        #expect(packet.channelID == packet.data.first)
        #expect(received.insert(packet.data).inserted)
    }
    try await first; try await second; try await host.value
    #expect(received.count == 40)
    let waiting = Task { try await client.receivePacket() }
    await client.close(throwing: ClientError.queueFull)
    do { _ = try await waiting.value; Issue.record("Failed close returned normally") }
    catch { #expect(error as? ClientError == .queueFull) }
    await client.close()
    do { _ = try await client.receivePacket(); Issue.record("Repeated close removed the failure") }
    catch { #expect(error as? ClientError == .queueFull) }
}

@Test func oversizedSocketDatagramIsCountedAndNotDelivered() async throws {
    let server = try TestListener(host: "127.0.0.1")
    let socket = try DatagramSocket(host: "127.0.0.1", port: server.port)
    defer { socket.close() }
    try socket.send(Data([0]))
    let address = try await server.receive().address
    let reader = Task {
        var iterator = socketEvents(socket).makeAsyncIterator()
        return try await iterator.next()
    }
    let deadline = Task {
        do { try await Task.sleep(for: .seconds(2)); socket.close() } catch {}
    }
    defer { deadline.cancel(); reader.cancel() }
    try server.send(Data(count: 4097), to: address)
    try server.send(Data([9]), to: address)
    #expect(try await reader.value == [Data([9])])
    #expect(socket.discardedDatagrams == 1)
}
