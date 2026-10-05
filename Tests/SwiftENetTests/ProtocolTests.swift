import Foundation
import Testing
@testable import SwiftENet

@Test func codecReadsDataSlicesAndEmptyPayloads() throws {
    let datagram = Datagram(peerID: 0, sessionID: 2, sentTime: 123,
        commands: [.init(channel: 0, sequence: 1, body: .reliable(Data())),
                   .init(channel: 1, sequence: 2, body: .unreliable(sequence: 3, payload: Data([4, 5])))])
    let bytes = datagram.encoded()
    let storage = Data([0xAA, 0xBB]) + bytes + Data([0xCC])
    #expect(try Datagram.decode(storage[2..<(2 + bytes.count)]) == datagram)
}

private func readyPeer(now: UInt64 = 0, channels: Int = 48, window: UInt32 = 65_536) -> Peer {
    var peer = Peer(connectID: 0x11223344, connectData: 0xAABBCCDD, channels: channels, now: now)
    _ = peer.service(now: now)
    let verify = Command(sequence: 1, body: .verify(.init(peerID: 7, incomingSession: 1, outgoingSession: 2,
                                                           window: window, channels: UInt32(channels), connectID: 0x11223344)))
    let output = peer.receive(Datagram(peerID: 0, sessionID: 1, sentTime: UInt16(truncatingIfNeeded: now), commands: [verify]).encoded(), now: now)
    #expect(peer.state == .connected)
    #expect(output.datagrams.count == 1)
    return peer
}

private func inbound(_ commands: [Command], time: UInt16 = 1, session: UInt8 = 1) -> Data {
    Datagram(peerID: 0, sessionID: session, sentTime: time, commands: commands).encoded()
}

@Test func readGroupCombinesACKsAndFlushesAtTheCommandLimit() throws {
    var peer = readyPeer()
    var datagrams: [Data] = []
    for sequence: UInt16 in 1...40 {
        let output = peer.receive(inbound([.init(channel: 0, sequence: sequence, body: .reliable(Data([1])))], time: sequence),
                                  now: UInt64(sequence), flushACKs: false)
        #expect(output.packets.map(\.data) == [Data([1])])
        if sequence < 32 { #expect(output.datagrams.isEmpty) }
        datagrams += output.datagrams
    }
    #expect(datagrams.count == 1)
    datagrams += peer.service(now: 41).datagrams
    #expect(datagrams.count == 2)
    let commands = try datagrams.flatMap { try Datagram.decode($0).commands }
    #expect(commands.count == 40)
    for (index, command) in commands.enumerated() {
        let sequence = UInt16(index + 1)
        #expect(command.channel == 0)
        #expect(command.body == .acknowledge(sequence: sequence, time: sequence))
    }
    _ = peer.receive(inbound([.init(channel: 0, sequence: 41, body: .reliable(Data([2])))], time: 42), now: 42, flushACKs: false)
    let close = peer.receive(inbound([.init(sequence: 2, body: .disconnect(0))], time: 43), now: 43, flushACKs: false)
    #expect(peer.state == .closed(nil))
    #expect(try close.datagrams.flatMap { try Datagram.decode($0).commands }.count == 2)
}

@Test func fragmentsReserveBytesAndEntriesBeforeAcceptingMoreMessages() {
    for (total, count, limit) in [(1_048_576, 1024, 4), (4096, 4096, 2)] {
        var peer = readyPeer()
        let first = Fragment(start: 1, count: UInt32(count), number: 0, total: UInt32(total), offset: 0, payload: Data([1]))
        for channel in 0..<limit {
            let accepted = peer.receive(inbound([.init(channel: UInt8(channel), sequence: 1, body: .fragment(first))]), now: 1)
            #expect(accepted.datagrams.count == 1)
        }
        let rejected = peer.receive(inbound([.init(channel: UInt8(limit), sequence: 1, body: .fragment(first))]), now: 1)
        #expect(rejected.datagrams.isEmpty)
        // Existing reservations must still accept duplicates and new pieces.
        #expect(peer.receive(inbound([.init(channel: 0, sequence: 1, body: .fragment(first))]), now: 2).datagrams.count == 1)
        var second = first
        second.number = 1; second.offset = 1; second.payload = Data([2])
        #expect(peer.receive(inbound([.init(channel: 0, sequence: 2, body: .fragment(second))]), now: 3).datagrams.count == 1)
        #expect(peer.state == .connected)
    }
}

@Test func connectGoldenBytes() throws {
    var peer = Peer(connectID: 0x11223344, connectData: 0xAABBCCDD)
    let packet = try #require(peer.service(now: 0x1234).datagrams.first)
    let expected = try #require(Data(hexString: "8FFF123482FF00010000FFFF000003840001000000000030000000000000000000001388000000020000000211223344AABBCCDD"))
    #expect(packet == expected)
    #expect(try Datagram.decode(packet).encoded() == packet)
}

@Test func codecCoversEveryCommandAndRejectsTruncation() throws {
    let fragment = Fragment(start: 1, count: 1, number: 0, total: 2, offset: 0, payload: Data([1, 2]))
    let bodies: [CommandBody] = [.acknowledge(sequence: 3, time: 4), .connect(.init(connectID: 9), data: 8),
        .verify(.init(connectID: 9)), .disconnect(7), .ping, .reliable(Data([1, 2])), .unreliable(sequence: 3, payload: Data([1, 2])),
        .fragment(fragment), .unsequenced(group: 2, payload: Data([1, 2])), .bandwidth(incoming: 4, outgoing: 5),
        .throttle(interval: 5, increase: 2, decrease: 2), .unreliableFragment(fragment)]
    for body in bodies {
        let datagram = Datagram(peerID: 1, sessionID: 3, sentTime: 2, commands: [.init(body: body)])
        let bytes = datagram.encoded()
        #expect(try Datagram.decode(bytes) == datagram)
        for count in 0..<bytes.count {
            #expect(throws: ClientError.self) { try Datagram.decode(Data(bytes.prefix(count))) }
        }
    }
}

@Test func validatesConnectAndNegotiatesChannels() throws {
    var peer = Peer(connectID: 10, connectData: 20)
    _ = peer.service(now: 0)
    let invalid = Command(sequence: 1, body: .verify(.init(peerID: 1, incomingSession: 1, outgoingSession: 2, connectID: 11)))
    _ = peer.receive(inbound([invalid]), now: 1)
    #expect(peer.state == .closed(.invalidConnect))
    var limited = readyPeer(channels: 1)
    try limited.enqueue(Data([3]), channel: 47, delivery: .reliable)
    let packet = try #require(limited.service(now: 2).datagrams.first)
    #expect(try Datagram.decode(packet).commands.first?.channel == 0)
}

@Test func reliableOrderingDuplicatesAndChannelIndependence() throws {
    var peer = readyPeer()
    let two = Command(channel: 0, sequence: 2, body: .reliable(Data([2])))
    let one = Command(channel: 0, sequence: 1, body: .reliable(Data([1])))
    #expect(peer.receive(inbound([two]), now: 2).packets.isEmpty)
    let other = Command(channel: 1, sequence: 1, body: .reliable(Data([9])))
    #expect(peer.receive(inbound([other]), now: 3).packets.map(\.data) == [Data([9])])
    #expect(peer.receive(inbound([one]), now: 4).packets.map(\.data) == [Data([1]), Data([2])])
    let duplicate = peer.receive(inbound([one, two]), now: 5)
    #expect(duplicate.packets.isEmpty)
    #expect(try Datagram.decode(try #require(duplicate.datagrams.first)).commands.count == 2)
    #expect(peer.receive(inbound([Command(channel: 0, sequence: 3, body: .reliable(Data([3])))], session: 2), now: 6).packets.isEmpty)
}

@Test func lostAckRetriesIdenticalBytesAndStopsAfterAck() throws {
    var peer = readyPeer()
    try peer.enqueue(Data([1, 2]), channel: 3, delivery: .reliable)
    let initial = try #require(peer.service(now: 1).datagrams.first)
    _ = peer.receive(inbound([.init(body: .ping)]), now: 500)
    #expect(peer.service(now: 504).datagrams.isEmpty)
    let retry = try #require(peer.service(now: 505).datagrams.first)
    #expect(try Datagram.decode(initial).commands == Datagram.decode(retry).commands)
    let ack = Command(channel: 3, body: .acknowledge(sequence: 1, time: 505))
    _ = peer.receive(inbound([ack]), now: 510)
    #expect(peer.metrics.roundTripTimeMs == 5)
    #expect(peer.service(now: 511).datagrams.isEmpty)
    #expect(peer.nextServiceTime == 1010)
}

@Test func unreliableDependsOnReliableAndRejectsOldSequence() {
    var peer = readyPeer()
    let future = Command(channel: 0, sequence: 1, body: .unreliable(sequence: 2, payload: Data([2])))
    #expect(peer.receive(inbound([future]), now: 1).packets.isEmpty)
    let reliable = Command(channel: 0, sequence: 1, body: .reliable(Data([1])))
    #expect(peer.receive(inbound([reliable]), now: 2).packets.map(\.data) == [Data([1]), Data([2])])
    #expect(peer.receive(inbound([future]), now: 3).packets.isEmpty)
}

@Test func fragmentedMessageReassemblesOutOfOrderOnce() throws {
    var sender = readyPeer(), receiver = readyPeer()
    let payload = Data((0..<10_000).map { UInt8(truncatingIfNeeded: $0) })
    try sender.enqueue(payload, channel: 6, delivery: .unreliable)
    let datagrams = sender.service(now: 1).datagrams
    #expect(datagrams.count > 1)
    var received: [Data] = []
    for bytes in datagrams.reversed() {
        var packet = try Datagram.decode(bytes); packet.peerID = 0; packet.sessionID = 1
        let encoded = packet.encoded()
        received += receiver.receive(encoded, now: 2).packets.map(\.data)
        #expect(receiver.receive(encoded, now: 3).packets.isEmpty)
    }
    #expect(received == [payload])
}

@Test func rejectsFragmentBoundsOverlapAndIncompleteTimeout() throws {
    var peer = readyPeer()
    let first = Fragment(start: 1, count: 2, number: 0, total: 4, offset: 0, payload: Data([1, 2]))
    let invalid = Fragment(start: 1, count: 2, number: 1, total: 4, offset: 1, payload: Data([3, 4]))
    #expect(!peer.receive(inbound([.init(channel: 0, sequence: 1, body: .fragment(first))]), now: 1).datagrams.isEmpty)
    #expect(peer.receive(inbound([.init(channel: 0, sequence: 2, body: .fragment(invalid))]), now: 2).datagrams.isEmpty)
    let ping = Command(body: .ping)
    _ = peer.receive(inbound([ping]), now: 9_000)
    _ = peer.service(now: 10_001)
    #expect(peer.state == .closed(.timedOut))
}

@Test func unreliableFragmentsAndUnsequencedDuplicates() {
    var peer = readyPeer()
    let first = Fragment(start: 1, count: 2, number: 0, total: 4, offset: 0, payload: Data([1, 2]))
    let last = Fragment(start: 1, count: 2, number: 1, total: 4, offset: 2, payload: Data([3, 4]))
    #expect(peer.receive(inbound([.init(channel: 0, body: .unreliableFragment(last))]), now: 1).packets.isEmpty)
    #expect(peer.receive(inbound([.init(channel: 0, body: .unreliableFragment(first))]), now: 2).packets.map(\.data) == [Data([1, 2, 3, 4])])
    let unsequenced = Command(channel: 0, body: .unsequenced(group: 1, payload: Data([5])))
    #expect(peer.receive(inbound([unsequenced]), now: 3).packets.map(\.data) == [Data([5])])
    #expect(peer.receive(inbound([unsequenced]), now: 4).packets.isEmpty)
}

@Test func malformedTrailingCommandDoesNotAcknowledgePrefix() {
    var peer = readyPeer()
    let valid = inbound([.init(channel: 0, sequence: 1, body: .reliable(Data([1])))])
    #expect(peer.receive(valid + Data([0xFF]), now: 1).packets.isEmpty)
    #expect(peer.receive(valid, now: 2).packets.map(\.data) == [Data([1])])
}

@Test func queueAndMessageLimits() throws {
    var peer = readyPeer()
    #expect(throws: ClientError.messageTooLarge) { try peer.enqueue(Data(count: Peer.maximumMessageSize + 1), channel: 0, delivery: .reliable) }
    for _ in 0..<3 { try peer.enqueue(Data(count: Peer.maximumMessageSize), channel: 0, delivery: .reliable) }
    #expect(throws: ClientError.queueFull) { try peer.enqueue(Data(count: Peer.maximumMessageSize), channel: 0, delivery: .reliable) }
    let output = peer.service(now: 1)
    #expect(output.datagrams.allSatisfy { $0.count <= peer.mtu })
}

@Test func reliableSequenceAndTimestampWrap() throws {
    var peer = readyPeer(now: 65_530)
    for value in 1...65_540 {
        let sequence = UInt16(truncatingIfNeeded: value)
        let command = Command(channel: 0, sequence: sequence, body: .reliable(Data([UInt8(truncatingIfNeeded: value)])))
        #expect(peer.receive(inbound([command]), now: UInt64(65_530 + value)).packets.count == 1)
    }
    try peer.enqueue(Data([1]), channel: 1, delivery: .reliable)
    _ = peer.service(now: 131_070)
    let ack = Command(channel: 1, body: .acknowledge(sequence: 1, time: 65_534))
    _ = peer.receive(inbound([ack]), now: 131_075)
    #expect(peer.metrics.roundTripTimeMs == 5)
}

@Test func disconnectAcknowledgesAndIsIdempotent() {
    var peer = readyPeer()
    let result = peer.receive(inbound([.init(sequence: 2, body: .disconnect(7))]), now: 1)
    #expect(result.datagrams.count == 1)
    #expect(peer.state == .closed(nil))
    #expect(peer.close(now: 2).datagrams.isEmpty)
}

@Test func outboundSequenceWrapAndDuplicateAcks() throws {
    var peer = readyPeer()
    for value in 1...65_540 {
        let now = UInt64(value)
        try peer.enqueue(Data([1]), channel: 2, delivery: .reliable)
        let packet = try #require(peer.service(now: now).datagrams.first)
        let commands = try Datagram.decode(packet).commands
        let command = try #require(commands.first { $0.channel == 2 })
        #expect(command.sequence == UInt16(truncatingIfNeeded: value))
        let ack = Command(channel: 2, body: .acknowledge(sequence: command.sequence, time: UInt16(truncatingIfNeeded: now)))
        _ = peer.receive(inbound([ack]), now: now)
        _ = peer.receive(inbound([ack]), now: now)
    }
    #expect(peer.state == .connected)
}

@Test func windowAllowsOtherChannelsAfterOneChannelBlocks() throws {
    var peer = readyPeer()
    try peer.enqueue(Data(count: 100_000), channel: 0, delivery: .reliable)
    try peer.enqueue(Data([9]), channel: 1, delivery: .reliable)
    let first = peer.service(now: 1)
    let initial = try first.datagrams.flatMap { try Datagram.decode($0).commands }
    // Payload byte limits are shared, but management commands must still work.
    #expect(initial.filter { $0.channel == 0 }.reduce(0) { $0 + $1.body.payloadSize } <= 65_536)
    #expect(initial.contains { $0.channel == 1 })
    var acks = initial.filter(\.requestsAcknowledgement).map {
        Command(channel: $0.channel, body: .acknowledge(sequence: $0.sequence, time: 1))
    }
    while !acks.isEmpty {
        let batch = Array(acks.prefix(32)); acks.removeFirst(batch.count)
        _ = peer.receive(inbound(batch), now: 2)
    }
    #expect(!peer.service(now: 2).datagrams.isEmpty)
}

@Test func parserRejectsFlagsAndRandomMalformedBytesWithoutStateChange() {
    var peer = readyPeer()
    var seed: UInt64 = 0xBADC0FFEE
    for _ in 0..<10_000 {
        seed = seed &* 6364136223846793005 &+ 1
        let length = Int(seed % 96)
        var bytes = Data()
        for _ in 0..<length {
            seed = seed &* 6364136223846793005 &+ 1
            bytes.append(UInt8(truncatingIfNeeded: seed >> 32))
        }
        _ = peer.receive(bytes, now: 1)
    }
    #expect(peer.state == .connected)
    var invalid = inbound([.init(channel: 0, sequence: 1, body: .reliable(Data([1])))])
    invalid[4] = 6 // Reliable data cannot omit its acknowledgement flag.
    #expect(peer.receive(invalid, now: 2).datagrams.isEmpty)
    #expect(peer.receive(inbound([.init(channel: 0, sequence: 1, body: .reliable(Data([1])))]), now: 3).packets.map(\.data) == [Data([1])])
}

@Test func retainsReliableCommandsUntilTimeoutDespiteOtherTraffic() throws {
    var peer = readyPeer()
    try peer.enqueue(Data([1]), channel: 0, delivery: .reliable)
    _ = peer.service(now: 1)
    _ = peer.receive(inbound([.init(body: .ping)]), now: 9_999)
    _ = peer.service(now: 10_001)
    #expect(peer.state == .closed(.timedOut))
}

@Test func lossMetricsRefreshAfterAcknowledgedRetry() throws {
    var peer = readyPeer()
    try peer.enqueue(Data([1]), channel: 3, delivery: .reliable)
    _ = peer.service(now: 1)
    _ = peer.service(now: 505)
    _ = peer.receive(inbound([.init(channel: 3, body: .acknowledge(sequence: 1, time: 505))]), now: 510)
    _ = peer.receive(inbound([.init(body: .ping)]), now: 10_000)
    _ = peer.service(now: 10_000)
    #expect(try #require(peer.metrics.packetLossRatio) > 0)
    #expect(try #require(peer.metrics.packetLossRatio) <= 1)
    #expect(try #require(peer.metrics.packetLossVarianceRatio) > 0)
}

@Test func unreliableSequenceExhaustionUsesReliableDelivery() throws {
    var peer = readyPeer()
    for value in 1...65_535 {
        try peer.enqueue(Data([1]), channel: 2, delivery: .unreliable)
        let packet = try #require(peer.service(now: 1).datagrams.first)
        let command = try #require(try Datagram.decode(packet).commands.first)
        #expect(command.body == .unreliable(sequence: UInt16(value), payload: Data([1])))
    }
    try peer.enqueue(Data([2]), channel: 2, delivery: .unreliable)
    let packet = try #require(peer.service(now: 1).datagrams.first)
    let command = try #require(try Datagram.decode(packet).commands.first)
    #expect(command.body == .reliable(Data([2])))
    #expect(command.sequence == 1)
    let old = Command(channel: 1, body: .unreliable(sequence: 65_535, payload: Data([1])))
    #expect(peer.receive(inbound([old]), now: 1).packets.map(\.data) == [Data([1])])
    let reliable = Command(channel: 1, sequence: 1, body: .reliable(Data([2])))
    #expect(peer.receive(inbound([reliable]), now: 1).packets.map(\.data) == [Data([2])])
    let next = Command(channel: 1, sequence: 1, body: .unreliable(sequence: 1, payload: Data([3])))
    #expect(peer.receive(inbound([next]), now: 1).packets.map(\.data) == [Data([3])])
}

private extension Data {
    init?(hexString: String) {
        guard hexString.count % 2 == 0 else { return nil }
        var bytes: [UInt8] = []
        var index = hexString.startIndex
        while index < hexString.endIndex {
            let next = hexString.index(index, offsetBy: 2)
            guard let byte = UInt8(hexString[index..<next], radix: 16) else { return nil }
            bytes.append(byte); index = next
        }
        self.init(bytes)
    }
}

@Test func conflictingReliableRangesAreRejectedWithoutLosingValidData() {
    var peer = readyPeer()
    let first = Fragment(start: 1, count: 2, number: 0, total: 2, offset: 0, payload: Data([1]))
    #expect(peer.receive(inbound([.init(channel: 0, sequence: 1, body: .fragment(first))]), now: 1).datagrams.count == 1)
    // A normal message cannot occupy an incomplete fragment's sequence range.
    let conflict = Command(channel: 0, sequence: 1, body: .reliable(Data([9])))
    #expect(peer.receive(inbound([conflict]), now: 2).datagrams.isEmpty)
    let overlapping = Fragment(start: 2, count: 2, number: 0, total: 2, offset: 0, payload: Data([9]))
    #expect(peer.receive(inbound([.init(channel: 0, sequence: 2, body: .fragment(overlapping))]), now: 3).datagrams.isEmpty)
    var last = first; last.number = 1; last.offset = 1; last.payload = Data([2])
    #expect(peer.receive(inbound([.init(channel: 0, sequence: 2, body: .fragment(last))]), now: 4).packets.map(\.data) == [Data([1, 2])])
    let next = Command(channel: 0, sequence: 3, body: .reliable(Data([3])))
    #expect(peer.receive(inbound([next]), now: 5).packets.map(\.data) == [Data([3])])

    var buffered = readyPeer()
    let ahead = Command(channel: 0, sequence: 2, body: .reliable(Data([2])))
    #expect(buffered.receive(inbound([ahead]), now: 1).datagrams.count == 1)
    #expect(buffered.receive(inbound([.init(channel: 0, sequence: 1, body: .fragment(first))]), now: 2).datagrams.isEmpty)
    #expect(buffered.receive(inbound([.init(channel: 0, sequence: 1, body: .reliable(Data([1])))]), now: 3).packets.map(\.data) == [Data([1]), Data([2])])
}

@Test func blockedChannelDoesNotScheduleEmptyTimerWork() throws {
    var peer = readyPeer(channels: 2, window: 4096)
    for _ in 0..<4 { try peer.enqueue(Data(count: 872), channel: 0, delivery: .reliable) }
    try peer.enqueue(Data(count: 576), channel: 0, delivery: .reliable)
    _ = peer.service(now: 10)
    try peer.enqueue(Data(count: 64), channel: 0, delivery: .reliable)
    try peer.enqueue(Data(count: 16), channel: 0, delivery: .unreliable)
    #expect(peer.nextServiceTime == 510)
    for now in UInt64(11)..<510 {
        #expect(peer.service(now: now).datagrams.isEmpty)
        #expect(peer.nextServiceTime == 510)
    }
    // An ACK permits both queued messages without waiting for a timer.
    _ = peer.receive(inbound([.init(channel: 0, body: .acknowledge(sequence: 1, time: 10))]), now: 20)
    #expect(peer.nextServiceTime == 0)
    let commands = try peer.service(now: 20).datagrams.flatMap { try Datagram.decode($0).commands }
    #expect(commands.contains { $0.channel == 0 && !$0.requestsAcknowledgement })
}

@Test func bestEffortCanSendWithFullReliableWindowOnOtherChannel() throws {
    var peer = readyPeer(channels: 2, window: 4096)
    for _ in 0..<4 { try peer.enqueue(Data(count: 872), channel: 0, delivery: .reliable) }
    try peer.enqueue(Data(count: 608), channel: 0, delivery: .reliable)
    _ = peer.service(now: 10)
    try peer.enqueue(Data(count: 128), channel: 1, delivery: .unreliable)
    #expect(peer.nextServiceTime == 0)
    #expect(!peer.service(now: 11).datagrams.isEmpty)
    #expect(peer.nextServiceTime != 0)
}

@Test func unacknowledgedPingDoesNotSchedulePastIdleDeadline() {
    var peer = readyPeer()
    #expect(!peer.service(now: 500).datagrams.isEmpty)
    #expect(!peer.service(now: 1004).datagrams.isEmpty)
    #expect(peer.service(now: 1504).datagrams.isEmpty)
    #expect(peer.nextServiceTime == 2012)
}

@Test func sendByteCountersFollowQueueSendAndAcknowledgement() throws {
    var peer = readyPeer(channels: 2)
    #expect(peer.metrics.queuedSendBytes == 0)
    #expect(peer.metrics.inFlightSendBytes == 0)
    for index in 1...200 {
        try peer.enqueue(Data(count: 100), channel: 0, delivery: .reliable)
        try peer.enqueue(Data(count: 50), channel: 1, delivery: .unreliable)
        #expect(peer.metrics.queuedSendBytes == 106 + 58)
        _ = peer.service(now: UInt64(index * 10))
        #expect(peer.metrics.queuedSendBytes == 106)
        #expect(peer.metrics.inFlightSendBytes == 100)
        _ = peer.receive(inbound([.init(channel: 0, body: .acknowledge(sequence: UInt16(index), time: UInt16(index * 10)))]), now: UInt64(index * 10 + 1))
        #expect(peer.metrics.queuedSendBytes == 0)
        #expect(peer.metrics.inFlightSendBytes == 0)
    }
}

@Test func metricsDecodeOlderSnapshots() throws {
    let metrics = try JSONDecoder().decode(Metrics.self, from: Data("{\"isConnected\":true}".utf8))
    #expect(metrics.queuedSendBytes == nil)
    #expect(metrics.discardedSocketDatagrams == nil)
}
