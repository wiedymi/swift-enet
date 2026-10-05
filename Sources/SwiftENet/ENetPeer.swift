import Foundation

/// A deterministic, single-peer client. Time is monotonic milliseconds supplied
/// by the caller. Sockets and application encryption are outside this type.
struct ENetPeer {
    enum State: Equatable { case connecting, connected, closed(ENetError?) }
    typealias Delivery = ENetDelivery
    static let maximumMessageSize = 1_048_576
    static let maximumQueuedBytes = 4_194_304
    static let timeout: UInt64 = 10_000
    private static let sequenceWindow: UInt16 = 28_672

    struct Output {
        var datagrams: [Data] = []
        var packets: [ENetPacket] = []
    }
    private struct Outgoing {
        // Retain one immutable encoding, including encrypted payload bytes.
        // Retry metadata is the only additional saved state.
        var bytes: Data
        var firstSent: UInt64?
        var lastSent: UInt64?
        var attempts = 0
        var retryDelay: UInt64 = 0
        init(command: ENetCommand) { bytes = command.encoded() }
        var channel: UInt8 { bytes[1] }
        var sequence: UInt16 { UInt16(bytes[2]) << 8 | UInt16(bytes[3]) }
        var number: UInt8 { bytes[0] & 15 }
        var requestsAcknowledgement: Bool { bytes[0] & 128 != 0 }
        var cost: Int {
            switch number {
            case 6: max(1, bytes.count - 6)
            case 7, 9: max(1, bytes.count - 8)
            case 8, 12: max(1, bytes.count - 24)
            default: 1
            }
        }
    }
    private struct Message {
        var data: Data
        var sequences: UInt16 = 1
    }
    private struct FragmentID: Hashable {
        var reliable: Bool
        var dependency: UInt16
        var start: UInt16
    }
    private struct Assembly {
        // Reserve the full message and fragment budget at creation. Each later
        // insertion needs only local validation, not a scan of all channels.
        var data: Data
        var count: Int
        var started: UInt64
        var pieces: [UInt32: Range<Int>] = [:]
        mutating func insert(_ fragment: ENetFragment) -> Bool {
            let offset = Int(fragment.offset), end = offset + fragment.payload.count
            let range = offset..<end
            guard !pieces.values.contains(where: { range.overlaps($0) }) else { return false }
            data.withUnsafeMutableBytes { destination in
                fragment.payload.withUnsafeBytes { source in
                    UnsafeMutableRawBufferPointer(rebasing: destination[range]).copyMemory(from: source)
                }
            }
            pieces[fragment.number] = range
            return true
        }
    }
    private struct Channel {
        var outgoingReliable: UInt16 = 0
        var outgoingUnreliable: UInt16 = 0
        var incomingReliable: UInt16 = 0
        var incomingUnreliable: UInt16 = 0
        var reliable: [UInt16: Message] = [:]
        var unreliable: [UInt16: [UInt16: Data]] = [:]
        var assemblies: [FragmentID: Assembly] = [:]
        var retainedBytes: Int {
            reliable.values.reduce(0) { $0 + $1.data.count } +
            unreliable.values.reduce(0) { $0 + $1.values.reduce(0) { $0 + $1.count } } +
            assemblies.values.reduce(0) { $0 + $1.data.count }
        }
    }

    private(set) var state: State = .connecting
    private(set) var mtu = 900
    private(set) var window = 65_536
    private(set) var remotePeerID: UInt16 = 4095
    private(set) var incomingSession: UInt8 = 255
    private(set) var outgoingSession: UInt8 = 255
    private struct RTTEstimate { var mean: Int; var variance: Int }
    private var rtt: RTTEstimate?
    var roundTripTime: Int { rtt?.mean ?? 500 }
    var roundTripVariance: Int { rtt?.variance ?? 0 }
    var channelCount: Int { channels.count }
    private let connectID: UInt32
    private let started: UInt64
    private var channels: [Channel]
    private var managementSequence: UInt16 = 1
    private var outgoing: [Outgoing] = []
    private var acknowledgements: [ENetCommand] = []
    private var lastSend: UInt64
    private var lastCommunication: UInt64
    private var lossEpoch: UInt64
    private var packetsSent: UInt64 = 0
    private var packetsLost: UInt64 = 0
    private var loss: Double = 0
    private var lossVariance: Double = 0
    private var throttleInterval: UInt64 = 5_000
    private var throttleIncrease = 2
    private var throttleDecrease = 2
    private var throttle = 32
    private var throttleCounter = 0
    private var throttleEpoch: UInt64
    private var baselineRTT = 500
    private var baselineVariance = 0
    private var lowestRTT = 500
    private var highestVariance = 0
    private var incomingBandwidth: UInt32 = 0
    private var bandwidthEpoch: UInt64
    private var bandwidthBytes: UInt64 = 0
    private var unsequencedBase: UInt16 = 0
    private var unsequencedGroups: Set<UInt16> = []

    init(connectID: UInt32, connectData: UInt32, channels: Int = 48, now: UInt64 = 0) {
        precondition((1...255).contains(channels))
        self.connectID = connectID
        self.channels = Array(repeating: Channel(), count: channels)
        started = now; lastSend = now; lastCommunication = now; lossEpoch = now; throttleEpoch = now; bandwidthEpoch = now
        let parameters = ENetConnectParameters(channels: UInt32(channels), connectID: connectID)
        let command = ENetCommand(sequence: 1, body: .connect(parameters, data: connectData))
        outgoing = [.init(command: command)]
    }

    var metrics: ENetMetrics {
        guard state == .connected else { return .init(isConnected: false) }
        return .init(isConnected: true, roundTripTimeMs: roundTripTime, roundTripTimeVarianceMs: roundTripVariance,
                     packetLossRatio: loss, packetLossVarianceRatio: lossVariance)
    }

    private var queuedBytes: Int { outgoing.reduce(0) { $0 + $1.bytes.count } }
    private var retainedBytes: Int { channels.reduce(0) { $0 + $1.retainedBytes } }
    private var retainedEntries: Int {
        channels.reduce(0) { sum, channel in
            sum + channel.reliable.count + channel.unreliable.values.reduce(0) { $0 + $1.count } +
            channel.assemblies.values.reduce(0) { $0 + $1.count }
        }
    }
    private var inFlightBytes: Int {
        outgoing.reduce(0) { $0 + ($1.lastSent != nil && $1.requestsAcknowledgement ? $1.cost : 0) }
    }

    mutating func enqueue(_ data: Data, channel: UInt8, delivery: Delivery) throws {
        guard state == .connected else { throw ENetError.notConnected }
        guard data.count <= Self.maximumMessageSize else { throw ENetError.messageTooLarge }
        let channel = Int(channel) < channelCount ? channel : 0
        let index = Int(channel)
        let fragmentCapacity = mtu - 28
        let count = max(1, (data.count + fragmentCapacity - 1) / fragmentCapacity)
        guard count <= 4096, outgoing.count + count <= 8192, queuedBytes <= Self.maximumQueuedBytes - data.count - count * 24 else {
            throw ENetError.queueFull
        }
        // Never allocate sequences so far ahead that an old ACK can name a new command.
        if let oldest = outgoing.first(where: { $0.channel == channel && $0.requestsAcknowledgement }) {
            let distance = channels[index].outgoingReliable &- oldest.sequence
            guard Int(distance) + count < Int(Self.sequenceWindow) else { throw ENetError.queueFull }
        }
        if count > 1 {
            let start = channels[index].outgoingReliable &+ 1
            for number in 0..<count {
                let offset = number * fragmentCapacity
                let payload = data.subdata(in: (data.startIndex + offset)..<(data.startIndex + min(data.count, offset + fragmentCapacity)))
                let fragment = ENetFragment(start: start, count: UInt32(count), number: UInt32(number),
                                            total: UInt32(data.count), offset: UInt32(offset), payload: payload)
                channels[index].outgoingReliable &+= 1
                append(.init(channel: channel, sequence: channels[index].outgoingReliable, body: .fragment(fragment)))
            }
            channels[index].outgoingUnreliable = 0
        } else if delivery == .reliable || channels[index].outgoingUnreliable == .max {
            channels[index].outgoingReliable &+= 1
            channels[index].outgoingUnreliable = 0
            append(.init(channel: channel, sequence: channels[index].outgoingReliable, body: .reliable(data)))
        } else {
            channels[index].outgoingUnreliable &+= 1
            append(.init(channel: channel, sequence: channels[index].outgoingReliable,
                         body: .unreliable(sequence: channels[index].outgoingUnreliable, payload: data)))
        }
    }

    private mutating func append(_ command: ENetCommand) {
        outgoing.append(.init(command: command))
    }

    mutating func receive(_ data: Data, now: UInt64, flushACKs: Bool = true) -> Output {
        guard case .closed = state else { return receiveActive(data, now: now, flushACKs: flushACKs) }
        return Output()
    }

    private mutating func receiveActive(_ data: Data, now: UInt64, flushACKs: Bool) -> Output {
        // Parse the entire datagram before applying any command.
        guard let datagram = try? ENetDatagram.decode(data), datagram.peerID == 0,
              incomingSession == 255 || datagram.sessionID == incomingSession else { return Output() }
        var output = Output()
        for command in datagram.commands {
            let accepted: Bool
            switch command.body {
            case .verify(let parameters):
                if state == .connecting {
                    guard command.channel == 255, command.requestsAcknowledgement,
                          parameters.connectID == connectID, parameters.peerID < 4095,
                          parameters.incomingSession < 4, parameters.outgoingSession < 4,
                          (1...255).contains(parameters.channels), parameters.throttleInterval == 5000,
                          parameters.throttleIncrease == 2, parameters.throttleDecrease == 2 else {
                        fail(.invalidConnect); return output
                    }
                    remotePeerID = parameters.peerID
                    incomingSession = parameters.incomingSession
                    outgoingSession = parameters.outgoingSession
                    mtu = min(mtu, max(576, min(4096, Int(parameters.mtu))))
                    window = max(4096, min(65_536, Int(parameters.window)))
                    channels = Array(channels.prefix(min(channelCount, Int(parameters.channels))))
                    incomingBandwidth = parameters.incomingBandwidth
                    outgoing.removeAll { $0.channel == 255 && $0.sequence == 1 }
                    state = .connected
                }
                accepted = state == .connected && parameters.connectID == connectID
            case .acknowledge(let sequence, let sentTime):
                acknowledge(channel: command.channel, sequence: sequence, time: sentTime, now: now)
                accepted = true
            case .disconnect:
                accepted = true
            case .connect: accepted = false // This transport is a client.
            case .ping:
                accepted = state == .connected && command.channel == 255
            case .bandwidth(let incoming, _):
                accepted = state == .connected && command.channel == 255
                if accepted {
                    incomingBandwidth = incoming
                    window = incoming == 0 ? 65_536 : max(4096, min(65_536, Int(incoming / 65_536) * 4096))
                }
            case .throttle(let interval, let increase, let decrease):
                accepted = state == .connected && command.channel == 255
                if accepted {
                    throttleInterval = max(1, UInt64(interval))
                    throttleIncrease = min(32, Int(increase)); throttleDecrease = min(32, Int(decrease))
                }
            default:
                accepted = state == .connected && Int(command.channel) < channelCount &&
                    acceptPayload(command, now: now, packets: &output.packets)
            }
            if accepted {
                lastCommunication = now
                if command.requestsAcknowledgement, let time = datagram.sentTime {
                    // A read group can combine ACKs across datagrams. Flush
                    // at 32 commands so retained ACK storage stays bounded.
                    acknowledgements.append(.init(channel: command.channel, sequence: command.sequence,
                                                  body: .acknowledge(sequence: command.sequence, time: time)))
                    if acknowledgements.count == 32 { output.datagrams += flushAcknowledgements(now: now) }
                }
                if case .disconnect = command.body {
                    output.datagrams += flushAcknowledgements(now: now)
                    fail(nil)
                    break
                }
            }
        }
        if flushACKs { output.datagrams += flushAcknowledgements(now: now) }
        return output
    }

    private mutating func acknowledge(channel: UInt8, sequence: UInt16, time: UInt16, now: UInt64) {
        guard let index = outgoing.firstIndex(where: {
            $0.channel == channel && $0.sequence == sequence &&
            $0.requestsAcknowledgement && $0.lastSent != nil
        }) else { return }
        let elapsed = UInt16(truncatingIfNeeded: now) &- time
        // Ignore future timestamps and stale ACKs from an earlier sequence cycle.
        guard elapsed < 32_768, let first = outgoing[index].firstSent,
              UInt64(elapsed) <= now - first else { return }
        let sample = max(1, Int(elapsed))
        if var estimate = rtt {
            if baselineVariance <= baselineRTT / 32 { throttle = 32 }
            else if sample <= baselineRTT { throttle = min(32, throttle + throttleIncrease) }
            else if sample > baselineRTT + 2 * baselineVariance { throttle = max(0, throttle - throttleDecrease) }
            let difference = abs(sample - estimate.mean)
            estimate.variance -= (estimate.variance + 3) / 4
            estimate.variance += (difference + 3) / 4
            estimate.mean += sample >= estimate.mean ? (difference + 7) / 8 : -((difference + 7) / 8)
            estimate.mean = max(1, estimate.mean)
            rtt = estimate
        } else {
            rtt = RTTEstimate(mean: sample, variance: (sample + 1) / 2)
        }
        lowestRTT = min(lowestRTT, roundTripTime)
        highestVariance = max(highestVariance, roundTripVariance)
        if now - throttleEpoch >= throttleInterval {
            baselineRTT = lowestRTT; baselineVariance = max(1, highestVariance)
            lowestRTT = roundTripTime; highestVariance = roundTripVariance; throttleEpoch = now
        }
        outgoing.remove(at: index)
    }

    private func sequenceDistance(_ sequence: UInt16, from base: UInt16) -> UInt16 { sequence &- base }

    private mutating func acceptPayload(_ command: ENetCommand, now: UInt64, packets: inout [ENetPacket]) -> Bool {
        let index = Int(command.channel)
        let distance = sequenceDistance(command.sequence, from: channels[index].incomingReliable)
        switch command.body {
        case .reliable(let payload):
            guard command.requestsAcknowledgement else { return false }
            if distance == 0 || distance >= 32_768 { return true }
            guard distance < Self.sequenceWindow else { return false }
            if channels[index].reliable[command.sequence] != nil { return true }
            guard !overlapsReliable(start: command.sequence, count: 1, channel: index) else { return false }
            if distance == 1 {
                // Ordered payloads need no retained-message entry or global scan.
                channels[index].incomingReliable = command.sequence
                channels[index].incomingUnreliable = 0
                packets.append(.init(data: payload, channelID: UInt8(index)))
                dispatch(channel: index, packets: &packets)
                return true
            }
            guard canRetain(payload.count, channel: index) else { return false }
            channels[index].reliable[command.sequence] = .init(data: payload)
            dispatch(channel: index, packets: &packets)
            return true
        case .unreliable(let sequence, let payload):
            return acceptUnreliable(payload, dependency: command.sequence, sequence: sequence, channel: index, packets: &packets)
        case .fragment(let fragment):
            guard command.requestsAcknowledgement else { return false }
            let startDistance = sequenceDistance(fragment.start, from: channels[index].incomingReliable)
            if startDistance == 0 || startDistance >= 32_768 { return true }
            guard startDistance < Self.sequenceWindow, fragment.count <= UInt32(Self.sequenceWindow) - UInt32(startDistance),
                  command.sequence == fragment.start &+ UInt16(truncatingIfNeeded: fragment.number) else { return false }
            return acceptFragment(fragment, reliable: true, dependency: 0, channel: index, now: now, packets: &packets)
        case .unreliableFragment(let fragment):
            guard distance < Self.sequenceWindow,
                  distance != 0 || fragment.start > channels[index].incomingUnreliable else { return false }
            return acceptFragment(fragment, reliable: false, dependency: command.sequence, channel: index, now: now, packets: &packets)
        case .unsequenced(let group, let payload):
            let distance = group &- unsequencedBase
            if distance >= 32_768 { return true }
            if distance >= 1024 {
                unsequencedBase = group - group % 1024
                unsequencedGroups.removeAll(keepingCapacity: true)
            }
            guard !unsequencedGroups.contains(group), canRetain(payload.count, channel: index) else { return true }
            unsequencedGroups.insert(group); packets.append(.init(data: payload, channelID: UInt8(index)))
            return true
        default: return false
        }
    }

    // One reliable sequence range has one owner, including incomplete fragments.
    // Modular distances handle ranges that cross the 16-bit sequence boundary.
    private func overlapsReliable(start: UInt16, count: UInt16, channel: Int) -> Bool {
        func overlaps(_ other: UInt16, _ length: UInt16) -> Bool {
            start &- other < length || other &- start < count
        }
        return channels[channel].reliable.contains { overlaps($0.key, $0.value.sequences) } ||
            channels[channel].assemblies.contains { $0.key.reliable && overlaps($0.key.start, UInt16($0.value.count)) }
    }

    private func canRetain(_ size: Int, channel: Int, entries: Int = 1) -> Bool {
        size <= Self.maximumMessageSize && retainedEntries <= 8192 - entries && retainedBytes <= Self.maximumQueuedBytes - size &&
        channels[channel].reliable.count + channels[channel].unreliable.values.reduce(0, { $0 + $1.count }) +
        channels[channel].assemblies.count < 4096
    }

    private mutating func acceptUnreliable(_ payload: Data, dependency: UInt16, sequence: UInt16,
                                         channel: Int, packets: inout [ENetPacket]) -> Bool {
        let distance = dependency &- channels[channel].incomingReliable
        guard distance < Self.sequenceWindow else { return false }
        if distance == 0 {
            guard sequence > channels[channel].incomingUnreliable else { return true }
            channels[channel].incomingUnreliable = sequence; packets.append(.init(data: payload, channelID: UInt8(channel)))
        } else {
            if channels[channel].unreliable[dependency]?[sequence] != nil { return true }
            guard canRetain(payload.count, channel: channel) else { return false }
            channels[channel].unreliable[dependency, default: [:]][sequence] = payload
        }
        return true
    }

    private mutating func acceptFragment(_ fragment: ENetFragment, reliable: Bool, dependency: UInt16,
                                       channel: Int, now: UInt64, packets: inout [ENetPacket]) -> Bool {
        guard (1...4096).contains(fragment.count), fragment.number < fragment.count,
              fragment.total > 0, fragment.total <= Self.maximumMessageSize,
              fragment.count <= fragment.total, !fragment.payload.isEmpty,
              fragment.offset < fragment.total, fragment.payload.count <= Int(fragment.total - fragment.offset) else { return false }
        let id = FragmentID(reliable: reliable, dependency: dependency, start: fragment.start)
        if reliable, channels[channel].reliable[fragment.start] != nil { return true }
        if channels[channel].assemblies[id] != nil {
            guard channels[channel].assemblies[id]!.data.count == Int(fragment.total),
                  channels[channel].assemblies[id]!.count == Int(fragment.count) else { return false }
            if let existing = channels[channel].assemblies[id]!.pieces[fragment.number] {
                return existing.lowerBound == Int(fragment.offset) && existing.count == fragment.payload.count &&
                    channels[channel].assemblies[id]!.data[existing] == fragment.payload
            }
            // Mutate through the dictionary's storage. A local Assembly copy
            // would copy its fragment map on each insertion.
            guard channels[channel].assemblies[id]!.insert(fragment) else { return false }
        } else {
            guard (!reliable || !overlapsReliable(start: fragment.start, count: UInt16(fragment.count), channel: channel)),
                  canRetain(Int(fragment.total), channel: channel, entries: Int(fragment.count)) else { return false }
            channels[channel].assemblies[id] = Assembly(data: Data(count: Int(fragment.total)), count: Int(fragment.count), started: now)
            guard channels[channel].assemblies[id]!.insert(fragment) else { return false }
        }
        guard let assembly = channels[channel].assemblies[id], assembly.pieces.count == assembly.count else { return true }
        // Validated ranges never overlap. Equal total bytes therefore cover
        // the message exactly. Move the completed buffer without another copy.
        guard assembly.pieces.values.reduce(0, { $0 + $1.count }) == assembly.data.count else { return false }
        let payload = assembly.data
        channels[channel].assemblies.removeValue(forKey: id)
        if reliable {
            if fragment.start == channels[channel].incomingReliable &+ 1 {
                channels[channel].incomingReliable &+= UInt16(assembly.count)
                channels[channel].incomingUnreliable = 0
                packets.append(.init(data: payload, channelID: UInt8(channel)))
            } else {
                channels[channel].reliable[fragment.start] = .init(data: payload, sequences: UInt16(assembly.count))
            }
            dispatch(channel: channel, packets: &packets)
            return true
        }
        return acceptUnreliable(payload, dependency: dependency, sequence: fragment.start, channel: channel, packets: &packets)
    }

    private mutating func dispatch(channel: Int, packets: inout [ENetPacket]) {
        while let message = channels[channel].reliable.removeValue(forKey: channels[channel].incomingReliable &+ 1) {
            channels[channel].incomingReliable &+= message.sequences
            channels[channel].incomingUnreliable = 0
            packets.append(.init(data: message.data, channelID: UInt8(channel)))
        }
        let base = channels[channel].incomingReliable
        for dependency in Array(channels[channel].unreliable.keys) {
            if dependency == base, let messages = channels[channel].unreliable.removeValue(forKey: dependency) {
                for sequence in messages.keys.sorted() where sequence > channels[channel].incomingUnreliable {
                    if let payload = messages[sequence] { packets.append(.init(data: payload, channelID: UInt8(channel))); channels[channel].incomingUnreliable = sequence }
                }
            } else if dependency &- base >= 32_768 {
                channels[channel].unreliable.removeValue(forKey: dependency)
            }
        }
        for id in Array(channels[channel].assemblies.keys) where !id.reliable && id.dependency &- base >= 32_768 {
            channels[channel].assemblies.removeValue(forKey: id)
        }
    }

    mutating func service(now: UInt64) -> Output {
        guard state == .connecting || state == .connected else { return Output() }
        if state == .connecting && now - started >= Self.timeout { fail(.timedOut); return Output() }
        if now - lastCommunication >= Self.timeout { fail(.timedOut); return Output() }
        for index in channels.indices where !channels[index].assemblies.isEmpty {
            for (id, assembly) in channels[index].assemblies where now - assembly.started >= Self.timeout {
                if id.reliable { fail(.timedOut); return Output() }
                channels[index].assemblies.removeValue(forKey: id)
            }
        }
        if now - lossEpoch >= Self.timeout {
            if packetsSent > 0 {
                let sample = min(1, Double(packetsLost) / Double(packetsSent))
                lossVariance = lossVariance * 0.75 + abs(sample - loss) * 0.25
                loss = loss * 0.875 + sample * 0.125
            }
            packetsSent = 0; packetsLost = 0; lossEpoch = now
        }
        if now - bandwidthEpoch >= 1000 { bandwidthEpoch = now; bandwidthBytes = 0 }
        if state == .connected, now - max(lastCommunication, lastSend) >= 500,
           !outgoing.contains(where: { $0.channel == 255 && $0.number == 5 }) {
            managementSequence &+= 1
            append(.init(sequence: managementSequence, body: .ping))
        }
        var output = Output(datagrams: flushAcknowledgements(now: now))
        var commands: [Data] = []
        var byteCount = 4
        var retainedCount = 0
        var flight = inFlightBytes
        let flightLimit = max(mtu, window * throttle / 32)
        var blockedChannels: Set<UInt8> = []
        func makeDatagram(_ commands: [Data], peer: UInt16, session: UInt8) -> Data {
            var bytes = ENetDatagram(peerID: peer, sessionID: session, sentTime: UInt16(truncatingIfNeeded: now), commands: []).encoded()
            for command in commands { bytes.append(command) }
            return bytes
        }
        for index in outgoing.indices {
            var item = outgoing[index]
            // Compact only when best-effort traffic leaves the queue. Reliable
            // commands keep their existing array storage between ACKs.
            func retain(_ item: Outgoing, changed: Bool = false) {
                if changed || retainedCount != index { outgoing[retainedCount] = item }
                retainedCount += 1
            }
            let isReliable = item.requestsAcknowledgement
            if let first = item.firstSent, now - first >= Self.timeout { fail(.timedOut); return Output() }
            if let sent = item.lastSent, now - sent < item.retryDelay { retain(item); continue }
            if item.lastSent == nil && item.channel != 255 {
                if blockedChannels.contains(item.channel) || (isReliable && flight + item.cost > flightLimit) {
                    blockedChannels.insert(item.channel); retain(item); continue
                }
                if !isReliable {
                    throttleCounter = (throttleCounter + 7) % 32
                    if throttleCounter > throttle || (incomingBandwidth > 0 && bandwidthBytes + UInt64(item.bytes.count) > UInt64(incomingBandwidth)) {
                        continue // Best-effort traffic may be dropped under congestion.
                    }
                }
            }
            if item.lastSent != nil { packetsLost &+= 1 }
            else if isReliable { flight += item.cost }
            if commands.count == 32 || byteCount + item.bytes.count > mtu {
                output.datagrams.append(makeDatagram(commands, peer: remotePeerID, session: outgoingSession)); commands.removeAll(keepingCapacity: true); byteCount = 4
            }
            commands.append(item.bytes); byteCount += item.bytes.count
            lastSend = now
            bandwidthBytes += UInt64(item.bytes.count)
            packetsSent &+= 1
            if item.firstSent == nil { item.firstSent = now }
            item.lastSent = now; item.attempts = min(2, item.attempts + 1)
            let base = min(2000, roundTripTime + min(roundTripTime, 4 * max(1, roundTripVariance)))
            item.retryDelay = UInt64(max(1, base) * item.attempts)
            if isReliable { retain(item, changed: true) }
        }
        outgoing.removeLast(outgoing.count - retainedCount)
        if !commands.isEmpty { output.datagrams.append(makeDatagram(commands, peer: remotePeerID, session: outgoingSession)) }
        return output
    }

    private mutating func flushAcknowledgements(now: UInt64) -> [Data] {
        var datagrams: [Data] = []
        let count = min(32, (mtu - 2) / 8)
        var index = 0
        while index < acknowledgements.count {
            let end = min(acknowledgements.count, index + count)
            datagrams.append(ENetDatagram(peerID: remotePeerID, sessionID: outgoingSession, sentTime: nil,
                                          commands: Array(acknowledgements[index..<end])).encoded())
            index = end
        }
        acknowledgements.removeAll(keepingCapacity: true)
        return datagrams
    }

    var nextServiceTime: UInt64? {
        guard state == .connecting || state == .connected else { return nil }
        var deadline = max(lastCommunication, lastSend) + 500
        if state == .connecting { deadline = min(deadline, started + Self.timeout) }
        let availableFlight = max(mtu, window * throttle / 32) - inFlightBytes
        for command in outgoing {
            if let sent = command.lastSent { deadline = min(deadline, sent + command.retryDelay) }
            else if command.channel == 255 || command.cost <= availableFlight { return 0 }
        }
        return min(deadline, lastCommunication + Self.timeout)
    }

    mutating func close(now: UInt64) -> Output {
        guard state == .connecting || state == .connected else { return Output() }
        managementSequence &+= 1
        let command = ENetCommand(sequence: managementSequence, body: .disconnect(0), requestsAcknowledgement: false)
        let datagram = ENetDatagram(peerID: remotePeerID, sessionID: outgoingSession, sentTime: nil, commands: [command]).encoded()
        fail(nil)
        return Output(datagrams: [datagram])
    }

    private mutating func fail(_ error: ENetError?) {
        state = .closed(error)
        outgoing.removeAll(); acknowledgements.removeAll(); channels.removeAll()
    }
}
