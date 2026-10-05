import Foundation

// ENet numeric fields are big endian. Application framing is separate.
struct ENetConnectParameters: Equatable, Sendable {
    var peerID: UInt16 = 0
    var incomingSession: UInt8 = 255
    var outgoingSession: UInt8 = 255
    var mtu: UInt32 = 900
    var window: UInt32 = 65_536
    var channels: UInt32 = 48
    var incomingBandwidth: UInt32 = 0
    var outgoingBandwidth: UInt32 = 0
    var throttleInterval: UInt32 = 5_000
    var throttleIncrease: UInt32 = 2
    var throttleDecrease: UInt32 = 2
    var connectID: UInt32
}

struct ENetFragment: Equatable, Sendable {
    var start: UInt16
    var count: UInt32
    var number: UInt32
    var total: UInt32
    var offset: UInt32
    var payload: Data
}

enum ENetCommandBody: Equatable, Sendable {
    case acknowledge(sequence: UInt16, time: UInt16)
    case connect(ENetConnectParameters, data: UInt32)
    case verify(ENetConnectParameters)
    case disconnect(UInt32)
    case ping
    case reliable(Data)
    case unreliable(sequence: UInt16, payload: Data)
    case fragment(ENetFragment)
    case unsequenced(group: UInt16, payload: Data)
    case bandwidth(incoming: UInt32, outgoing: UInt32)
    case throttle(interval: UInt32, increase: UInt32, decrease: UInt32)
    case unreliableFragment(ENetFragment)

    var number: UInt8 {
        switch self {
        case .acknowledge: 1
        case .connect: 2
        case .verify: 3
        case .disconnect: 4
        case .ping: 5
        case .reliable: 6
        case .unreliable: 7
        case .fragment: 8
        case .unsequenced: 9
        case .bandwidth: 10
        case .throttle: 11
        case .unreliableFragment: 12
        }
    }

    var payloadSize: Int {
        switch self {
        case .reliable(let data), .unreliable(_, let data), .unsequenced(_, let data): data.count
        case .fragment(let fragment), .unreliableFragment(let fragment): fragment.payload.count
        default: 0
        }
    }
}

struct ENetCommand: Equatable, Sendable {
    var channel: UInt8
    var sequence: UInt16
    var body: ENetCommandBody
    var requestsAcknowledgement: Bool

    init(channel: UInt8 = 255, sequence: UInt16 = 0, body: ENetCommandBody, requestsAcknowledgement: Bool? = nil) {
        self.channel = channel
        self.sequence = sequence
        self.body = body
        self.requestsAcknowledgement = requestsAcknowledgement ?? [2, 3, 4, 5, 6, 8, 10, 11].contains(body.number)
    }

    fileprivate var encodedSize: Int {
        let header: Int
        switch body {
        case .acknowledge, .disconnect, .unreliable, .unsequenced: header = 8
        case .connect: header = 48
        case .verify: header = 44
        case .ping: header = 4
        case .reliable: header = 6
        case .fragment, .unreliableFragment: header = 24
        case .bandwidth: header = 12
        case .throttle: header = 16
        }
        return header + body.payloadSize
    }

    func encoded() -> Data {
        var bytes = Data(count: encodedSize)
        bytes.withUnsafeMutableBytes { buffer in
            var writer = ENetWriter(bytes: buffer)
            encode(into: &writer)
        }
        return bytes
    }

    fileprivate func encode(into writer: inout ENetWriter) {
        writer.u8(body.number | (requestsAcknowledgement ? 0x80 : 0) | (body.number == 9 || (body.number == 4 && !requestsAcknowledgement) ? 0x40 : 0))
        writer.u8(channel); writer.u16(sequence)
        switch body {
        case .acknowledge(let sequence, let time):
            writer.u16(sequence); writer.u16(time)
        case .connect(let parameters, let data):
            writer.parameters(parameters); writer.u32(data)
        case .verify(let parameters): writer.parameters(parameters)
        case .disconnect(let reason): writer.u32(reason)
        case .ping: break
        case .reliable(let payload):
            writer.u16(UInt16(payload.count)); writer.payload(payload)
        case .unreliable(let sequence, let payload), .unsequenced(let sequence, let payload):
            writer.u16(sequence); writer.u16(UInt16(payload.count)); writer.payload(payload)
        case .fragment(let fragment), .unreliableFragment(let fragment):
            writer.u16(fragment.start); writer.u16(UInt16(fragment.payload.count))
            writer.u32(fragment.count); writer.u32(fragment.number)
            writer.u32(fragment.total); writer.u32(fragment.offset); writer.payload(fragment.payload)
        case .bandwidth(let incoming, let outgoing):
            writer.u32(incoming); writer.u32(outgoing)
        case .throttle(let interval, let increase, let decrease):
            writer.u32(interval); writer.u32(increase); writer.u32(decrease)
        }
    }
}

struct ENetDatagram: Equatable, Sendable {
    var peerID: UInt16
    var sessionID: UInt8
    var sentTime: UInt16?
    var commands: [ENetCommand]

    func encoded() -> Data {
        let size = (sentTime == nil ? 2 : 4) + commands.reduce(0) { $0 + $1.encodedSize }
        var bytes = Data(count: size)
        bytes.withUnsafeMutableBytes { buffer in
            var writer = ENetWriter(bytes: buffer)
            let session = peerID == 4095 ? UInt16(0) : UInt16(sessionID & 3) << 12
            writer.u16(peerID | session | (sentTime == nil ? 0 : 0x8000))
            if let sentTime { writer.u16(sentTime) }
            for command in commands { command.encode(into: &writer) }
        }
        return bytes
    }

    static func decode(_ bytes: Data) throws -> Self {
        guard bytes.count <= 4096 else { throw ENetError.invalidPacket }
        return try bytes.withUnsafeBytes { buffer in
            var reader = ENetReader(bytes: buffer)
            let header = try reader.u16()
            guard header & 0x4000 == 0 else { throw ENetError.invalidPacket }
            let time: UInt16? = header & 0x8000 != 0 ? try reader.u16() : nil
            var commands: [ENetCommand] = []
            while !reader.atEnd {
                guard commands.count < 32 else { throw ENetError.invalidPacket }
                let flags = try reader.u8()
                guard flags & 0x30 == 0 else { throw ENetError.invalidPacket }
                let channel = try reader.u8()
                let sequence = try reader.u16()
                let body: ENetCommandBody
                switch flags & 0x0F {
                case 1: body = .acknowledge(sequence: try reader.u16(), time: try reader.u16())
                case 2: body = .connect(try reader.parameters(), data: try reader.u32())
                case 3: body = .verify(try reader.parameters())
                case 4: body = .disconnect(try reader.u32())
                case 5: body = .ping
                case 6: body = .reliable(try reader.payload(length: Int(reader.u16())))
                case 7:
                    let number = try reader.u16()
                    body = .unreliable(sequence: number, payload: try reader.payload(length: Int(reader.u16())))
                case 8: body = .fragment(try reader.fragment())
                case 9:
                    let group = try reader.u16()
                    body = .unsequenced(group: group, payload: try reader.payload(length: Int(reader.u16())))
                case 10: body = .bandwidth(incoming: try reader.u32(), outgoing: try reader.u32())
                case 11: body = .throttle(interval: try reader.u32(), increase: try reader.u32(), decrease: try reader.u32())
                case 12: body = .unreliableFragment(try reader.fragment())
                default: throw ENetError.invalidPacket
                }
                let acknowledged = flags & 0x80 != 0
                let expectedFlags: UInt8
                switch body.number {
                case 2, 3, 5, 6, 8, 10, 11: expectedFlags = 0x80
                case 9: expectedFlags = 0x40
                case 4: expectedFlags = acknowledged ? 0x80 : 0x40
                default: expectedFlags = 0
                }
                guard flags & 0xC0 == expectedFlags, !acknowledged || time != nil else {
                    throw ENetError.invalidPacket
                }
                commands.append(.init(channel: channel, sequence: sequence, body: body, requestsAcknowledgement: acknowledged))
            }
            guard !commands.isEmpty else { throw ENetError.invalidPacket }
            return .init(peerID: header & 0x0FFF, sessionID: UInt8((header >> 12) & 3), sentTime: time, commands: commands)
        }
    }
}

private struct ENetReader {
    let bytes: UnsafeRawBufferPointer
    var offset = 0
    var atEnd: Bool { offset == bytes.count }
    mutating func u8() throws -> UInt8 {
        guard offset < bytes.count else { throw ENetError.invalidPacket }
        defer { offset += 1 }
        return bytes[offset]
    }
    mutating func u16() throws -> UInt16 { (UInt16(try u8()) << 8) | UInt16(try u8()) }
    mutating func u32() throws -> UInt32 { (UInt32(try u16()) << 16) | UInt32(try u16()) }
    mutating func payload(length: Int) throws -> Data {
        guard length >= 0, length <= bytes.count - offset else { throw ENetError.invalidPacket }
        defer { offset += length }
        return Data(bytes: bytes.baseAddress!.advanced(by: offset), count: length)
    }
    mutating func parameters() throws -> ENetConnectParameters {
        .init(peerID: try u16(), incomingSession: try u8(), outgoingSession: try u8(),
              mtu: try u32(), window: try u32(), channels: try u32(), incomingBandwidth: try u32(),
              outgoingBandwidth: try u32(), throttleInterval: try u32(), throttleIncrease: try u32(),
              throttleDecrease: try u32(), connectID: try u32())
    }
    mutating func fragment() throws -> ENetFragment {
        let start = try u16(), length = try u16()
        let count = try u32(), number = try u32(), total = try u32(), offset = try u32()
        return .init(start: start, count: count, number: number, total: total, offset: offset,
                     payload: try payload(length: Int(length)))
    }
}

// Buffers exist only within Data's scoped byte access. Encoding allocates the
// exact complete size once. Byte stores do not require integer alignment.
private struct ENetWriter {
    let bytes: UnsafeMutableRawBufferPointer
    var offset = 0
    mutating func u8(_ value: UInt8) { bytes[offset] = value; offset += 1 }
    mutating func u16(_ value: UInt16) {
        u8(UInt8(truncatingIfNeeded: value >> 8)); u8(UInt8(truncatingIfNeeded: value))
    }
    mutating func u32(_ value: UInt32) {
        u16(UInt16(truncatingIfNeeded: value >> 16)); u16(UInt16(truncatingIfNeeded: value))
    }
    mutating func payload(_ data: Data) {
        data.withUnsafeBytes { source in
            UnsafeMutableRawBufferPointer(rebasing: bytes[offset..<(offset + source.count)]).copyMemory(from: source)
        }
        offset += data.count
    }
    mutating func parameters(_ value: ENetConnectParameters) {
        u16(value.peerID); u8(value.incomingSession); u8(value.outgoingSession)
        u32(value.mtu); u32(value.window); u32(value.channels)
        u32(value.incomingBandwidth); u32(value.outgoingBandwidth)
        u32(value.throttleInterval); u32(value.throttleIncrease); u32(value.throttleDecrease); u32(value.connectID)
    }
}
