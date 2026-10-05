import Foundation

/// Reliable delivery retries until ACK or timeout. Unreliable delivery can drop.
public enum ENetDelivery: Sendable { case reliable, unreliable }

/// A received application payload and its negotiated channel.
public struct ENetPacket: Equatable, Sendable {
    public let data: Data
    public let channelID: UInt8

    public init(data: Data, channelID: UInt8) {
        self.data = data
        self.channelID = channelID
    }
}
