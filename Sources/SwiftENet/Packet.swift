import Foundation

/// Reliable delivery retries until ACK or timeout. Unreliable delivery can drop.
public enum Delivery: Sendable { case reliable, unreliable }

/// A received application payload and its negotiated channel.
public struct Packet: Equatable, Sendable {
    public let data: Data
    public let channelID: UInt8

    public init(data: Data, channelID: UInt8) {
        self.data = data
        self.channelID = channelID
    }
}
