public enum ClientError: Error, Equatable, Sendable {
    case invalidPacket
    case invalidConnect
    case notConnected
    case queueFull
    case messageTooLarge
    case timedOut
    case closed
    case invalidChannel
    case packetReaderInUse
    case socketFailure(code: Int32)
}
