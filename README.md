# swift-enet

A Swift 6 ENet UDP client for Apple platforms. No CENet dependency.

SwiftENet provides reliable and unreliable messages, separate ordered channels,
fragment assembly, ACKs, retries, and connection metrics. Native socket reads
and a reusable retry timer run without application polling. The client actor
uses a native serial executor. Socket buffers have compiler-checked `Sendable`
conformance and use Apple's typed lock.

**Status:** initial client release. Tested against ENet 1.3.18 and the Moonlight
ENet fork. Live Sunshine/Apollo and VVMoon integration tests remain separate.
This is a bounded client implementation, not a complete ENet server or C API replacement.

## Requirements

- Swift 6.0 or newer, with complete strict concurrency checks.
- macOS 14, iOS 17, tvOS 17, or visionOS 1 or newer.
- A UDP host that uses the ENet wire protocol.

Linux and Windows are not supported. The package uses Apple Dispatch and Darwin sockets.

## Add the package

```swift
.package(url: "https://github.com/wiedymi/swift-enet.git", from: "0.2.0")
```

Add `.product(name: "SwiftENet", package: "swift-enet")` to your target dependencies.

## Use the client

```swift
import Foundation
import SwiftENet

let client = try await Client.connect(
    host: "127.0.0.1", port: 47999, connectData: 0, channelCount: 4
)
try await client.send(Data([1, 2, 3]), channelID: 0, delivery: .reliable)
if let packet = try await client.receivePacket() {
    print(packet.channelID, packet.data.count)
}
let metrics = await client.snapshotMetrics()
await client.close()
```

`connect` completes the ENet handshake and supports cancellation. The host can
negotiate fewer channels than requested; read `await client.channelCount`.
`send` queues a message. It does not wait for an ACK. An out-of-range channel
throws `ClientError.invalidChannel`. Received packets include their channel ID.

Only one task can wait for `receivePacket()` at a time. Cancelling that task
cancels its read, not the connection. A clean close returns `nil`; a failed
connection throws. Call `close()` when finished. `close(throwing:)` lets a higher
protocol layer end the connection with its own error.

Independent sessions have independent actors and sockets. A wrapper actor can
share the client's public `unownedExecutor` to avoid an extra executor change.

## Update from 0.1.0

Version 0.2.0 removes redundant type prefixes. Use `Client`, `Packet`, `Delivery`,
`Metrics`, and `ClientError` after `import SwiftENet`. Use `SwiftENet.Client` if
another imported module has the same name. Method names and behavior are unchanged.

## Behavior and limits

- Request 1–255 channels. Moonlight adapters can request 48 and handle a host
  that negotiates one. The library does not silently change a send channel.
- Initial MTU: 900 bytes. Reliable messages larger than a datagram use fragments.
  Large messages requested as unreliable also use reliable fragments.
- Maximum message: 1 MiB. Maximum fragment count: 4,096.
- Each protocol byte queue: 4 MiB. Outgoing commands: 8,192. Total retained or
  reserved receive entries: 8,192. The application queue holds at most 4,096
  packets and 4 MiB. The raw socket queue holds at most 256 datagrams.
- A first fragment reserves the complete message and fragment budget. Conflicting
  reliable sequence ranges and overlapping fragment byte ranges are rejected.
- Peer timeout: 10 seconds. ENet idle ping: 500 ms. Retries use the Moonlight
  fork's bounded linear delay. This retry policy is not selectable in this release.
- IPv4 and explicit IPv6 are supported. Dual-family hostnames prefer IPv4.
  There is no family switch after a handshake timeout.
- Server mode, broadcast, compression, and checksum negotiation are not supported.
- Application encryption, authentication, and application ping messages belong
  to the caller. They are not part of ENet.

On macOS 15, iOS/tvOS 18, and visionOS 2 or newer, socket and timer callbacks
enter the actor directly on its executor. Older supported systems schedule an
actor task for these callbacks. Those systems build, but need device validation.

## Tests and performance

```sh
swift test -Xswiftc -strict-concurrency=complete
swift test -c release -Xswiftc -strict-concurrency=complete
swift test --sanitize=thread -Xswiftc -strict-concurrency=complete
```

Tests cover wire bytes, every command, malformed packets, sequence and timestamp
wrap, channel ordering, lost ACKs, duplicate packets, fragment bounds, queue
limits, IPv4/IPv6, cancellation, and background retries.

[Wire contract](docs/PROTOCOL.md) · [Review](docs/REVIEW.md)

Performance results for the earlier embedded implementation are retained in
[swift-moonlight](https://github.com/wiedymi/swift-moonlight). They are not a
measurement of this extracted package or of full streaming CPU and energy use.

## License

MIT. The protocol implementation was written independently. No upstream ENet C
source is included. See [LICENSE](LICENSE).
