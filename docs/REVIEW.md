# Review for 0.1.0

The review covers the wire parser, reliable ordering, fragment assembly, bounded
queues, native UDP socket, Swift 6 actor isolation, cancellation, and teardown.
The package has no third-party runtime dependency and no C target.

## Changes from the embedded implementation

1. Received packets include their channel ID. The public sender rejects a channel
   outside the negotiated range. Moonlight-specific fallback stays in its adapter.
2. Reliable sequence ranges have one owner. Normal messages and other fragments
   cannot occupy an incomplete fragment's sequence range. Tests check rejected
   conflicts and successful delivery of the valid message that follows.
3. Swift jobs and native socket/timer callbacks use one explicit serial queue.
   The earlier built-in executor path produced Thread Sanitizer access reports
   when a caller sent a packet while a timer serviced a retry. Explicit queue
   submission removes those reports without disabling checks or adding suppressions.
4. A failed close retains its error for current and later readers. Repeated close
   does not replace that failure. Cancelling one read does not close the client.
5. ENet owns reads, retries, and idle ping. Application framing, encryption,
   startup messages, and application pings remain outside the package.

## Checks

- 32 tests pass in debug and release with complete strict concurrency checks.
- The same 32 tests pass under Thread Sanitizer, with no race report.
- UDP tests include IPv4/IPv6, oversized packets, cancellation, late address lookup,
  concurrent senders on two channels, a reader, and retries without app polling.
- Protocol tests include all command layouts, malformed packets, fragmentation,
  overlapping ranges, budgets, duplicate ACKs, and sequence/timestamp wrap.
- Optional C-host checks run from the SwiftMoonlight integration harness. The C
  host and CENet baseline are built in temporary folders and are not dependencies.
  All five workloads pass against stock ENet and the pinned Moonlight fork.
  A separate 70,000-packet, one-channel check passes sequence wrap and fallback.

## Bounds and trade-offs

Byte access stays inside Data's scoped buffer access. Input parsing completes
before protocol state changes. Declared sizes, fragment counts, offsets, and
packet counts are checked before allocation or integer conversion. Fragments
reserve the whole message budget, which accepts fewer simultaneous incomplete
messages under the same bounds. Reliable ranges use modular sequence distances.
The public client takes real monotonic time; callers cannot inject decreasing time.

The retry policy matches the Moonlight ENet fork, rather than stock ENet's older
exponential policy. It is fixed in this release. The package is an Apple client,
not a general server or cross-platform replacement. Large unreliable messages
use reliable fragments. These choices are stated in the README.

Newer Apple runtimes check isolation and enter the actor directly from native
callbacks. Older supported runtimes use actor tasks. Older-device behavior and
live Sunshine/Apollo streaming still need device tests. Unit, loopback, C-host,
and build results do not establish complete app CPU or energy use.

## CI runtime checks

Debug and release checks run on macOS 26. The macOS 26 GitHub runner built the ThreadSanitizer test binary, but its test process made no progress before test output. The cause is not yet confirmed. ThreadSanitizer runs separately on macOS 15; the full local macOS 26 suite also passes under ThreadSanitizer. Both CI jobs have a ten-minute limit. This keeps sanitizer coverage while the hosted macOS 26 startup issue is unresolved.
