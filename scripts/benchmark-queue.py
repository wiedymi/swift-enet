#!/usr/bin/env python3
"""Measure service/deadline work for stalled reliable queues; remove all build files."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--revision', help='Compare a committed revision instead of the working tree')
parser.add_argument('--debug', action='store_true')
parser.add_argument('--output', type=Path)
args = parser.parse_args()
with tempfile.TemporaryDirectory(prefix='swift-enet-queue-') as folder:
    folder = Path(folder)
    sources = []
    hashes = {}
    for original in sorted((root / 'Sources/SwiftENet').glob('*.swift')):
        data = subprocess.check_output(['git', 'show', f'{args.revision}:Sources/SwiftENet/{original.name}'], cwd=root) if args.revision else original.read_bytes()
        target = folder / original.name
        target.write_bytes(data)
        sources.append(str(target))
        hashes[original.name] = hashlib.sha256(data).hexdigest()
    main = folder / 'main.swift'
    main.write_text('''import Foundation
import Dispatch
var results: [[String: Double]] = []
for count in [1, 64, 512, 4096] {
    var peer = Peer(connectID: 7, connectData: 0, channels: 2)
    _ = peer.service(now: 0)
    let verify = Command(sequence: 1, body: .verify(.init(peerID: 7, incomingSession: 1, outgoingSession: 2, channels: 2, connectID: 7)))
    _ = peer.receive(Datagram(peerID: 0, sessionID: 1, sentTime: 0, commands: [verify]).encoded(), now: 0)
    for _ in 0..<count { try peer.enqueue(Data(count: 500), channel: 0, delivery: .reliable) }
    _ = peer.service(now: 10)
    var samples: [Double] = []
    var checksum: UInt64 = 0
    for _ in 0..<5 {
        let start = DispatchTime.now().uptimeNanoseconds
        for _ in 0..<2000 {
            checksum &+= UInt64(peer.service(now: 11).datagrams.count)
            checksum &+= peer.nextServiceTime ?? 0
        }
        samples.append(Double(DispatchTime.now().uptimeNanoseconds - start) / 2000 / 1000)
    }
    results.append(["queuedCommands": Double(count), "medianMicroseconds": samples.sorted()[2], "checksum": Double(checksum)])
}
print(String(data: try JSONSerialization.data(withJSONObject: results), encoding: .utf8)!)
''')
    binary = folder / 'benchmark'
    subprocess.run(['swiftc', '-swift-version', '6', '-strict-concurrency=complete', '-Onone' if args.debug else '-O', *sources, str(main), '-o', str(binary)], check=True)
    result = {'revision': args.revision or 'working-tree', 'optimization': 'debug' if args.debug else 'release', 'sourceSHA256': hashes, 'machine': subprocess.check_output(['sysctl', '-n', 'machdep.cpu.brand_string'], text=True).strip(), 'compiler': subprocess.check_output(['swiftc', '--version'], stderr=subprocess.STDOUT, text=True).strip(), 'results': json.loads(subprocess.check_output([str(binary)]))}
    text = json.dumps(result, indent=2) + '\n'
    if args.output:
        args.output.write_text(text)
    print(text)
