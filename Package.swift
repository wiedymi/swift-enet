// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "swift-enet",
    platforms: [.iOS(.v17), .macOS(.v14), .tvOS(.v17), .visionOS(.v1)],
    products: [.library(name: "SwiftENet", targets: ["SwiftENet"])],
    targets: [
        .target(name: "SwiftENet"),
        .testTarget(name: "SwiftENetTests", dependencies: ["SwiftENet"]),
    ],
    swiftLanguageModes: [.v6]
)
