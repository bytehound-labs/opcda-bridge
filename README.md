# opcda-bridge

[![CI](https://github.com/bytehound-labs/opcda-bridge/actions/workflows/checks.yml/badge.svg)](https://github.com/bytehound-labs/opcda-bridge/actions/workflows/checks.yml)
[![codecov](https://codecov.io/gh/bytehound-labs/opcda-bridge/branch/main/graph/badge.svg?token=)](https://codecov.io/gh/bytehound-labs/opcda-bridge)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=bytehound-labs_opcda-bridge&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=bytehound-labs_opcda-bridge)
[![opcda-bridge on crates.io](https://img.shields.io/crates/v/opcda-bridge.svg)](https://crates.io/crates/opcda-bridge)
[![opcda-bridge on docs.rs](https://docs.rs/opcda-bridge/badge.svg)](https://docs.rs/opcda-bridge)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](https://doc.rust-lang.org/edition-guide/rust-2024/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A lightweight Rust gateway that connects classic Windows OPC DA servers to cross-platform
clients. The Windows gateway uses COM/DCOM locally and exposes a gRPC service; the reusable Rust
client and command-line client run on Windows, Linux, and macOS.

## Network safety

The gateway is unauthenticated, unencrypted, and listens on `0.0.0.0:7600`. When installed as a
Windows service it runs as `LocalSystem`. Any client that can reach it can read and write tags
allowed by the OPC DA server account and control index operations. Deploy it only on a trusted,
segmented network and restrict the Windows Firewall rule to approved client addresses. The
gateway archive and service registration do not create firewall rules.

## Quick start

### Gateway

On the Windows host with the OPC DA server, download the official
[`opcda-bridge-gateway-windows-x86.zip`](https://github.com/bytehound-labs/opcda-bridge/releases)
archive, verify its checksum and provenance, and run `opcda-bridge-gateway.exe`. The supported
gateway artifact is 32-bit x86 (`i686-pc-windows-msvc`), including on 64-bit Windows.

See [Gateway deployment, service, and firewall](docs/gateway-deployment.md) before exposing the
listener or installing the Windows service.

### Client

Install `opcda-bridge-client` from a platform archive, crates.io, or the Arch User Repository.
For example, list OPC DA servers registered on the gateway:

```sh
opcda-bridge-client --host 192.168.1.50:7600 servers
```

The client and gateway versions do not need to match. Check their negotiated features before
depending on optional operations:

```sh
opcda-bridge-client --host 192.168.1.50:7600 compatibility
```

Automatic index refresh is an explicit per-server opt-in. New and recreated indexes start off;
manual refresh and retry never change the saved choice. Enable it through BHTune's
**Enable Auto-refresh** button or the Rust
client's `set_search_index_auto_refresh` method. The saved choice survives gateway restarts.
Disabling it stops future scheduling without cancelling an active build or removing cached tags.
The gateway has no `index.enabled` or `index.paused` override. Pacing, maintenance windows,
health protection, and retry backoff still govern automatic work. An active build does not
start a second scheduled build.

## Documentation

| Topic                                                      | Guide                                                            |
| ---------------------------------------------------------- | ---------------------------------------------------------------- |
| Components and request flow                                | [Architecture](docs/architecture.md)                             |
| Windows setup, service, and firewall                       | [Gateway deployment](docs/gateway-deployment.md)                 |
| Persistent index operations and large-namespace acceptance | [Indexing and search](docs/indexing-and-search.md)               |
| Protocol negotiation and package compatibility             | [Protocol and compatibility](docs/protocol-and-compatibility.md) |
| Common connection, browse, and index issues                | [Troubleshooting](docs/troubleshooting.md)                       |
| Package releases and artifact verification                 | [Release and package verification](docs/release.md)              |

The generated [compatibility report](COMPATIBILITY.md) and
[machine-readable catalog](compatibility.json) describe protocol release lines and test evidence.
The [security policy](SECURITY.md) explains the threat model and vulnerability reporting process.

## Packages

The workspace publishes four independently versioned crates:

- `opcda-bridge-proto` — Protobuf schema and generated types.
- `opcda-bridge` — reusable, typed Rust client library.
- `opcda-bridge-client` — cross-platform CLI.
- `opcda-bridge-gateway` — Windows OPC DA gateway and namespace index. Its gRPC service separates
  RPC handling, protocol/error mappings, and live search. The index keeps a stable facade over
  focused enrollment, scheduling, traversal, storage, query, and status modules; the gateway crate
  README describes their responsibilities.

The gateway's native OPC DA/COM adapter uses the separate
[`bytehound-opc-da-client`](https://github.com/bytehound-labs/opc-cli) crate, which is maintained
outside this workspace. Gateway builds require version 0.3.1 or later for correct Browse
marshalling when no property IDs are requested.

GitHub release archives are provided for the client and gateway. Package compatibility is
negotiated through protocol and capability versions; equal package versions are not required.

## Contributing

Contributions use focused pull requests and the repository's required validation checks. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the contributor workflow.

## License

[MIT](LICENSE)
