# opcda-bridge

[![CI](https://github.com/mikeboiko/opcda-bridge/actions/workflows/checks.yml/badge.svg)](https://github.com/mikeboiko/opcda-bridge/actions/workflows/checks.yml)
[![codecov](https://codecov.io/gh/mikeboiko/opcda-bridge/branch/main/graph/badge.svg?token=)](https://codecov.io/gh/mikeboiko/opcda-bridge)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](https://doc.rust-lang.org/edition-guide/rust-2024/)

A lightweight Rust gateway bridging classic OPC DA (Windows/COM) servers to remote Linux/macOS/Windows clients.

## Status

Active development. Gateway and client are functional end-to-end — OPC DA read/write/browse passing against a live Kepware server.

## Why

OPC DA (OLE for Process Control, Data Access) is a Windows-only, COM/DCOM-based industrial protocol still running on countless PLCs, DCSs, and SCADA systems that predate its successor, OPC UA.

`opcda-bridge` aims to be the same idea distilled to a single static Rust binary per side:

- **Gateway** — runs on the Windows host alongside the OPC DA server, speaks native COM.
- **Client** — runs anywhere (Linux, macOS, Windows), talks to the gateway over the network.

## Installation

Prebuilt binaries for every tagged release are attached to the [Releases](https://github.com/mikeboiko/opcda-bridge/releases) page.

### Gateway (Windows)

The gateway runs on the Windows host alongside the OPC DA server(s) you want to expose.

- **Prebuilt binary** — download `opcda-bridge-gateway-windows-x86.zip` from the latest
  `opcda-bridge-gateway-v*` release, extract it, and run `opcda-bridge-gateway.exe`. No installer
  needed. The binary targets 32-bit Windows (`i686`), matching the architecture most legacy OPC DA
  servers still require for COM interop.
- **From source** (requires a Rust toolchain with 2024 edition support, i.e. Rust 1.85+, and the
  Protocol Buffers compiler `protoc` on `PATH`):
  ```sh
  git clone https://github.com/mikeboiko/opcda-bridge.git
  cd opcda-bridge
  cargo build --release -p opcda-bridge-gateway
  ./target/release/opcda-bridge-gateway.exe
  ```

### Client (Linux, macOS, Windows)

- **Prebuilt binary** — download the archive for your OS from the latest `opcda-bridge-client-v*`
  release, extract it, and run `opcda-bridge-client`:
  - Linux (x86_64): `opcda-bridge-client-linux-x86_64.tar.gz`
  - macOS (arm64): `opcda-bridge-client-macos-arm64.tar.gz`
  - Windows (x86_64): `opcda-bridge-client-windows-x86_64.zip`
- **Arch Linux (AUR)**:
  ```sh
  yay -S opcda-bridge-client-bin
  ```
- **From source** (same prerequisites as the gateway):
  ```sh
  git clone https://github.com/mikeboiko/opcda-bridge.git
  cd opcda-bridge
  cargo build --release -p opcda-bridge-client
  ./target/release/opcda-bridge-client --help
  ```

## Usage

### 1. Start the gateway

On the Windows machine running the OPC DA server:

```sh
opcda-bridge-gateway.exe
```

It listens on all interfaces on port `7600` by default — override with the `OPC_BRIDGE_PORT`
environment variable. If the client will connect from another machine, open that port in the
Windows Firewall.

### 2. Run client commands

Point the client at the gateway with `--host <address:port>` (or set `OPC_BRIDGE_HOST`; both
default to `localhost:7600`):

- List the OPC DA servers registered on the gateway's host:
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 servers
  ```
- Browse a server's tag tree (add `--flat` for a flat tag list instead of the tree):
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 browse --server Kepware.KepServerEX.V5
  ```
- Read one or more tag values:
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 read --server Kepware.KepServerEX.V5 Simulink.Device1.Python.D
  ```
- Write a value to a tag (parsed automatically as bool, int, float, or string):
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 write --server Kepware.KepServerEX.V5 Simulink.Device1.Python.D 42
  ```

Every command prints its result as a table. Run `opcda-bridge-client --help` or
`opcda-bridge-client <command> --help` for the full flag reference.

## Architecture

- Gateway (Windows-only) built on [`opc-da-client`](https://github.com/wends155/opc-cli) for the COM/OPC DA layer — no dependency on proprietary SDKs (OPC Labs QuickOPC, Graybox, Matrikon, etc.).
- Client (cross-platform) is a plain network client with no COM/Windows dependency.
- Scope is intentionally OPC DA only for now — see [`AGENTS.md`](AGENTS.md) for the reasoning.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow, coding standards, and how to get set up.

## License

[MIT](LICENSE)
