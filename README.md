# opcda-bridge

[![CI](https://github.com/mikeboiko/opcda-bridge/actions/workflows/checks.yml/badge.svg)](https://github.com/mikeboiko/opcda-bridge/actions/workflows/checks.yml)
[![Coverage](https://github.com/mikeboiko/opcda-bridge/actions/workflows/coverage.yml/badge.svg)](https://github.com/mikeboiko/opcda-bridge/actions/workflows/coverage.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](https://doc.rust-lang.org/edition-guide/rust-2024/)

A lightweight Rust gateway bridging classic OPC DA (Windows/COM) servers to remote Linux/macOS clients.

## Status

Active development. Gateway and client are functional end-to-end — OPC DA read/write/browse passing against a live Kepware server. See the [roadmap](#) for upcoming features.

## Why

OPC DA (OLE for Process Control, Data Access) is a Windows-only, COM/DCOM-based industrial protocol still running on countless PLCs, DCSs, and SCADA systems that predate its successor, OPC UA.

`opcda-bridge` aims to be the same idea distilled to a single static Rust binary per side:

- **Gateway** — runs on the Windows host alongside the OPC DA server, speaks native COM.
- **Client** — runs anywhere (Linux, macOS, Windows), talks to the gateway over the network.

## Architecture

- Gateway (Windows-only) built on [`opc-da-client`](https://github.com/wends155/opc-cli) for the COM/OPC DA layer — no dependency on proprietary SDKs (OPC Labs QuickOPC, Graybox, Matrikon, etc.).
- Client (cross-platform) is a plain network client with no COM/Windows dependency.
- Scope is intentionally OPC DA only for now — see [`AGENTS.md`](AGENTS.md) for the reasoning.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow, coding standards, and how to get set up.

## License

[MIT](LICENSE)
