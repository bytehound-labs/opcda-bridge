# opcda-bridge

A lightweight Rust gateway bridging classic OPC DA (Windows/COM) servers to remote Linux/macOS clients.

## Status

Early scaffolding. Architecture and key dependencies are decided; only a minimal `cargo init`
binary exists so far — no gateway/client/COM logic has landed yet.

## Why

OPC DA (OLE for Process Control, Data Access) is a Windows-only, COM/DCOM-based industrial protocol still running on countless PLCs, DCSs, and SCADA systems that predate its successor, OPC UA. The existing open-source bridge for non-Windows clients, [openopc2](https://github.com/iterativ/openopc2), works well but carries a full Python/Pyro5 stack and legacy dependency baggage.

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
