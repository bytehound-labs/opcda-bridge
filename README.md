# opcda-bridge

[![CI](https://github.com/bytehound-labs/opcda-bridge/actions/workflows/checks.yml/badge.svg)](https://github.com/bytehound-labs/opcda-bridge/actions/workflows/checks.yml)
[![codecov](https://codecov.io/gh/bytehound-labs/opcda-bridge/branch/main/graph/badge.svg?token=)](https://codecov.io/gh/bytehound-labs/opcda-bridge)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](https://doc.rust-lang.org/edition-guide/rust-2024/)
[![opcda-bridge on crates.io](https://img.shields.io/crates/v/opcda-bridge.svg)](https://crates.io/crates/opcda-bridge)
[![opcda-bridge on docs.rs](https://docs.rs/opcda-bridge/badge.svg)](https://docs.rs/opcda-bridge)
[![opcda-bridge-client on crates.io](https://img.shields.io/crates/v/opcda-bridge-client.svg)](https://crates.io/crates/opcda-bridge-client)
[![opcda-bridge-gateway on crates.io](https://img.shields.io/crates/v/opcda-bridge-gateway.svg)](https://crates.io/crates/opcda-bridge-gateway)

A lightweight Rust gateway bridging classic OPC DA (Windows/COM) servers to remote Linux/macOS/Windows clients.

## Status

Active development. Gateway and client are functional end-to-end — OPC DA read/write/browse passing
against a live Kepware server. The workspace publishes the reusable client library, protocol
definitions, cross-platform CLI, and Windows gateway as separate crates.

## Why

OPC DA (OLE for Process Control, Data Access) is a Windows-only, COM/DCOM-based industrial protocol still running on countless PLCs, DCSs, and SCADA systems that predate its successor, OPC UA.

`opcda-bridge` aims to be the same idea distilled to a single static Rust binary per side:

- **Gateway** — runs on the Windows host alongside the OPC DA server, speaks native COM.
- **Client** — runs anywhere (Linux, macOS, Windows), talks to the gateway over the network.

## Installation

Prebuilt binaries for every tagged release are attached to the [Releases](https://github.com/bytehound-labs/opcda-bridge/releases) page.

### Gateway (Windows)

The gateway runs on the Windows host alongside the OPC DA server(s) you want to expose.

- **Prebuilt binary** — download `opcda-bridge-gateway-windows-x86.zip` from the latest
  `opcda-bridge-gateway-v*` release, extract it, and run `opcda-bridge-gateway.exe`. No installer
  needed. The binary targets 32-bit Windows (`i686`), matching the architecture most legacy OPC DA
  servers still require for COM interop.
- **From source** (requires Rust 1.88+ and the
  Protocol Buffers compiler `protoc` on `PATH`):
  ```sh
  git clone https://github.com/bytehound-labs/opcda-bridge.git
  cd opcda-bridge
  cargo build --release -p opcda-bridge-gateway
  ./target/release/opcda-bridge-gateway.exe
  ```
- **Install from crates.io** (requires Rust 1.88+ and `protoc`):
  ```sh
  cargo install opcda-bridge-gateway
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
  git clone https://github.com/bytehound-labs/opcda-bridge.git
  cd opcda-bridge
  cargo build --release -p opcda-bridge-client
  ./target/release/opcda-bridge-client --help
  ```
- **Install from crates.io** (same prerequisites as the gateway):
  ```sh
  cargo install opcda-bridge-client
  ```
  This places `opcda-bridge-client` on `PATH` at `~/.cargo/bin/opcda-bridge-client`; re-run the same
  command (add `--force` to overwrite an existing install) to upgrade. Use
  `cargo install opcda-bridge-client --version 0.2.0` to pin a specific published version.

## Usage

### 1. Start the gateway

On the Windows machine running the OPC DA server:

```sh
opcda-bridge-gateway.exe
```

It listens on all interfaces on port `7600` by default — override with `--port`, the
`OPC_BRIDGE_PORT` environment variable, or a config file (see [Configuration](#configuration)
below). If the client will connect from another machine, open that port in the Windows Firewall.

Press `Ctrl+C` (or close the console window) to stop it — the gateway finishes any in-flight
requests before exiting rather than dropping them mid-response.

### 2. Run client commands

Point the client at the gateway with `--host <address:port>` (or set `OPC_BRIDGE_HOST`; both
default to `localhost:7600`). A default OPC DA server and other settings can also come from a
config file — see [Configuration](#configuration) below.

- List the OPC DA servers registered on the gateway's host:
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 servers
  ```
- Browse a server's tags. Default output is one level of the tag tree, rooted at `--path`
  (the top level if omitted); `Branch` entries are suffixed `/` — pass one back in as `--path` to
  drill down a level further. Add `--flat` for a flat list of every tag instead, and `--max-tags`
  to cap how many are streamed back (default 1000):
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 browse --server Kepware.KepServerEX.V5
  opcda-bridge-client --host 192.168.1.50:7600 browse --server Kepware.KepServerEX.V5 --path Simulink.Device1
  ```
- Read one or more tag values:
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 read --server Kepware.KepServerEX.V5 Simulink.Device1.Python.D
  ```
- Write a value to a tag (parsed automatically as bool, int, float, or string):
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 write --server Kepware.KepServerEX.V5 Simulink.Device1.Python.D 42
  ```

Every command prints its result as a table by default, except `browse` in its default tree mode,
which prints an indented tree instead (pass `--flat` for the tabular form). Pass `--output json`
(or its shorthand, `--json`) for machine-readable output instead — see
[JSON output](#json-output) below. `--host`, `--config`, `--output`, and `--json` may be placed
either before or after the subcommand (e.g. both `--json read ...` and `read ... --json` work).
Run `opcda-bridge-client --help` or `opcda-bridge-client <command> --help` for the full flag
reference.

### JSON output

Every command accepts `--output json` (or the shorthand `--json`) to print a pretty-printed JSON
array instead of a table, for shell scripting, CI, or piping into `jq`. Field names are the same
across both formats:

```sh
opcda-bridge-client --host 192.168.1.50:7600 --json read --server Kepware.KepServerEX.V5 Simulink.Device1.Python.D
```

```json
[
  {
    "tag_id": "Simulink.Device1.Python.D",
    "value": "42",
    "quality": "Good",
    "timestamp": "2024-01-01T00:00:00Z"
  }
]
```

`browse` always emits a flat JSON array of `{"tag_id": ..., "node_type": ...}` objects in this
mode, regardless of `--flat` — the indented tree is a display-only rendering with no JSON
equivalent. Commands that fail print a structured `{"error": "..."}` object to stderr instead of
the usual `Error: ...` text, and the process still exits non-zero.

`--output`/`--json` follow the same `CLI flag > environment variable > config file > default`
precedence as every other setting (env var `OPC_BRIDGE_OUTPUT`, config key `output`), so a script
can set the environment variable once instead of passing `--json` on every invocation.

### Using the client from other languages

The lightest-weight integration path for calling the client from a script (Python, shell, etc.)
is shelling out to the binary with `--json` and parsing stdout, as shown above. For heavier
integration — a long-running .NET or Python service that talks to the gateway directly — it's
usually better to generate native gRPC stubs straight from
[`opcda-bridge-proto/proto/bridge.proto`](crates/opcda-bridge-proto/proto/bridge.proto) (e.g. `grpcio-tools` for
Python, `Grpc.Tools` for .NET) and skip the client binary entirely; it's the same wire protocol
the CLI itself speaks. One caveat: the gateway serves plaintext HTTP/2 (no TLS), so a .NET
client needs to opt in explicitly with
`AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true)` before
connecting.

For a Rust program, skip both of the above and depend on
[`opcda-bridge`](https://crates.io/crates/opcda-bridge) directly instead: it's the same typed
connect/list-servers/browse/read/write API `opcda-bridge-client` itself is built on, returning
plain Rust values (`Vec<String>`, `Vec<BrowseNode>`, `Vec<TagValue>`, `WriteResult`) with no
`clap`, `tabled`, `serde_json`, or `toml` pulled in transitively — just `opcda-bridge-proto`,
`tonic`, and `thiserror`. Add it to a project with:

```sh
cargo add opcda-bridge
```

See the crate's [API documentation](https://docs.rs/opcda-bridge) for a usage example.

## Configuration

Both binaries accept a `--config <path>` flag pointing at a TOML file. Every setting resolves
with the same precedence, highest first:

**CLI flag > environment variable > config file > built-in default**

A config file (or an individual key within it) is entirely optional — anything not set falls
back through the rest of the chain. If `--config` is omitted, each binary looks for a config
file in a default location; a missing file there is not an error, since it may simply not have
been created yet. A file that _does_ exist but fails to parse as TOML is always a hard error,
pointing at the file and the parse problem.

### Gateway

Looks for `opcda-bridge-gateway.toml` next to the executable unless `--config` gives another
path. See
[`crates/opcda-bridge-gateway/opcda-bridge-gateway.example.toml`](crates/opcda-bridge-gateway/opcda-bridge-gateway.example.toml)
for every available key.

| Setting     | CLI flag | Env var           | Config key | Default |
| ----------- | -------- | ----------------- | ---------- | ------- |
| Listen port | `--port` | `OPC_BRIDGE_PORT` | `port`     | `7600`  |

Logging settings (`log.*`) are also read from this file — see [Logging](#logging) below.

### Client

Looks for a config file in a platform-specific location unless `--config` gives another path:

- Linux/macOS: `$XDG_CONFIG_HOME/opcda-bridge/client.toml`, falling back to
  `$HOME/.config/opcda-bridge/client.toml`.
- Windows: `%APPDATA%\opcda-bridge\client.toml`.

See
[`crates/opcda-bridge-client/client.example.toml`](crates/opcda-bridge-client/client.example.toml)
for every available key.

| Setting               | CLI flag              | Env var             | Config key | Default                               |
| --------------------- | --------------------- | ------------------- | ---------- | ------------------------------------- |
| Gateway address       | `--host`              | `OPC_BRIDGE_HOST`   | `host`     | `localhost:7600`                      |
| Default OPC DA server | `--server`            | —                   | `server`   | none — must be set one way or another |
| Browse tag cap        | `--max-tags`          | —                   | `max_tags` | `1000`                                |
| Output format         | `--output` / `--json` | `OPC_BRIDGE_OUTPUT` | `output`   | `table`                               |

`server` has no built-in default: if it is left unset by every source, `browse`/`read`/`write`
fail with an error rather than guessing a server.

## Logging

The gateway writes structured logs to a rolling file next to its executable — by default
`logs/opcda-bridge-gateway.log.<date>` — through a non-blocking writer, so logging never adds
latency to request handling. When a console is attached (running interactively, as opposed to
under a background/service process), the same log lines are also printed to stdout.

| Setting      | CLI flag         | Env var    | Config key     | Default                       |
| ------------ | ---------------- | ---------- | -------------- | ----------------------------- |
| Level/filter | `--log-level`    | `RUST_LOG` | `log.level`    | `info`                        |
| Directory    | `--log-dir`      | —          | `log.dir`      | `logs` next to the executable |
| Format       | `--log-format`   | —          | `log.format`   | `pretty`                      |
| Rotation     | `--log-rotation` | —          | `log.rotation` | `daily`                       |

- **Level/filter** accepts a single level (`error`, `warn`, `info`, `debug`, `trace`) or a full
  [`tracing_subscriber::EnvFilter`](https://docs.rs/tracing-subscriber) directive spec, e.g.
  `opcda_bridge_gateway=debug,tower=warn`. An invalid value falls back to `info` rather than
  preventing the gateway from starting.
- **Format** is `pretty` (human-readable text) or `json` (one JSON object per line, for log
  shippers such as Fluent Bit or Vector).
- **Rotation** is `hourly`, `daily`, or `never` (a single file that grows indefinitely).

## Running as a Windows service

The gateway can run under the Windows Service Control Manager (SCM) instead of an interactive
console, so it starts automatically at boot without a logged-in user. Manage it with built-in
subcommands — no need to hand-roll `sc.exe` invocations:

| Command                              | Effect                                            |
| ------------------------------------ | ------------------------------------------------- |
| `opcda-bridge-gateway.exe install`   | Registers the service (auto-start, `LocalSystem`) |
| `opcda-bridge-gateway.exe start`     | Starts the registered service                     |
| `opcda-bridge-gateway.exe status`    | Prints the service's current SCM state            |
| `opcda-bridge-gateway.exe stop`      | Requests a graceful stop                          |
| `opcda-bridge-gateway.exe uninstall` | Stops (if running) and removes the service        |

Run `install`, `uninstall`, `start`, and `stop` from an elevated (Administrator) prompt — the SCM
rejects these operations otherwise.

Any flags that should apply every time the service starts — `--port`, `--config`, `--log-*` —
must be given to `install` **before** the subcommand, since they become the service's permanent
launch arguments:

```sh
opcda-bridge-gateway.exe --port 7700 --log-dir C:\logs install
```

not `opcda-bridge-gateway.exe install --port 7700`. If the client will connect from another
machine, remember to open the listen port in the Windows Firewall, same as console mode.

Once running as a service there is no console, so [logging](#logging) always goes to the file
sink — the same location and settings as console mode (next to the executable by default, or
wherever `--log-dir`/`log.dir` points). The service also shuts down the same way `Ctrl+C` does in
console mode: `stop` drains in-flight requests before the SCM reports it `Stopped`.

## Architecture

- Gateway (Windows-only) built on [`opc-da-client`](https://github.com/wends155/opc-cli) for the COM/OPC DA layer — no dependency on proprietary SDKs (OPC Labs QuickOPC, Graybox, Matrikon, etc.).
- Client (cross-platform) is a plain network client with no COM/Windows dependency.
- Scope is intentionally OPC DA only for now — see [`AGENTS.md`](AGENTS.md) for the reasoning.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow, coding standards, and how to get set up.

## License

[MIT](LICENSE)
