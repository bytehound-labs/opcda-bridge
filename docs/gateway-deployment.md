# Gateway deployment, service, and firewall

The gateway runs on the Windows host that has the OPC DA server registered. The client can run
separately on Linux, macOS, or Windows and connects to the gateway over TCP.

## Security boundary

The gateway is an unauthenticated, unencrypted gRPC service over plaintext HTTP/2. It binds to
`0.0.0.0` (all IPv4 interfaces) and listens on TCP port `7600` by default. Any client that can
reach the listener can enumerate servers, browse and search namespaces, read and write tags
allowed by the OPC DA server account, and operate the namespace index. The bind address is not
configurable.

When installed as a Windows service, `OpcdaBridgeGateway` runs as `LocalSystem` and starts
automatically. A gateway launched interactively runs as the account that starts the process.
Protect the executable, configuration, logs, and index database from modification by untrusted
users.

Deploy the gateway only on a trusted, segmented OT network. Do not expose port `7600` to the
Internet or an untrusted network. The release archive and service commands do not create Windows
Firewall rules. Add an inbound rule yourself and restrict it to the client addresses that need
access. For example, from an elevated PowerShell prompt, replace the example address before
creating the rule:

```powershell
New-NetFirewallRule `
  -DisplayName "opcda-bridge gateway" `
  -Direction Inbound `
  -Action Allow `
  -Protocol TCP `
  -LocalPort 7600 `
  -RemoteAddress 192.168.1.50 `
  -Profile Domain,Private
```

Port `7600` is the protected gateway listener. Diagnostic sidecars use port `7602` and a separate
executable, configuration, log directory, and SQLite index database. Never point two gateway
processes at the same index database path.

## Install the gateway

The official Windows gateway is 32-bit x86 (`i686-pc-windows-msvc`), including when it runs on
64-bit Windows. Windows uses WOW64 to run the x86 executable. A host-default x64 build is not a
supported substitute.

Download `opcda-bridge-gateway-windows-x86.zip` from the
[GitHub Releases](https://github.com/bytehound-labs/opcda-bridge/releases) page, verify the
release checksums and attestations as described in [Release and package verification](release.md),
extract it on the OPC DA server host, and run `opcda-bridge-gateway.exe`.

Building or installing from source requires Rust 1.88 or newer, `protoc` on `PATH`, and the
explicit x86 target:

```powershell
rustup target add i686-pc-windows-msvc
cargo build --release --locked -p opcda-bridge-gateway --target i686-pc-windows-msvc
```

To install the published crate:

```powershell
rustup target add i686-pc-windows-msvc
cargo install --locked --target i686-pc-windows-msvc opcda-bridge-gateway
```

A plain `cargo build --release` on 64-bit Windows produces an x64 host build and does not produce
the supported gateway artifact.

## Run interactively

Start the gateway from a console on the Windows OPC DA host:

```powershell
.\opcda-bridge-gateway.exe
```

The default listener is `0.0.0.0:7600`. The listening port can be changed with `--port`,
`OPC_BRIDGE_PORT`, or the `port` configuration key; changing the port does not change the bind
address. `Ctrl+C` requests graceful shutdown and lets in-flight requests drain.

## Run as a Windows service

The gateway includes service-management subcommands:

| Command | Effect |
| --- | --- |
| `opcda-bridge-gateway.exe install` | Registers the automatic `OpcdaBridgeGateway` service as `LocalSystem` |
| `opcda-bridge-gateway.exe start` | Starts the registered service |
| `opcda-bridge-gateway.exe status` | Reports the Service Control Manager state |
| `opcda-bridge-gateway.exe stop` | Requests graceful shutdown |
| `opcda-bridge-gateway.exe uninstall` | Stops the service if needed and removes its registration |

Run `install`, `start`, `stop`, and `uninstall` from an elevated Administrator prompt. The service
reports `Running` after its listener is ready and drains in-flight requests during a stop.

Options that must apply to each service start are captured during installation. Put them before
the `install` subcommand:

```powershell
.\opcda-bridge-gateway.exe --config C:\ProgramData\opcda-bridge\gateway.toml install
```

The service retains the executable path and the applicable `--config`, `--port`, and `--log-*`
arguments supplied at installation. Flags after `install` do not configure the service.

## Configuration and logs

The gateway loads `opcda-bridge-gateway.toml` from beside the executable unless `--config` names
another file. Settings use this precedence:

**CLI flag > environment variable > TOML file > built-in default**

An absent automatically discovered configuration file is allowed. A file that exists but contains
invalid TOML is an error. The listen port defaults to `7600`; the example file documents gateway,
logging, and index settings:

[`opcda-bridge-gateway.example.toml`](../crates/opcda-bridge-gateway/opcda-bridge-gateway.example.toml)

The gateway writes rolling logs under `logs/` beside the executable by default. Services have no
console, so their output is in the log files. The `log` settings select level/filter, directory,
format (`pretty` or newline-delimited `json`), and rotation (`hourly`, `daily`, or `never`).

## Related references

- [Indexing and search operations](indexing-and-search.md)
- [Troubleshooting](troubleshooting.md)
- [Security policy](../SECURITY.md)
