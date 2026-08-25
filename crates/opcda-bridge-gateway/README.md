# opcda-bridge-gateway

Windows gateway that exposes native OPC DA (COM/DCOM) servers over the network to
[`opcda-bridge-client`](https://github.com/bytehound-labs/opcda-bridge) and other gRPC clients.

Install it with Cargo on a Windows host:

```sh
cargo install opcda-bridge-gateway
```

The gateway is versioned independently from the client and protocol crates. Prebuilt archives use
`opcda-bridge-gateway-vX.Y.Z` tags; client/gateway interoperability is determined by the wire
protocol and advertised capabilities rather than matching package versions.

The gateway-wide `GetGatewayInfo` RPC advertises the protocol-feature ranges supported by the
running binary without opening an OPC DA server. The client compatibility command uses this
handshake for deployment checks; `GetCapabilities` remains the per-server operational capability
endpoint and supports older clients.

The gateway must run on the Windows machine hosting the OPC DA server. See the repository README
for service installation, configuration, and firewall setup.

Tag browsing uses native one-level OPC DA enumeration with bounded pages. The gateway owns opaque
browse sessions and continuation tokens, preserves exact ItemIDs separately from display names,
and reports whether a page is complete. Namespace search is a bounded progressive operation that
can be cancelled by dropping the client stream.
DA3 root ItemIDs and unused filters are sent as required non-null empty strings. A server that
also supports DA2 falls back only when its first DA3 root browse returns
`RPC_X_NULL_REF_POINTER` or `E_NOTIMPL`, and reports that compatibility decision explicitly.

The gateway also exposes persistent indexed-search status, refresh, pause, resume, cancel, and
query operations for explicitly configured OPC servers. Capability responses advertise indexed
search support, its protocol version, the configured result limit, and the server's index state.
Indexed results contain exact ItemIDs and breadcrumb labels, never browse-session node keys.
Refreshes run asynchronously, and gateway shutdown cancels active indexing before the process exits.
An inventory can complete successfully with a non-fatal warning when the OPC server rejects
specific namespace branches; the generation remains active and usable, and the status diagnostic
is reported as a warning unless the index state is `failed`.
Completed active generations are durable across gateway restarts. Activation is a short atomic
metadata transition, so search and status remain responsive while a refresh becomes active.
Superseded and abandoned data is reclaimed in bounded background batches through a separate
SQLite WAL connection. An interrupted refresh is superseded when a complete active generation
remains available, so status and search continue to use that snapshot while cleanup runs.
An interrupted initial build remains failed and visible because no complete snapshot can replace it.

Read responses contain semantic values. For an OPC DA `VT_BSTR`, the gateway forwards the exact
BSTR contents without adding display quote characters; quotes remain only when present in the
server value.

Read responses contain semantic values. For an OPC DA `VT_BSTR`, the gateway forwards the exact
BSTR contents without adding display quote characters; quotes remain only when present in the
server value.

Configure the index in the gateway TOML file under `[index]`. Automatic indexing is restricted to
the explicit `servers` allow-list and uses a service-writable SQLite database, conservative
batch/rate/duty-cycle defaults, a two-second foreground quiet period, and one build at a time.
The default database path is `%PROGRAMDATA%\\opcda-bridge\\index.sqlite3` on Windows and
`$XDG_DATA_HOME/opcda-bridge/index.sqlite3` (falling back to
`$HOME/.local/share/opcda-bridge/index.sqlite3`) on Linux/macOS. See the example file for all
available settings, including maintenance windows and health thresholds.
