# opcda-bridge-gateway

Windows gateway that exposes native OPC DA (COM/DCOM) servers over the network to
[`opcda-bridge-client`](https://github.com/bytehound-labs/opcda-bridge) and other gRPC clients.

Install it with Cargo on a Windows host:

```sh
cargo install opcda-bridge-gateway
```

The gateway must run on the Windows machine hosting the OPC DA server. See the repository README
for service installation, configuration, and firewall setup.

Tag browsing uses native one-level OPC DA enumeration with bounded pages. The gateway owns opaque
browse sessions and continuation tokens, preserves exact ItemIDs separately from display names,
and reports whether a page is complete. Namespace search is a bounded progressive operation that
can be cancelled by dropping the client stream.

The gateway also exposes persistent indexed-search status, refresh, pause, resume, cancel, and
query operations for explicitly configured OPC servers. Capability responses advertise indexed
search support, its protocol version, the configured result limit, and the server's index state.
Indexed results contain exact ItemIDs and breadcrumb labels, never browse-session node keys.
Refreshes run asynchronously, and gateway shutdown cancels active indexing before the process exits.

Configure the index in the gateway TOML file under `[index]`. Automatic indexing is restricted to
the explicit `servers` allow-list and uses a service-writable SQLite database, conservative
batch/rate/duty-cycle defaults, a two-second foreground quiet period, and one build at a time.
The default database path is `%PROGRAMDATA%\\opcda-bridge\\index.sqlite3` on Windows and
`$XDG_DATA_HOME/opcda-bridge/index.sqlite3` (falling back to
`$HOME/.local/share/opcda-bridge/index.sqlite3`) on Linux/macOS. See the example file for all
available settings, including maintenance windows and health thresholds.
