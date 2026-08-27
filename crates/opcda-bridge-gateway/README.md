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
and reports whether a page is complete. Only selectable item and branch-and-item nodes expose
ItemIDs; branch-only nodes retain their private navigation identifiers behind opaque node keys.
Namespace search is a bounded progressive operation that can be cancelled by dropping the client
stream.
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
Completed active generations are durable across gateway restarts. Activation is an atomic metadata
transition, and promotion status uses a read-only SQLite connection plus filesystem diagnostics, so
status remains responsive even while the writer is in the promotion critical section.
Only one build for a server can hold its gateway-wide file lock at a time; contention reports the
owning process metadata. On Windows, that metadata is kept in an adjacent `.build.owner` sidecar
because the locked file itself may be unreadable.
Superseded and abandoned data is reclaimed in bounded background batches through a separate
SQLite WAL connection, coordinated by a database-wide writer gate shared with every build mutation
for the same database file, including builds for other servers. Cleanup defers while any build is
active, keeps its request registered while waiting, yields between batches so a build can make
progress, and resumes pending requests after the last build finishes even when the request came
from another manager instance sharing the database. Shutdown is also observed when it races with
that deferred wait. An interrupted refresh is superseded when a complete active generation
remains available, so status and search continue to use that snapshot while cleanup runs.
An interrupted initial build remains failed and visible because no complete snapshot can replace it.
Database coordination and persistent build-lock paths use the canonical identity of the database
file, so existing-file aliases such as relative paths and symlinks cannot bypass coordination.
If the file and its parent cannot be canonicalized, the original path spelling is retained.
Independent in-memory databases are isolated from the registry and do not create filesystem
build-lock sidecars.
Uncached indexed searches use a separate read-only SQLite connection and rank only a bounded
candidate set in memory, so a broad query cannot hold the coordinator's foreground database
mutex while it scans the FTS index. Status, discovery, reads, writes, and lazy browse therefore
remain available while search work is in progress. Matching is case-insensitive with
exact/prefix/contains ranking, and responses report when additional results exist beyond the
requested limit. During promotion, searches use the active generation already returned by the
promotion-safe status path instead of waiting for the writable database mutex. Cancellation
requests received before inventory startup returns its control handle are retained and applied
once the handle is available.

Read responses contain semantic values. For an OPC DA `VT_BSTR`, the gateway forwards the exact
BSTR contents without adding display quote characters; quotes remain only when present in the
server value.

Read responses contain semantic values. For an OPC DA `VT_BSTR`, the gateway forwards the exact
BSTR contents without adding display quote characters; quotes remain only when present in the
server value.

Configure the index in the gateway TOML file under `[index]`. Automatic indexing is restricted to
the explicit `servers` allow-list and uses a service-writable SQLite database, conservative
batch/rate/duty-cycle defaults, a two-second foreground quiet period, and one build at a time.
Native inventory batches are bounded to 1,000 entries by the OPC DA client contract.
Native inventory slicing and SQLite commit batching are independently bounded, and adaptive
controller decisions update both the native slice batch size and pacing interval; the commit
interval provides a time limit for low-volume inventories. Runtime status includes rolling
foreground latency/error/quality metrics, host/storage availability, and persisted scheduler
backoff diagnostics.
If the native client rejects an initial or adaptive pacing update, the build fails visibly and
the previous complete generation remains active; pacing errors are never logged and ignored.
Completed generations are refreshed weekly by default. The first automatic build waits for a
configured maintenance window; when no window is configured, use the manual refresh operation.
Startup grace and deterministic per-server schedule jitter prevent indexing from starting
immediately after a restart or in lockstep across targets.
The default database path is `%PROGRAMDATA%\\opcda-bridge\\index.sqlite3` on Windows and
`$XDG_DATA_HOME/opcda-bridge/index.sqlite3` (falling back to
`$HOME/.local/share/opcda-bridge/index.sqlite3`) on Linux/macOS. See the example file for all
available settings, including maintenance windows, health thresholds, and adaptive AIMD
rate/batch/duty-cycle floors and ceilings. Adaptive indexing starts at the canary profile and
backs off or pauses when recent foreground OPC errors or bad-quality reads, or host/storage
guardrails, deteriorate.
Pre-build and health OPC operations are bounded by `operation_timeout_seconds`, so an
unresponsive target cannot hold the scheduler indefinitely.
An optional `sentinel_tag` is read during health probes; omitted or unavailable sentinel
configuration is reported explicitly rather than treated as a healthy zero value. Status also
distinguishes a configured sentinel from its probe result, so an unprobed sentinel is not reported
as absent.
