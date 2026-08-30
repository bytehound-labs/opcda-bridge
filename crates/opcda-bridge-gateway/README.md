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

Development branches may pin a compatible, unreleased `bytehound-opc-da-client` revision while
the native client and gateway are tested together. A packaged or published gateway must use the
corresponding released client version instead.

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
Foreground operations are reference-counted per server; indexing stays paused while any foreground
user is active and remains paused through the configured quiet period after the last foreground
operation ends.
An inventory can complete successfully with a non-fatal warning when the OPC server rejects
specific namespace branches; the generation remains active and usable, and the status diagnostic
is reported as a warning unless the index state is `failed`.
Each build checks its maintenance-window, health, and adaptive recovery gates before requesting
the next inventory event. Health readiness combines capability and latency checks with the
optional sentinel tag; unhealthy targets pause inventory with bounded exponential backoff and
healthy recovery resumes it with the configured pacing. Without a sentinel tag, the health
status is reported as `Unavailable` while capability and latency checks can still permit the
build. The event wait is bounded by the configured operation timeout; an expired wait requests
native cancellation with the `inventory_event_timeout` source before the generation is marked
failed. Cancellation, health failure, or a rejected pacing update terminates the build without
replacing the last complete generation.
Pending entries are flushed before terminal state is recorded, and successful completion with a
non-fatal inventory warning remains searchable while the warning is exposed in status.
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
Status combines the persisted generation snapshot with runtime build, health, storage,
foreground, and scheduler diagnostics. During promotion, persisted status is read through a
read-only connection; a runtime error overrides the reported state only when no build is active.
Database coordination and persistent build-lock paths use the canonical identity of the database
file, so existing-file aliases such as relative paths and symlinks cannot bypass coordination.
If the file and its parent cannot be canonicalized, the original path spelling is retained.
Independent in-memory databases are isolated from the registry and do not create filesystem
build-lock sidecars.
Uncached indexed searches use a separate read-only SQLite connection and rank only bounded
candidate sets in memory, so a broad query cannot hold the coordinator's foreground database
mutex while it scans the FTS index. Exact searches use separate equality lookups on ordered
normalized display-name and ItemID indexes, exclude display-name matches from the lower-priority
ItemID lookup, then merge and deduplicate those candidates before ranking; this avoids sorting a
whole common-name result set before applying the limit. Prefix searches use separate
lexicographic range probes over those same indexes; each probe is bounded to `limit + 1` rows.
Prefix bounds handle Unicode scalar boundaries, including the surrogate gap and the maximum
scalar value. Contains searches use the trigram FTS index. Status,
discovery, reads, writes, and lazy browse therefore remain available while search work is in
progress. Matching is case-insensitive with exact/prefix/contains ranking, and
responses report when additional results exist beyond the requested limit. During promotion,
searches use the active generation already returned by the promotion-safe status path instead of
waiting for the writable database mutex.
When a populated database is missing required secondary indexes or the trigram full-text index,
startup records that preparation is required instead of building those objects during ordinary
gateway work. Refresh, control, search, and obsolete-generation cleanup remain blocked until the
gateway is stopped and the one-shot `index-prepare` command completes:
`opcda-bridge-gateway --config PATH index-prepare`. The command exits after preparation without
starting the gateway or contacting OPC DA. Preparation creates and populates the missing objects
in one transaction, validates them before commit, and records a retryable failure marker
if it cannot complete. Empty databases prepare automatically, and status remains inspectable while
preparation is required. Unexpected existing index definitions fail initialization, and
inconsistent full-text data is quarantined rather than served as a potentially incomplete cache;
validation checks both row counts and the stored server/generation/item/display values plus
normalized breadcrumb values. Relational breadcrumbs are JSON arrays while the FTS copy uses
space-separated searchable text; both that representation and legacy JSON-form FTS rows are
normalized before comparison, so partial, duplicate, or same-sized replacement rows cannot pass
as a consistent index.
Refresh setup is staged before the asynchronous build task is launched. If startup, capability
negotiation, generation creation, task launch, or shutdown fails at that boundary, the
provisional generation is abandoned and its build reservation is released while the last
complete active generation remains available.
Cancellation
requests received before inventory startup returns its control handle are retained and applied
once the handle is available.
Cancellation requests carry a source label through the gateway adapter into the native client.
Diagnostic logs identify inventory startup boundaries, pause and foreground transitions, and
health/controller state transitions at the default informational level. Maintenance and pacing
waits, health-probe details, and bounded native-operation entry/return records — including browse
paths, item names, durations, iterator results, and failures — are debug-level diagnostics to
avoid high-volume production logs. Successful foreground browse-page completions are also
debug-level records, so a large indexed traversal does not fill the normal production log with
one entry per page.

Read responses contain semantic values. For an OPC DA `VT_BSTR`, the gateway forwards the exact
BSTR contents without adding display quote characters; quotes remain only when present in the
server value.

Configure the index in the gateway TOML file under `[index]`. Automatic indexing is restricted to
the explicit `servers` allow-list and uses a service-writable SQLite database, conservative
batch/rate/duty-cycle defaults, a two-second foreground quiet period, and one build at a time.
Native inventory batches are bounded to 1,000 entries by the OPC DA client contract.
Native inventory slicing and SQLite commit batching are independently bounded, and adaptive
controller decisions update the native slice batch size and item-rate limit; native operation
cost applies the rate once (one item for DA2 operations and the requested page size for DA3),
without a gateway-side burst/token limiter or an additional batch-size delay before every
operation. The commit interval provides a time limit for low-volume inventories. Runtime status
includes rolling
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
