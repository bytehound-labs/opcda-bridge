# opcda-bridge

[![CI](https://github.com/bytehound-labs/opcda-bridge/actions/workflows/checks.yml/badge.svg)](https://github.com/bytehound-labs/opcda-bridge/actions/workflows/checks.yml)
[![codecov](https://codecov.io/gh/bytehound-labs/opcda-bridge/branch/main/graph/badge.svg?token=)](https://codecov.io/gh/bytehound-labs/opcda-bridge)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=bytehound-labs_opcda-bridge&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=bytehound-labs_opcda-bridge)
[![opcda-bridge on crates.io](https://img.shields.io/crates/v/opcda-bridge.svg)](https://crates.io/crates/opcda-bridge)
[![opcda-bridge on docs.rs](https://docs.rs/opcda-bridge/badge.svg)](https://docs.rs/opcda-bridge)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](https://doc.rust-lang.org/edition-guide/rust-2024/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A lightweight Rust gateway bridging classic OPC DA (Windows/COM) servers to remote Linux/macOS/Windows clients.

## Status

Active development. Gateway and client are functional end-to-end — OPC DA read/write/browse passing
against a live Kepware server. The workspace publishes the reusable client library, protocol
definitions, cross-platform CLI, and Windows gateway as separate crates. Each crate is versioned
independently, and release-plz publishes only packages with releasable changes after the generated
metadata passes the required release integrity check.

The indexed API line introduces indexed namespace search and extends the gRPC capabilities contract.
The additive wire protocol remains compatible with older gateways and clients. Indexed-search
availability is determined by the advertised protocol and capability versions, not by matching
crate or binary version numbers.
Public Rust struct additions in the pre-1.0 API are source-breaking for downstream struct
literals, so releases containing those additions use the next minor API version rather than a
patch version. The protobuf wire additions remain backward-compatible.

### Versions and compatibility

The published packages have independent SemVer versions and package-specific tags:

- `opcda-bridge-proto` — generated protocol types.
- `opcda-bridge` — reusable Rust client library.
- `opcda-bridge-client` — cross-platform CLI.
- `opcda-bridge-gateway` — Windows OPC DA gateway.

All four packages publish to crates.io. GitHub Releases and prebuilt archives are created only for
the client and gateway tags. A client release and a gateway release therefore do not need to carry
the same version number; choose and pin each binary independently. Client/gateway interoperability
is defined by the wire protocol, `protocol_version`, indexed-search protocol and capability flags,
and the compatibility checks in CI. Changes to a reusable dependency can also produce a dependent
package release when the dependent binary or API needs to be rebuilt.

The cross-version compatibility workspace pins each historical client to its original protocol
crate version. This keeps older Rust clients compiling against the schema they were released
with while the tests exercise their wire compatibility with current gateways.

## Why

OPC DA (OLE for Process Control, Data Access) is a Windows-only, COM/DCOM-based industrial protocol still running on countless PLCs, DCSs, and SCADA systems that predate its successor, OPC UA.

`opcda-bridge` aims to be the same idea distilled to a single static Rust binary per side:

- **Gateway** — runs on the Windows host alongside the OPC DA server, speaks native COM.
- **Client** — runs anywhere (Linux, macOS, Windows), talks to the gateway over the network.

## Client/gateway compatibility

Check a deployed pair before using optional protocol features:

```sh
opcda-bridge-client --host 192.168.1.50:7600 compatibility
opcda-bridge-client --host 192.168.1.50:7600 compatibility \
  --require namespace --require indexed-search
```

The check uses the gateway-wide protocol handshake and does not contact an OPC DA server. For an
older gateway that predates that handshake, provide `--server` (or configure a default server) to
infer compatibility from its legacy capabilities response. A `full` result means all advertised
features overlap; `partial` means core read/write operations overlap while an optional feature does
not; `incompatible` means core or a required feature cannot be negotiated; and `unknown` means the
gateway cannot describe itself. Overlapping but untested package pairs are reported as
`unverified` and remain usable. The report includes both the client binary version and the reusable
library version implementing its protocol contract.

The [compatibility catalog](COMPATIBILITY.md) documents protocol release lines and boundary-test
evidence. The machine-readable [`compatibility.json`](compatibility.json) file is suitable for
deployment tooling. Client and gateway package versions are independent and do not need to match.

## Installation

Prebuilt client and gateway binaries are attached to their package-specific tags on the
[Releases](https://github.com/bytehound-labs/opcda-bridge/releases) page.

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
  The package uses standard Arch `pkgver-pkgrel` versioning and versioned source filenames to
  avoid reusing stale local `makepkg` or `yay` cache files between releases.
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
  `cargo install opcda-bridge-client --version 0.4.3` to pin a specific published version. The
  client and gateway versions are independent, so pinning one does not select the other.

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
- Inspect the gateway's browse/search capabilities for a server:
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 capabilities --server Kepware.KepServerEX.V5
  ```
- Check client/gateway protocol compatibility without opening an OPC DA server:
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 compatibility
  ```
  Add `--require namespace` or `--require indexed-search` when deployment requires those optional
  features. Older gateways can be checked with `--server Kepware.KepServerEX.V5`, which enables
  legacy capability inference.
- Browse one bounded page of immediate children. The root request opens a session; use the returned
  opaque session, node, and continuation values unchanged to expand a branch or load another page:
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 browse --server Kepware.KepServerEX.V5 --page-size 200
  opcda-bridge-client --host 192.168.1.50:7600 browse --server Kepware.KepServerEX.V5 \
    --session-id SESSION --parent-node-key NODE_KEY
  opcda-bridge-client --host 192.168.1.50:7600 browse --server Kepware.KepServerEX.V5 \
    --session-id SESSION --page-token PAGE_TOKEN
  ```
  `--all` follows continuation tokens explicitly and stops at the `--max-results` safety cap
  (10,000 by default). It can be expensive and is not used for normal tree navigation.
- Search the live namespace independently of tree browsing. This compatibility/diagnostic path
  traverses the OPC server; results arrive progressively, progress is written to stderr, and
  Ctrl+C drops the active stream. Exact ItemIDs in results remain the identities used for
  read/write:
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 search Device1 \
    --server Kepware.KepServerEX.V5 --match-mode contains
  ```
  A normal search stream starts with an initial progress event, emits matches in browse order
  with progress updates after each page, and ends with a completion event. Result or visit caps
  can terminate the stream early with a truncation warning.
- Use the persistent gateway-owned index for fast interactive discovery. Only servers explicitly
  allowed by the gateway configuration can be indexed, and indexed search never falls back to
  live traversal:
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 index-status \
    --server Kepware.KepServerEX.V5
  opcda-bridge-client --host 192.168.1.50:7600 index-search Device1 \
    --server Kepware.KepServerEX.V5 --match-mode contains
  opcda-bridge-client --host 192.168.1.50:7600 index-refresh \
    --server Kepware.KepServerEX.V5
  ```
  Operators can use `index-pause`, `index-resume`, and `index-cancel` for an active build.
  Refreshes run asynchronously, and responses distinguish `not-indexed`, `partial`, `ready`,
  `stale`, `refreshing`, and `failed` states. A completed inventory may also carry a non-fatal
  warning while remaining `ready`; clients display that diagnostic as a warning rather than
  treating the active generation as failed. A no-match response is authoritative only for a
  complete index.
  Active generations remain durable across restarts. Activation is an atomic metadata transition;
  promotion status uses a read-only SQLite connection and filesystem diagnostics, so status remains
  responsive even while the writer is in the promotion critical section. Superseded and abandoned
  data is reclaimed in bounded background batches through a database-wide writer gate shared with
  every build mutation, including builds for other servers using the same database file. Cleanup
  defers while a build is active, yields between batches so a build can proceed, and resumes pending
  requests after the last active build finishes, even when the cleanup request was scheduled by a
  different manager instance sharing the same database file, so indexing maintenance does not
  interrupt indexed search requests or compete with progress/failure writes. Deferred cleanup also
  exits cleanly if gateway shutdown begins before it starts waiting for build completion. Transient
  cleanup stops before starting another write batch once shutdown is requested, and a batch that
  finds no remaining obsolete rows makes no write. Transient cleanup failures are retried with
  bounded backoff, and pending cleanup requests remain tracked until a completed pass confirms
  that no rerun is needed. Persisted retry deadlines take precedence
  after a restart, so a failed server is not retried immediately just because the gateway was
  restarted. A refresh interrupted by restart is superseded when a complete
  active generation remains available, so the durable snapshot stays ready while cleanup runs;
  foreground operations are reference-counted per server; indexing stays paused while any
  foreground user is active and remains paused through the configured quiet period after the last
  foreground operation ends.
  interrupted initial builds and genuine refresh failures remain visible as failed. Older failed
  generations do not make a newer active generation appear failed. If relational index rows and
  the full-text index disagree after an interrupted legacy startup repair, the rebuildable cache
  is quarantined rather than serving silently incomplete substring results. Validation compares
  both row counts and the stored server/generation/item/display values plus normalized breadcrumb
  values. Relational breadcrumbs are JSON arrays while the FTS copy uses space-separated
  searchable text; legacy JSON-form FTS rows are normalized too, while partial, duplicate, and
  same-sized replacement rows are still detected.
  Status combines the persisted generation snapshot with runtime build, health, storage,
  foreground, and scheduler diagnostics. During promotion, persisted status is read through a
  read-only connection; a runtime error overrides the reported state only when no build is active.
  Database coordination and persistent build-lock paths use the canonical identity of the database
  file, so existing-file aliases such as relative paths and symlinks cannot bypass coordination.
  If the file and its parent cannot be canonicalized, the original path spelling is retained.
  Independent in-memory databases are not shared through the registry and do not create filesystem
  build-lock sidecars.
  Indexed queries use a dedicated read-only SQLite connection and bounded candidate sets, keeping
  broad searches out of the foreground database mutex. Exact searches use separate equality
  lookups on ordered normalized display-name and ItemID indexes, exclude display-name matches
  from the lower-priority ItemID lookup, then merge and deduplicate those candidates before
  ranking; this avoids sorting a whole common-name result set before applying the limit. Prefix
  searches use separate lexicographic range probes over the normalized indexes. Each probe is
  bounded to `limit + 1` rows; prefix bounds handle Unicode scalar boundaries, including the
  surrogate gap and the maximum scalar value; contains searches use the trigram FTS index.
  During promotion, searches reuse the active generation reported by promotion-safe status rather
  than reacquiring the writable database mutex. Cancellation requests received while inventory
  startup is still acquiring its control handle are retained and applied as soon as that handle
  becomes available.
  Cancellation requests carry a source label through the gateway adapter into the native client,
  and diagnostic logs cover startup boundaries, pause/foreground/health/controller transitions,
  maintenance and pacing waits, and bounded native operation entry/return details. Transition
  records are informational; maintenance, pacing, health-probe, and actionable operation details
  use debug-level logging, while high-frequency native iterator refill and per-entry diagnostics
  remain at trace level to keep normal production logs bounded. These records distinguish gateway
  scheduling and cancellation cleanup from a native browse or event-delivery stall without
  changing protocol behavior.
  Event-delivery waits use the configured operation timeout; when that deadline expires, the
  gateway requests native cancellation with an `inventory_event_timeout` source label before
  recording the build failure.
  If refresh setup fails or shutdown wins before the background build task starts, the provisional
  generation is abandoned and the build reservation is released without disturbing the last
  complete active generation.
  Before each inventory event, a build passes its maintenance-window, health, and adaptive
  recovery gates. Health probes check server capabilities, latency, and the optional sentinel
  tag; an unhealthy server pauses the build with bounded exponential backoff, while a healthy
  recovery resumes it with the configured pacing. Without a sentinel tag, the health status is
  reported as `Unavailable` while capability and latency checks can still permit the build.
  Healthy probe observations carry no failure detail; failure reasons are populated only for
  capability, sentinel, or latency failures.
  The configured item rate is enforced once by native-operation cost: DA2 operations cost one
  item, while a DA3 page is charged by the number of entries requested. The slice batch size
  selects the page size but does not add a separate delay before every native operation, so DA2
  traversal is not throttled twice. There is no separate gateway burst limiter. The legacy
  `index.burst_size` setting is accepted for configuration compatibility but has no effect.
  Cancellation, probe failure, or a rejected pacing update stops the build and preserves the last
  complete generation. Pending entries are
  flushed before terminal state is recorded, and successful completion or a non-fatal inventory
  warning remains distinct from a failed or cancelled build.
  Matching is case-insensitive with exact/prefix/contains ranking, and responses report
  `has_more` when the requested result window is exceeded. This preserves status, discovery,
  reads, writes, and lazy browse responsiveness during search.
  If the native client rejects an initial or adaptive pacing update, the active build fails
  visibly and the prior complete generation is preserved; pacing errors are not ignored.
  DA3 root ItemIDs and unused filters are marshalled as required non-null empty strings. If the
  first DA3 root browse still returns `RPC_X_NULL_REF_POINTER` or `E_NOTIMPL`, a server that also
  supports DA2 continues through DA2 with an explicit compatibility warning. The persisted
  generation records that negotiated fallback explicitly, so a genuine DA2-only server that later
  gains DA3 support still triggers normal profile-change invalidation.
  DA2 hierarchical inventory validates every server-reported branch before queueing it.
  Branch-only names rejected by native navigation with `E_INVALIDARG` are skipped and included in
  the completion warning, while names that resolve to exact items remain selectable.
- Release a browse session before its gateway-side expiry:

  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 close-browse-session SESSION
  ```

- Read one or more tag values:
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 read --server Kepware.KepServerEX.V5 Simulink.Device1.Python.D
  ```
- Write a value to a tag (parsed automatically as bool, int, float, or string):
  ```sh
  opcda-bridge-client --host 192.168.1.50:7600 write --server Kepware.KepServerEX.V5 Simulink.Device1.Python.D 42
  ```

Every command prints human-readable output by default. Browse output includes the session ID,
namespace organization/source, completeness, warning, and next-page token so partial results are
never presented as complete. Pass `--output json` (or its shorthand, `--json`) for machine-readable
output instead — see
[JSON output](#json-output) below. `--host`, `--config`, `--output`, and `--json` may be placed
either before or after the subcommand (e.g. both `--json read ...` and `read ... --json` work).
Run `opcda-bridge-client --help` or `opcda-bridge-client <command> --help` for the full flag
reference.

### JSON output

Every command accepts `--output json` (or the shorthand `--json`) for scripting, CI, or piping
into `jq`. Most commands print a pretty-printed JSON array:

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

Read values are semantic strings. For an OPC DA `VT_BSTR`, the `value` field contains the exact
BSTR contents: `AUT` is returned as `AUT`, an empty BSTR is empty, and quote characters are
preserved only when they are part of the BSTR itself.

`browse` emits a metadata object containing `session_id`, `nodes`, `next_page_token`, `complete`,
`organization`, `source`, `warning`, and `pages`. Each node keeps its opaque `node_key`, local
`display_name`, typed `kind`, and optional exact `item_id` separate. Only selectable `item` and
`branch_and_item` nodes expose an ItemID; branch-only nodes use their opaque key for navigation
without presenting private DA3 navigation identifiers as selectable tags. `search` emits
newline-delimited JSON events so matches and progress remain streaming rather than waiting for the
full search.
`index-search` emits one object containing ranked `matches`, `has_more`, and full index `status`;
its matches contain exact ItemIDs and breadcrumb labels but no session-bound node keys.
Commands that fail print a structured `{"error": "..."}` object to stderr instead of the usual
`Error: ...` text, and the process still exits non-zero.

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
[`opcda-bridge`](https://crates.io/crates/opcda-bridge) directly instead. It exposes typed
capabilities, one-page browse requests/responses, explicit session close, cancellable live-search
streams, persistent indexed-search status/refresh/control/query methods, and read/write operations
without `clap`, `tabled`, `serde_json`, or `toml` pulled in transitively — just
`opcda-bridge-proto`, `tonic`, `tonic-prost`, `uuid`, and `thiserror`. Add it to a project with:

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

The persistent namespace index is opt-in by server: index operations are accepted only for ProgIDs
in `index.servers`, and automatic indexing never scans any other server. A valid complete
generation remains available while a refresh runs, and failed or cancelled refreshes never replace
it.
When an existing populated database is missing a required secondary index or the trigram full-text
index, gateway startup records that preparation is required instead of creating large objects
during ordinary operation. Ordinary database opens inspect the schema and index definitions but do
not scan every relational and FTS row, so status and search remain responsive on multi-million-entry
databases. Stop the gateway and run the one-shot maintenance command
`opcda-bridge-gateway --config PATH index-prepare`; this is a one-shot command that exits after
preparation without starting the gateway or contacting OPC DA. It creates and populates the
missing objects transactionally, validates them before commit, and records a retryable failure marker if
preparation cannot complete. Explicit preparation performs the full relational/FTS consistency check.
Empty databases prepare automatically, and status remains available while preparation is required.
Unexpected index definitions fail initialization, and inconsistent full-text data is quarantined
rather than served as an incomplete cache. The preparation command is database-only, can run on a
non-Windows host, and returns a nonzero status when preparation fails. It uses the normal gateway
CLI/configuration parsing but does not initialize COM or open a listener; the gateway's normal OPC
DA serving mode still requires Windows.

| Index setting              | Config key                            | Default                        |
| -------------------------- | ------------------------------------- | ------------------------------ |
| Database path              | `index.database_path`                 | Platform data directory        |
| Automatic indexing         | `index.enabled`                       | `true`                         |
| Indexed server allow-list  | `index.servers`                       | Empty                          |
| Refresh interval           | `index.refresh_interval_seconds`      | `604800` (7 days)              |
| First automatic build      | `index.initial_build_policy`          | `maintenance_window`           |
| Startup grace period       | `index.startup_grace_period_seconds`  | `30` seconds                   |
| Schedule jitter            | `index.schedule_jitter_seconds`       | `21600` seconds                |
| Inventory slice batch      | `index.inventory_batch_size`          | `100` entries (max `1000`)     |
| SQLite commit batch        | `index.commit_batch_size`             | `100` entries                  |
| SQLite commit interval     | `index.commit_interval_ms`            | `1000` ms                      |
| Legacy batch size fallback | `index.batch_size`                    | `100` (max `1000`)             |
| Average item rate          | `index.item_rate_limit`               | `250` items/second             |
| Legacy burst allowance     | `index.burst_size`                    | accepted but ignored           |
| Active duty cycle          | `index.duty_cycle_percent`            | `20`%                          |
| Adaptive pacing            | `index.adaptive`                      | `true`                         |
| Adaptive canary profile    | `index.canary_*`                      | `50` items/s, batch `25`, `5`% |
| Adaptive floor profile     | `index.minimum_*`                     | `10` items/s, batch `1`, `1`%  |
| Foreground quiet period    | `index.quiet_period_seconds`          | `2` seconds                    |
| Health probe interval      | `index.health_probe_interval_seconds` | `30` seconds                   |
| Health latency threshold   | `index.health_latency_threshold_ms`   | `500` ms                       |
| OPC operation timeout      | `index.operation_timeout_seconds`     | `30` seconds                   |
| Sentinel health tag        | `index.sentinel_tag`                  | Unavailable when omitted       |
| Minimum free space         | `index.minimum_free_space_bytes`      | `100 MiB`                      |
| Storage headroom           | `index.storage_headroom_bytes`        | `10 MiB`                       |
| Maintenance windows        | `index.maintenance_windows`           | Empty                          |
| Concurrent builds          | `index.concurrency`                   | `1`                            |
| Query-cache capacity       | `index.query_cache_capacity`          | `256` entries                  |
| Start paused               | `index.paused`                        | `false`                        |
| Maximum indexed results    | `index.max_results`                   | `50`                           |

The default database locations are `$XDG_DATA_HOME/opcda-bridge/index.sqlite3` (falling back to
`$HOME/.local/share/opcda-bridge/index.sqlite3`) on Linux/macOS and
`%PROGRAMDATA%\\opcda-bridge\\index.sqlite3` on Windows. Maintenance-window entries are local
24-hour ranges such as `22:00-06:00`; when configured, indexing is deferred outside those ranges.
Adaptive indexing uses recent foreground OPC errors and bad-quality reads as health signals in
addition to latency and host/storage guardrails.
The status reports whether a sentinel tag is configured separately from whether its latest probe
is healthy or unavailable.

Run only one gateway process with a given index database path. A gateway automatically loads
`opcda-bridge-gateway.toml` next to its executable, so launching a second copy from the same
directory can otherwise start a second inventory against the same SQLite file. When multiple
gateway instances are intentional, give each instance an explicit, different
`index.database_path` and configure indexing on only the instance that should build that
server's index. Each server's build uses a persistent sibling `.build.lock` file. On Windows,
`.build.owner` carries the same owner metadata because the locked file may be unreadable; it is
removed on a clean lock release and may remain after forced termination until the next acquisition
overwrites it. The operating-system advisory lock, not either file's existence, determines whether
a build is active. Do not delete either path while a gateway may still be running.

### Client

Looks for a config file in a platform-specific location unless `--config` gives another path:

- Linux/macOS: `$XDG_CONFIG_HOME/opcda-bridge/client.toml`, falling back to
  `$HOME/.config/opcda-bridge/client.toml`.
- Windows: `%APPDATA%\opcda-bridge\client.toml`.

See
[`crates/opcda-bridge-client/client.example.toml`](crates/opcda-bridge-client/client.example.toml)
for every available key.

| Setting               | CLI flag                     | Env var             | Config key                 | Default                               |
| --------------------- | ---------------------------- | ------------------- | -------------------------- | ------------------------------------- |
| Gateway address       | `--host`                     | `OPC_BRIDGE_HOST`   | `host`                     | `localhost:7600`                      |
| Default OPC DA server | `--server`                   | —                   | `server`                   | none — must be set one way or another |
| Browse page size      | `browse --page-size`         | —                   | `page_size`                | `200`                                 |
| Browse `--all` cap    | `browse --all --max-results` | —                   | `browse_all_limit`         | `10000`                               |
| Search result cap     | `search --max-results`       | —                   | `search_max_results`       | `200`                                 |
| Index search cap      | `index-search --max-results` | —                   | `index_search_max_results` | `50`                                  |
| Output format         | `--output` / `--json`        | `OPC_BRIDGE_OUTPUT` | `output`                   | `table`                               |

`server` has no built-in default: if it is left unset by every source,
`capabilities`/`browse`/`search`/`read`/`write` fail rather than guessing a server.

## Logging

The gateway writes structured logs to a rolling file next to its executable — by default
`logs/opcda-bridge-gateway.<date>.log` — through a non-blocking writer, so logging never adds
latency to request handling. Daily rotation uses `YYYY-MM-DD`, hourly rotation uses
`YYYY-MM-DD-HH`, and `never` uses `logs/opcda-bridge-gateway.log`. When a console is attached
(running interactively, as opposed to under a background/service process), the same log lines
are also printed to stdout.

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
console mode: the SCM reports `Running` only after the listener is ready, and `stop` drains
in-flight requests before it reports `Stopped`.

## Architecture

- Gateway (Windows-only) built on the ByteHound-maintained [`bytehound-opc-da-client`](https://github.com/bytehound-labs/opc-cli/tree/main/opc-da-client) package for the COM/OPC DA layer — no dependency on proprietary SDKs (OPC Labs QuickOPC, Graybox, Matrikon, etc.).
- Client (cross-platform) is a plain network client with no COM/Windows dependency.
- Scope is intentionally OPC DA only for now — see [`AGENTS.md`](AGENTS.md) for the reasoning.

## Contributing

All changes, including documentation-only fixes, use a short-lived feature branch and focused
pull request. Start from synchronized `main`, keep one logical change group per PR, run the
applicable checks, and repair the same branch until every required status is green. Pull requests
are squash-merged only after the applicable SonarQube analysis reports zero `OPEN`/`CONFIRMED`
issues; intentional Accepted or False Positive findings need a durable rationale and related
link. After merging, wait for the `main` workflows and SonarQube analysis before starting
dependent work. See [CONTRIBUTING.md](CONTRIBUTING.md) for the complete workflow and coding
standards.

CI is change-aware: documentation-only changes do not rebuild the workspace, while required status
checks still complete for branch protection. Release-plz compatibility lockfile updates are
proposed as checked `release-plz-*` pull requests rather than pushed directly to `main`.

CI validates Rust code and package metadata, checks Protobuf compatibility against `main` with Buf,
runs CodeQL and Semgrep analysis, scans complete Git history with the open-source Gitleaks CLI,
and audits workflow files with actionlint and zizmor. Configuration, output, and protocol
boundaries have property tests plus standalone cargo-fuzz smoke targets.

SonarQube Cloud analyzes the Rust workspace for maintainability, reliability, security, complexity,
and duplication issues. Rust coverage is imported from the same `cargo llvm-cov` LCOV report used
by the coverage workflow; integration tests, fuzz targets, the compatibility workspace, and build
output are classified or excluded so they do not distort source coverage. Relevant pull requests
and pushes to `main` run the analysis, with a full scan every Wednesday at 04:47 UTC and an
available manual dispatch. Fork pull requests report an intentional skip because repository
secrets are unavailable. If a scan fails after its report is uploaded, the workflow log includes
the Compute Engine response and the projects visible to the configured analysis token, allowing
project-identity and server-side processing failures to be distinguished without exposing the
token.

With the SonarScanner CLI installed and `SONAR_TOKEN` exported, reproduce the analysis locally:

```sh
cargo llvm-cov --workspace --locked --lcov --output-path lcov.info
sonar-scanner
```

Tagged binary releases include SHA-256 checksums, a CycloneDX SBOM, keyless Sigstore signatures,
and GitHub artifact provenance attestations. Running the release workflow manually builds and
uploads packages as workflow artifacts without creating a GitHub release. The client and gateway
use their own package tags; protocol and library releases remain crates.io releases without binary
archives.

## License

[MIT](LICENSE)
