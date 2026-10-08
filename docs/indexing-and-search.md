# Indexing and search operations

The gateway's persistent namespace index accelerates repeated tag discovery without storing live
tag values. Live browse and live search remain available independently of the index.

## Enrollment and generation lifecycle

A fresh gateway has no enrolled servers. Start an index with `index-refresh` using the exact ProgID
returned by `opcda-bridge-client servers`. The gateway validates that ProgID before persisting
enrollment. A successful manual build creates a durable active generation; the gateway schedules
that enrolled server for refresh according to its per-server auto-refresh setting and the global
`index.enabled` and `index.paused` policies. The default interval is seven days. Automatic indexing does not start a
first build on an un-enrolled server.

Refreshes write to a staging generation. Promotion is an atomic metadata transition: the previous
complete generation remains searchable during a refresh, and a failed or cancelled refresh does
not replace it. A failed initial build remains visible as failed because there is no complete
generation to preserve. A successful inventory may carry a non-fatal warning for skipped
malformed branches; the generation remains ready and the warning remains visible in status.

Status distinguishes `not-indexed`, `partial`, `ready`, `stale`, `refreshing`, `failed`, and
`deleting`. A `deleting` status is temporary while the gateway removes the enrollment and index
data. A no-match result is authoritative only when the index is complete. A non-fatal completion
warning does not by itself make a usable active generation failed.

## Index implementation

`index.rs` is the `IndexManager` facade and retains stable public type re-exports. Internal
responsibilities are split across enrollment, scheduling and cleanup, traversal and health,
SQLite storage and generation promotion, indexed search and ranking, and status and metrics
modules. The test suite follows the same domains under `index/tests/`, with shared fixtures in
`index/tests/mod.rs`.

## Client commands

Use the live `search` command when the gateway should traverse the OPC server. It streams results
in browse order, reports progress, and can return a truncation warning when result or visit limits
are reached. `index-search` queries the persistent index and never falls back to a live traversal.

```sh
opcda-bridge-client --host 192.168.1.50:7600 index-status \
  --server Kepware.KepServerEX.V5
opcda-bridge-client --host 192.168.1.50:7600 index-search Device1 \
  --server Kepware.KepServerEX.V5 --match-mode contains
opcda-bridge-client --host 192.168.1.50:7600 index-refresh \
  --server Kepware.KepServerEX.V5
```

`index-status --watch 5` polls status every five seconds. Refresh runs asynchronously. The client
also exposes `index-pause`, `index-resume`, and `index-cancel` for an active build. The Rust library
and protocol include additional per-server index controls.

Indexed queries return exact ItemIDs and breadcrumb labels, not browse-session node keys. Matching
is case-insensitive and ranks exact, prefix, and contains matches. `has_more` indicates that
additional ranked matches exist past the requested result limit.

## Database and generation safety

Each gateway process that indexes servers must use a unique, explicit `index.database_path`.
Launching another process with the same configuration can otherwise target the same SQLite file.
The default path is `%PROGRAMDATA%\\opcda-bridge\\index.sqlite3` on Windows and
`$XDG_DATA_HOME/opcda-bridge/index.sqlite3` on Linux/macOS, falling back to
`$HOME/.local/share/opcda-bridge/index.sqlite3`.

Each server's build uses a persistent sibling `.build.lock` file. On Windows, `.build.owner`
stores process metadata because the locked file may not be readable. The operating-system advisory
lock determines whether a build is active; the sidecar's existence alone does not. A clean lock
release removes `.build.owner`; forced termination may leave it for the next acquisition to
overwrite. Do not delete either file while a gateway may be running.

All writes to an index database share a database-wide writer gate, including build progress,
failure updates, and cleanup batches. Cleanup uses a separate SQLite WAL connection, defers while
builds are active, yields between bounded batches, and retries transient failures. Search uses a
read-only connection and bounded candidate sets so broad search work does not block status,
discovery, reads, writes, or lazy browse. Database and build-lock identities use canonical file
paths where available, preventing relative-path and symlink aliases from bypassing coordination.

Startup migrates older index schemas transactionally through the schema 2-to-3-to-4 sequence.
Existing generations and full-text data are preserved. Servers with a usable active generation
are enabled for scheduled refresh; failed-only histories require a manual retry before automatic
refresh is enabled.

## Pacing and safety controls

The gateway's default profile uses 256-entry native inventory slices, 1,024-entry SQLite commit
batches, a 1,000 ms commit interval, no item-rate pacing, a 100% duty cycle, and one build at a
time. Native slice size and SQLite commit size are independent controls.

`index.item_rate_limit` and `index.duty_cycle_percent` are separate:

- `item_rate_limit = 0` disables item-rate pacing. A nonzero rate is forwarded to the native
  item-rate limiter and charged by each inventory operation's item cost.
- `duty_cycle_percent = 100` removes intentional pauses between active work periods.
- The gateway leaves the native minimum operation interval at zero. Do not derive a sleep interval
  from `batch_size / item_rate_limit`.

Adaptive AIMD pacing is opt-in (`index.adaptive = false` by default). When enabled, the gateway can
throttle or pause inventory in response to foreground latency, OPC health, host load, storage
pressure, and recent errors or bad-quality reads. Health probes include server capabilities and
latency; an optional sentinel tag adds a tag-read health check. Without a sentinel tag, sentinel
health is reported as unavailable while capability and latency checks can still permit indexing.
Maintenance windows, startup grace, schedule jitter, retry backoff, storage headroom, and
per-operation timeouts remain active independently of adaptive pacing.

`index.inventory_root` restricts a build to that exact canonical ItemID. `index.worker_count`
defaults to one and is bounded at four. Multiple namespace workers are used only when the server
provides a complete, session-backed hierarchical root page with at least two expandable branches;
otherwise the gateway uses one full-root inventory.

The complete set of options and defaults is documented in the
[gateway example configuration](../crates/opcda-bridge-gateway/opcda-bridge-gateway.example.toml).
The gateway-wide `index.enabled` switch controls startup and scheduled work. Manual status, browse,
search, refresh, and read operations remain available while it is disabled. Per-server scheduled
refresh can be disabled without deleting the searchable generation.

`index.enabled` defaults to `true` and `index.paused` defaults to `false`. Explicit configuration
values take precedence. Both switches govern the background scheduler; neither is changed by a
per-server enable/disable request. Disabling a server's preference does not cancel an active
build; use the separate cancel control for that operation.

The saved `auto_refresh_enabled` preference is not proof that scheduling is permitted.
`scheduler.auto_refresh_policy` reports `allowed`, `disabled`, or `paused`, with disabled taking
precedence when both administrative blockers apply. A blocked scheduler reports no next-refresh
date. This configuration policy is separate from foreground, health, and operator pauses on an
active build. A missing policy diagnostic from an older gateway means unknown, not disabled.

## Large-namespace acceptance runbook

Use this sequence before a full refresh on a large or production namespace:

1. Identify the active gateway process, listener, configuration, and index database. Keep the
   protected port `7600` listener and its database untouched. Diagnostic sidecars use port `7602`
   and a separate database path.
2. Stage a checksum-verified official gateway artifact in a new versioned directory. Before
   replacing a diagnostic sidecar, preserve its executable, configuration, database, `-wal` and
   `-shm` files, logs, and build-lock owner metadata. Verify the backup manifest and hashes.
3. Confirm the sidecar's exact process command line, listener ownership, database path, gateway
   status, server discovery, a known read, and indexed search. Set `paused = false` before asking
   it to build.
4. Run a short canary. Confirm status and foreground reads remain responsive, the previous active
   generation remains searchable, progress writes and commit batches advance, cleanup does not
   block progress, and cancelling the canary preserves the previous active generation.
5. Start the complete refresh only after the canary passes. Monitor gateway and OPC host CPU,
   memory, disk activity, storage headroom, process lifetime, foreground read latency and quality,
   and index progress. Stop on an unsafe or unexplained regression.
6. Accept the run only when a new generation is ready. Record start/end time, duration, entries
   seen, unique items, persisted entries, generation number, active/paused time, effective item
   rate, batch sizes, duty cycle, adaptive changes, warnings, host load, read quality/latency,
   search results, browse behavior, restart persistence, and cleanup results.

A running process or open listener is not proof of acceptance. The promoted generation's persisted
entry counts are authoritative; progress events can be asynchronous. A warning for skipped
malformed branches is acceptable only when the generation reaches ready and the warning remains
visible. Run one heavy build at a time, keep monitoring lightweight and bounded, check host
memory/swap before a full refresh, and collect metrics incrementally. Record the process IDs and
command lines before any authorized restart.

### Recorded field measurements

The 2026-08-26 handover records an HP/Kepware acceptance of approximately 284 unique entries in
72.8 seconds, with discovery, hierarchical browse, exact ItemIDs, Good-quality reads, indexed
search, and restart persistence verified. Foreground reads measured approximately 22 ms p50 and
30 ms p95/maximum.

The same handover records a prior Yokogawa build of 6,741,689 entries over approximately 12 hours
26 minutes. A later canary against gateway release 0.4.9 failed before the first inventory entry
was processed when an index progress write encountered SQLite writer contention with cleanup; the
existing active generation remained available. At the handover, the separate sidecar's active
generation was recorded as generation 1 with 6,741,689 entries; query the database and listener
again before any operational action. The previous successful build is a load reference, not
evidence that a release containing the writer-coordination fix passes acceptance.

## Related references

- [Gateway deployment, service, and firewall](gateway-deployment.md)
- [Protocol and compatibility](protocol-and-compatibility.md)
- [Troubleshooting](troubleshooting.md)
