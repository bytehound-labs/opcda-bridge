# opcda-bridge

A Windows-side gateway that speaks native OPC DA (COM/DCOM) to industrial control
systems, plus a cross-platform (Linux/macOS/Windows) client that talks to the gateway over
the network — a single static binary per side, no legacy dependency stack.

## Status

Active development. Workspace contains gateway (`opcda-bridge-gateway`), client
(`opcda-bridge-client`), a reusable client library (`opcda-bridge`), and shared proto
definitions (`opcda-bridge-proto`). Gateway and client are functional end-to-end against a live
Kepware server.

## Origin and scope discipline

This project started as an exploration of building an OPC DA bridge in Rust, then briefly considered a
much broader scope (a generic industrial protocol multiplexer supporting OPC UA/DA/Modbus/etc.,
similar to [`ng-gateway`](https://github.com/shiyuecamus/ng-gateway) or Telegraf). **Deliberately
scoped back down to OPC DA only** to avoid never shipping a v1. Resist re-expanding scope to other
protocols until an OPC DA MVP (gateway + client, read/write working end-to-end) actually ships. If
a generic multiplexer is revisited later, it should be a separate project/crate built _on top of_
a working opcda-bridge, not a redesign of it.

## Key architectural decisions

- **Build on the ByteHound-maintained [`bytehound-opc-da-client`](https://github.com/bytehound-labs/opc-cli/tree/main/opc-da-client)
  package** (MIT, async/trait-based, `windows-rs`-backed) for the COM/OPC DA layer, rather than reimplementing raw
  OPC DA COM interfaces from scratch. Its lower-level sibling
  [`opc_da`](https://github.com/Ronbb/rust_opc) (also MIT) is a fallback/reference if
  `opc-da-client` proves insufficient. `opc_da`'s repo bundles OPC Foundation IDL files under the
  OPC Foundation's own license terms (separate from its overall MIT license) — worth a re-read if
  this ever becomes a legal question.
- **No proprietary OPC SDKs.** Deliberately do not depend on OPC Labs QuickOPC or Graybox's
  `gbda_aut.dll` — both carry licensing terms incompatible with redistribution in an open-source
  project (the author's FalconTune/AccuTune `Main` repo hit this exact wall — see that repo's
  history if the reasoning needs re-deriving).
- **Use native lazy OPC DA browsing for hierarchical servers.** The gateway requests one bounded
  level at a time through the scalable browse API in the `opc-da-client` fork. DA 3.0 continuation
  points and DA 2.x browse positions remain on their native session; the gRPC layer exposes only
  gateway-owned opaque session, page, and node tokens. Flat namespaces are reported as flat rather
  than reconstructed into a potentially incomplete hierarchy, and exact ItemIDs remain separate
  from display names.
- **Index databases are process-scoped resources.** Every gateway process that can index a server
  must use a unique, explicit `index.database_path`; an executable launched next to the shared
  TOML automatically loads that file, so two direct launches can otherwise build into the same
  SQLite database. Gateway logs include the process ID, resolved database path, server,
  generation, operation, and terminal build outcome to make this class of deployment error
  diagnosable.
- **Inventory failures are terminal, typed failures.** The native inventory worker catches
  unexpected panics, logs the payload type without exposing panic contents through the public
  protocol, and delivers an `OpcError` to the stream. Fixed-size COM iterator buffers validate
  every reported count before indexing so malformed native counts cannot panic the worker.
- **Indexed search is isolated from foreground database coordination.** Uncached full-text
  queries open a read-only SQLite connection outside the process-wide writable database mutex,
  retain only a bounded ranked candidate set, and fetch metadata for the final result page.
  Search must never make status, discovery, reads, writes, or lazy browse wait on a broad query.
- **Architecture split**: Gateway (Windows-only, COM) + cross-platform client talking to it over
  the network.
- **Compatibility contract**: Client and gateway package versions are independent. Runtime
  compatibility is negotiated by the gateway-wide `GetGatewayInfo` protocol-feature ranges, with
  legacy `GetCapabilities` fallback only when an explicit OPC server is supplied. The canonical
  release-line catalog is `crates/opcda-bridge-proto/compatibility.toml`; generated
  `COMPATIBILITY.md` and `compatibility.json` must stay synchronized.

## Reference test environment

Manually validated (not automated/CI) against: a Windows host running Kepware KEPServerEX, OPC
server `Kepware.KepServerEX.V5`, tag `Simulink.Device1.Python.D`. This is a good smoke-test
target for early gateway development.

## Conventions

- **Trunk-based git flow**: single long-lived `main` branch, short-lived PR branches, squash
  merges, no `develop`/release branches, releases tagged directly off `main`. Contrast with the
  author's FalconTune/AccuTune repos, which use a `dev`-branch + `--no-ff` merge model — do not
  carry that convention over here.
- **Commits**: [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`,
  `chore:`, etc.).
- **Formatting/linting**: `cargo fmt` (default settings) and
  `cargo clippy --all-targets --all-features -- -D warnings`, once code exists.
- Full contributor workflow is documented in [`CONTRIBUTING.md`](CONTRIBUTING.md); this file is
  for future coding-agent sessions, not human contributors.

## Build / Test / Lint / Coverage

- **Build**: `cargo build`
- **Test**: `cargo test --workspace`
- **Lint**: `cargo fmt --check --all` and
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- **Coverage**: `cargo llvm-cov --workspace --lcov`
- **Cross-version compatibility**: `cargo test --manifest-path compatibility-tests/Cargo.toml --locked`
- **Report drift**: `python3 scripts/generate-compatibility-report.py --check`

### Security and release validation

All GitHub Actions in `.github/workflows/` are pinned to immutable commit SHAs. Change-aware
security workflows run CodeQL, Semgrep, full-history Gitleaks, actionlint, zizmor, Buf
Protobuf compatibility checks, and bounded cargo-fuzz smoke tests. Tagged binary releases
publish SHA-256 checksums, a CycloneDX SBOM, keyless Sigstore signatures, and GitHub artifact
provenance attestations; `workflow_dispatch` builds package artifacts without publishing.

The gateway crate is Windows-only (COM); the client crate is cross-platform. Tests that require
the `OpcClient` trait use a mock implementation so they run on all platforms.

The cross-version workflow exercises published 0.3.2 and 0.4.0 boundary clients against current
and historical gateway services backed by mock `OpcClient` implementations. An exact package pair
does not need prior CI evidence when its negotiated protocol ranges overlap, but the CLI reports
such pairings as `unverified`. Release-plz pull requests regenerate the isolated test workspace's
lockfile before running the same locked test command when package manifests change, because path
package versions change in the release branch. The release workflow commits the matching lockfile
after a release commit so the main branch remains runnable with `--locked`.

An intentional Protobuf break requires the `breaking-protobuf` label, a new or changed catalog
boundary, updated evidence, and regenerated compatibility reports. Release-integrity validation
rejects publishable package versions that do not fall within exactly one catalog release line.

### Coverage enforcement

Coverage is tracked by Codecov and enforced at **100%** via `codecov.yml` (project and patch
targets both at 100% with a 1% threshold). CI fails if coverage drops. All code — including error
branches, edge cases, and default values — must be tested. When adding new code, add
corresponding tests in the same PR to maintain 100% coverage.

### Test design for the gateway

The gateway binary (`opcda-bridge-gateway`) depends on `opc-da-client` only on Windows
(`#[cfg(target_os = "windows")]`). To keep the gateway's core logic testable on all platforms,
an `OpcClient` trait (in `crates/opcda-bridge-gateway/src/opc.rs`) abstracts OPC DA operations. The concrete
adapter (`opc_da_adapter.rs`) wraps the native browse-session client and is Windows-only. The run
loop (`crates/opcda-bridge-gateway/src/run.rs`) that serves the gRPC service and drains it on shutdown is generic
over `OpcClient` too, so it runs under tests on any platform even though the gateway only ships
for Windows. Both `server`'s and `run`'s tests share one `MockOpcClient`
(`crates/opcda-bridge-gateway/src/test_support.rs`) to exercise all RPC handler and shutdown paths without touching
COM.

Index lifecycle tests must cover an active build with an obsolete runtime error, exact database
operation diagnostics, failed/cancelled generation cleanup, read-only search access, and
deterministic full-text ranking. Operational SQLite errors such as lock contention must not be
treated as corrupt-cache evidence and must not trigger quarantine.

### Config file precedence

Both binaries resolve every configurable setting with **CLI flag > environment variable >
config file > built-in default** precedence
(`crates/opcda-bridge-gateway/src/config.rs`, `crates/opcda-bridge-client/src/config.rs`).
To keep this composable while satisfying the 100% coverage gate:

- Path discovery (`config_path_from_exe` / `config_path_from`) is a pure function taking
  environment values as explicit arguments rather than reading `std::env` inline, so every
  permutation is testable without the `ENV_MUTEX` dance.
- `load_config_file(path, missing_is_error)` takes an explicit bool rather than inferring intent:
  an auto-discovered path silently falls back to defaults when absent (`missing_is_error =
false`), but an explicit `--config` path is a hard error if missing (`true`). Malformed TOML is
  always a hard error regardless.
- To layer a config file _underneath_ clap's own `CLI > env` resolution, config-backed fields
  drop their clap `default_value` and become `Option<T>`; a `.or(config_value).unwrap_or(default)`
  chain is applied once after parsing.

### Logging

`crates/opcda-bridge-gateway/src/logging.rs` builds a layered `tracing-subscriber` registry: a non-blocking rolling
file writer (`tracing-appender`) is always on, and a stdout layer is added only when
`std::io::stdout().is_terminal()` — a Windows service has no console, so the file layer must never
depend on one being present. The `WorkerGuard` returned by `tracing_appender::non_blocking` must
be held for the process lifetime (a local binding in `main()` that lives until the function
returns is enough); dropping it early silently truncates buffered log lines on exit. Settings
resolve through the same CLI > env > config > default precedence as the rest of the config
surface, with `RUST_LOG` folded into `--log-level` via clap's `env` attribute exactly like
`--port`/`OPC_BRIDGE_PORT`.

`init_tracing` installs a process-global subscriber, and `try_init()` can only succeed once per
process — `cargo test` runs every unit test in a crate in one shared process, so which test
"wins" that single real installation is not deterministic. To stay testable under the 100%
coverage gate anyway, `init_tracing` is a thin wrapper around
`init_tracing_with_stdout(settings, attach_stdout: bool)`: "is a console attached" becomes an
explicit, injectable parameter instead of being read inline. Tests drive every layer-construction
branch (JSON vs. pretty format, stdout attached vs. detached) directly through that `bool` and
deliberately never assert whether `try_init()` returned `Ok` or `Err` — only that the code path
leading up to it runs.

### Windows service

`crates/opcda-bridge-gateway/src/service.rs` follows the same "extract a testable pure representation, map it onto
the real Windows type in a thin shim" pattern as `logging.rs`. The top of the file is
platform-neutral and covered by Linux tests: `ServiceDefinition`/`build_service_definition` (what
to register), `service_launch_arguments` (which CLI flags become the service's permanent launch
arguments), `ServiceLifecycle` (a plain mirror of `windows_service::service::ServiceState` that
pins down the intended `StartPending → Running → StopPending → Stopped` reporting order, with
`Running` emitted only after listener readiness), and
`is_scm_launch_error_code` (the raw `ERROR_FAILED_SERVICE_CONTROLLER_CONNECT` check, kept as a
plain `Option<i32>` comparison rather than matching on the Windows-only `windows_service::Error`
directly). Only the `#[cfg(target_os = "windows")] mod windows_impl` submodule — the actual
`ServiceManager`/`service_dispatcher`/`service_control_handler` glue — is invisible to the Linux
coverage run, and it is kept intentionally thin: `install`/`uninstall`/`start`/`stop`/`status`
each just map a `ServiceDefinition`/`SERVICE_NAME` onto the matching `windows_service` API call.

`main()` cannot call into itself from library code (the bin and lib are separate compilation
units), so the config/logging/COM/listener bootstrap that both console mode and the service entry
point need lives in `run::run_gateway()`, not in `main.rs`. `main()` is now a thin dispatcher:
parse `Cli`, dispatch any `ServiceCommand` subcommand to `service::*`, otherwise try
`service::run_as_service()` and fall back to `run::run_gateway()` only when
`service::is_run_outside_scm` recognizes the "not launched by the SCM" error — the same
`run_gateway()` call console mode always made, just reached from a second entry point.

Reporting `StopPending` happens from _inside_ the async `shutdown` future passed to
`run_gateway`, at the instant the control handler's oneshot channel resolves — before the actual
request-drain begins, not after `run_gateway` returns (too late to be meaningful) and not from
inside the control handler closure itself (which doesn't have the `ServiceStatusHandle` yet,
since that's the return value of the same `register()` call the closure is passed to).
`ServiceStatusHandle` is `Clone` and documented safe to use from any thread, so the closure's
registration and the shutdown future's status report use two independent clones of the same
handle.

`install` requires flags before the subcommand (e.g. `opcda-bridge-gateway.exe --port 7700
install`, not `install --port 7700`) since the SCM always launches a service's executable bare —
whatever the operator wants applied every time the service starts must be baked into the
registration itself via `service_launch_arguments`, not left to how the process happened to be
invoked once at install time.

### Client output formats

`crates/opcda-bridge-client/src/output.rs` holds `OutputFormat` (`Table`/`Json`) and the two pure functions every
command routes through: `render<T: Tabled + Serialize>(rows, format)` and `format_error(err,
format)`. Command-specific rows and browse/search event structures derive serialization separately,
so JSON keys remain the Rust field names — the external contract for scripted consumers — while
table headers stay controlled separately via `#[tabled(rename = "...")]`.
`WriteRow.error` is `Option<String>` rather than the pre-JSON code's `.unwrap_or_default()`
collapse to `""`, so JSON can distinguish "no error" (`null`) from an actual empty string; the
table rendering still shows an empty cell for `None` via `#[tabled(rename = "Error",
display("display::option", ""))]` (the built-in `tabled::derive::display::option` helper, which
takes the fallback-for-`None` string as its second argument).

`--json` is a plain boolean flag, not linked to `--output` via clap's `conflicts_with` —
`conflicts_with` can misfire against env-sourced values, so precedence between the two is instead
resolved once in code (`output::resolve_from_cli`: `--json` always wins if set, otherwise
`--output`) rather than left to clap. Browse JSON emits page/session/completeness metadata, while
search JSON emits newline-delimited progressive events.

`host`/`config`/`output`/`json` on `Cli` are all `#[arg(..., global = true)]`, so they can be
passed either before or after the subcommand (clap's default otherwise requires top-level flags
to precede the subcommand token, which is easy to trip over when a flag like `--json` is added
after a command's positional args out of habit).

`main()` can no longer return `anyhow::Result<()>` once errors need format-aware rendering:
`lib::run() -> ExitCode` resolves the CLI-only output format (`output::resolve_from_cli`) _before_
calling `config::load_config`, so a config-load failure — which happens before the config file's
own `output` key could ever be known — still gets reported in a sensible format (`--json`/
`--output`/`OPC_BRIDGE_OUTPUT`, or `table` if none of those were given). Only once the config
loads successfully does the fully-resolved `CLI > env > config > default` format apply to the
command's own result. `run()` itself is a thin `Cli::parse()` wrapper around the real, unit-tested
entry point `run_with_cli(cli: Cli) -> ExitCode` — the same "inject the otherwise-unparseable
input as a parameter" pattern `logging.rs` uses for `is_terminal()` — so every branch (success,
command error, config-load error, both output formats) is covered by tests that construct a `Cli`
directly instead of needing to control `std::env::args()`. The one line real coverage can't reach
that way — `run()`'s own body, which only ever runs from the compiled binary — is covered instead
by `client/tests/main_integration.rs` spawning the real binary against a closed local port
(`127.0.0.1:1`, an immediate, deterministic connection refusal) so the full `run() ->
run_with_cli() -> fail()` path executes at least once outside of unit tests, alongside the
pre-existing `--help` integration test (which alone doesn't reach this: clap exits the process
from inside `Cli::parse()` before `run_with_cli` is ever called).

### Client library (`opcda-bridge`)

The reusable client library lives in `crates/opcda-bridge` and owns the typed
capabilities, browse-session, search, connect/list-servers/read/write API extracted from the CLI's
`crates/opcda-bridge-client/src/commands.rs`. It has no CLI presentation dependencies, so
downstream Rust applications can depend on `opcda-bridge` without pulling in `clap`, `tabled`,
`serde_json`, or `toml`.

- **API surface**: `Client::connect(host: &str)`, `.capabilities(server)`, `.browse(server,
page_size)`, `.browse_page(request)`, `.close_browse_session(session_id)`, `.search_stream(request)`,
  `.list_servers()`, `.read(server, tags)`, and `.write(server, tag, value)` return typed Rust values
  (`Capabilities`, `BrowsePage`, `SearchStream`, `Vec<String>`, `Vec<TagValue>`, and `WriteResult`).
- **Paging contract**: browse methods request one page and never automatically follow continuation
  tokens. Use `BrowsePageRequest::next` or an explicit application-level collection loop when bulk
  results are intended.
- **Dependency boundary**: `opcda-bridge` depends on the published `opcda-bridge-proto`,
  `tonic`, `tonic-prost`, `uuid`, and `thiserror`. Runtime crates such as `tokio` and
  `tokio-stream` are only dev-dependencies because browse and search use tonic streams directly.
- **Error contract**: `Error::Connect(tonic::transport::Error)` and `Error::Rpc(tonic::Status)`
  use transparent error rendering so the CLI's existing error output remains unchanged.
- **Published distribution**: `opcda-bridge` is consumed from crates.io with a normal SemVer
  dependency (`opcda-bridge = "0.4"`). Git dependencies are not part of the supported consumer
  path.
- **OPC DA client publication**: Publish `bytehound-opc-da-client` through
  `.github/workflows/publish-opcda-client.yml` from an authenticated GitHub CLI session rather
  than requiring local Cargo registry credentials. Pass the exact upstream `opc-cli` ref and run
  the workflow with `dry_run=true` first; after it succeeds, rerun with `dry_run=false`. The
  Windows workflow supplies the repository's `CARGO_REGISTRY_TOKEN` secret only to the publishing
  step. Never paste or print that token.
- **Release automation**: the four published crates are independently versioned. release-plz runs
  separate release-PR and publish jobs, creates package-specific tags, publishes only packages
  with releasable changes, and cascades releases through the configured
  `changelog_include` dependency edges when a reusable library or protocol change requires a
  dependent rebuild. GitHub Releases are enabled only for the client and gateway packages; the
  protocol and reusable library publish to crates.io without binary archives. The
  `release_commits` allowlist excludes both scoped and unscoped release-plz commit forms; the
  package-aware `release-integrity` check rejects release PRs containing only generated metadata,
  and a crates.io rate limit bounds publishing if another guard regresses. Client/gateway runtime
  compatibility is defined by protocol and capability versions, not equal crate versions.
