# opcda-bridge

A Windows-side gateway that speaks native OPC DA (COM/DCOM) to industrial control
systems, plus a cross-platform (Linux/macOS/Windows) client that talks to the gateway over
the network — a single static binary per side, no legacy dependency stack.

## Status

Active development. Workspace is scaffolded with gateway (`opcda-bridge-gateway`), client
(`opcda-bridge-client`), shared proto definitions (`bridge-proto`), and an umbrella root crate
(`opcda-bridge`). Gateway and client are functional end-to-end against a live Kepware server.

## Origin and scope discipline

This project started as an exploration of building an OPC DA bridge in Rust, then briefly considered a
much broader scope (a generic industrial protocol multiplexer supporting OPC UA/DA/Modbus/etc.,
similar to [`ng-gateway`](https://github.com/shiyuecamus/ng-gateway) or Telegraf). **Deliberately
scoped back down to OPC DA only** to avoid never shipping a v1. Resist re-expanding scope to other
protocols until an OPC DA MVP (gateway + client, read/write working end-to-end) actually ships. If
a generic multiplexer is revisited later, it should be a separate project/crate built _on top of_
a working opcda-bridge, not a redesign of it.

## Key architectural decisions

- **Build on the [`opc-da-client`](https://github.com/wends155/opc-cli) crate** (MIT,
  async/trait-based, `windows-rs`-backed) for the COM/OPC DA layer, rather than reimplementing raw
  OPC DA COM interfaces from scratch. Its lower-level sibling
  [`opc_da`](https://github.com/Ronbb/rust_opc) (also MIT) is a fallback/reference if
  `opc-da-client` proves insufficient. `opc_da`'s repo bundles OPC Foundation IDL files under the
  OPC Foundation's own license terms (separate from its overall MIT license) — worth a re-read if
  this ever becomes a legal question.
- **No proprietary OPC SDKs.** Deliberately do not depend on OPC Labs QuickOPC or Graybox's
  `gbda_aut.dll` — both carry licensing terms incompatible with redistribution in an open-source
  project (the author's FalconTune/AccuTune `Main` repo hit this exact wall — see that repo's
  history if the reasoning needs re-deriving).
- **Architecture split**: Gateway (Windows-only, COM) + cross-platform client talking to it over
  the network.

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

The gateway crate is Windows-only (COM); the client crate is cross-platform. Tests that require
the `OpcClient` trait use a mock implementation so they run on all platforms.

### Coverage enforcement

Coverage is tracked by Codecov and enforced at **100%** via `codecov.yml` (project and patch
targets both at 100% with a 1% threshold). CI fails if coverage drops. All code — including error
branches, edge cases, and default values — must be tested. When adding new code, add
corresponding tests in the same PR to maintain 100% coverage.

### Test design for the gateway

The gateway binary (`opcda-bridge-gateway`) depends on `opc-da-client` only on Windows
(`#[cfg(target_os = "windows")]`). To keep the gateway's core logic testable on all platforms,
an `OpcClient` trait (in `gateway/src/opc.rs`) abstracts OPC DA operations. The concrete
adapter (`opc_da_adapter.rs`) wraps `opc_da_client::OpcDaWrapper` and is Windows-only. The run
loop (`gateway/src/run.rs`) that serves the gRPC service and drains it on shutdown is generic
over `OpcClient` too, so it runs under tests on any platform even though the gateway only ships
for Windows. Both `server`'s and `run`'s tests share one `MockOpcClient`
(`gateway/src/test_support.rs`) to exercise all RPC handler and shutdown paths without touching
COM.

### Config file precedence

Both binaries resolve every configurable setting with **CLI flag > environment variable >
config file > built-in default** precedence (`gateway/src/config.rs`, `client/src/config.rs`).
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

`gateway/src/logging.rs` builds a layered `tracing-subscriber` registry: a non-blocking rolling
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

`gateway/src/service.rs` follows the same "extract a testable pure representation, map it onto
the real Windows type in a thin shim" pattern as `logging.rs`. The top of the file is
platform-neutral and covered by Linux tests: `ServiceDefinition`/`build_service_definition` (what
to register), `service_launch_arguments` (which CLI flags become the service's permanent launch
arguments), `ServiceLifecycle` (a plain mirror of `windows_service::service::ServiceState` that
pins down the intended `StartPending → Running → StopPending → Stopped` reporting order), and
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
