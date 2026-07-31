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
