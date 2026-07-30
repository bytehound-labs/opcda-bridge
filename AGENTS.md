# opcda-bridge

A Rust take on the [openopc2](https://github.com/iterativ/openopc2) concept: a Windows-side
gateway that speaks native OPC DA (COM/DCOM) to industrial control systems, plus a cross-platform
(Linux/macOS/Windows) client that talks to the gateway over the network — a single static binary
per side, no Python/Pyro5/legacy dependency stack.

## Status

Pre-implementation as of repo creation (2026-07). No `Cargo.toml`/`src/` yet — architecture and
key dependencies are decided (below), but nothing has been scaffolded or coded.

## Origin and scope discipline

This project started as an exploration of rewriting `openopc2` in Rust, then briefly considered a
much broader scope (a generic industrial protocol multiplexer supporting OPC UA/DA/Modbus/etc.,
similar to [`ng-gateway`](https://github.com/shiyuecamus/ng-gateway) or Telegraf). **Deliberately
scoped back down to OPC DA only** to avoid never shipping a v1. Resist re-expanding scope to other
protocols until an OPC DA MVP (gateway + client, read/write working end-to-end) actually ships. If
a generic multiplexer is revisited later, it should be a separate project/crate built _on top of_
a working opcda-bridge, not a redesign of it.

## Key architectural decisions

- **Do not copy or transliterate `openopc2`'s Python source.** It's GPL-2.0-or-later; a
  derivative work would inherit that license. This is a clean-room rewrite based on
  understanding its _behavior/architecture_, not its code.
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
  the network — mirrors `openopc2`'s gateway/client split, not its implementation.

## Reference test environment

Manually validated (not automated/CI) against: a Windows host running Kepware KEPServerEX, OPC
server `Kepware.KepServerEX.V5`, tag `Simulink.Device1.Python.D`. This exact combination proved
out `openopc2` end-to-end in a prior session and is a good smoke-test target for early gateway
development.

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

## Build / Test / Lint

Not yet applicable — no `Cargo.toml` exists. Once scaffolded: `cargo build`, `cargo test`,
`cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`. The gateway crate
will be Windows-only (COM); the client crate should be cross-platform.
