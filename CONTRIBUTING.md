# Contributing to opcda-bridge

opcda-bridge is under active development. The practices below apply to all contributions.

## Development workflow: trunk-based

- `main` is the only long-lived branch and should always be green (builds, passes CI).
- Work happens on short-lived branches named `<type>/<short-description>` (e.g.
  `feat/gateway-rpc-protocol`, `fix/quality-flag-mapping`), opened as a PR and merged within a
  day or two — not long-running feature branches.
- PRs are squash-merged, so the squash commit message (not the intermediate commits) must follow
  the commit convention below.
- No `develop` branch and no long-lived `release` branches. Releases are tagged directly off
  `main` (`<component>-vX.Y.Z`, [SemVer](https://semver.org/)).
- Incomplete or experimental work that must land before it's fully ready goes behind a Cargo
  feature flag rather than sitting unmerged on a branch.
- Trivial fixes (typos, doc tweaks) may be pushed directly to `main`; everything else goes
  through a PR so CI runs.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <description>`.

Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`.
Example: `feat(gateway): add tag subscription support`.

## Code style

- Format with `cargo fmt --all` (default rustfmt settings) before committing.
- Lint with `cargo clippy --workspace --all-targets --all-features -- -D warnings`; fix every
  warning or justify an explicit `#[allow(...)]` with a comment.
- Both are enforced automatically by a [lefthook](https://github.com/evilmartians/lefthook)
  `pre-commit` hook (`.lefthook.yml`), which also formats `Cargo.toml`/TOML with `taplo` and
  Markdown/YAML/JSON with `prettier`. Run `lefthook install` once after cloning to enable it.

## Testing

- Unit-test protocol/parsing logic with `cargo test --workspace` — no hardware required.
- The gateway depends on `opc-da-client` only on Windows (`#[cfg(target_os = "windows")]`). Its
  core logic is abstracted behind an `OpcClient` trait
  (`crates/opcda-bridge-gateway/src/opc.rs`) so it stays testable
  on any OS: tests exercise a hand-written `MockOpcClient` instead of a live COM connection.
- Hardware-in-the-loop tests (against a real Windows host + OPC DA server) aren't part of CI;
  note manual verification steps in the PR description when a change needs them.

## CI

PRs must pass `cargo fmt --check --all`, `cargo clippy --workspace --all-targets --all-features
-- -D warnings`, and `cargo test --workspace` before merge. The gateway crate is Windows-only
(COM); the client crate should build and test on Linux, macOS, and Windows.

## Pull requests

- Keep PRs small and focused — one logical change each.
- Describe what changed and why; link an issue if one exists.
- Squash-merge once CI is green.

## Releases

The workspace publishes four crates to crates.io:

- `opcda-bridge-proto` — generated gRPC protocol types.
- `opcda-bridge` — reusable async Rust client library.
- `opcda-bridge-client` — cross-platform CLI.
- `opcda-bridge-gateway` — Windows OPC DA gateway.

`release-plz` creates release pull requests, publishes crates in dependency order, and creates
SemVer tags and GitHub releases. The binary release workflow uses the client and gateway tags to
build platform archives. Published crate names and versions are permanent; a yanked version
cannot be reused.

## License

By contributing, you agree your contributions are licensed under the project's [MIT
license](LICENSE). No CLA/DCO sign-off required.
