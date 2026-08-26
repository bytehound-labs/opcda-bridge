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
- SonarQube Cloud analyzes the Rust workspace, compatibility support, and fuzz targets for
  maintainability, reliability, security, complexity, and duplication issues. Reproduce its
  coverage input locally with `cargo llvm-cov --workspace --locked --lcov --output-path lcov.info`,
  then run `sonar-scanner` with `SONAR_TOKEN` exported. The workflow runs for relevant pull
  requests and pushes to `main`, plus a Wednesday 04:47 UTC weekly scan; fork pull requests
  intentionally skip the secret-bearing analysis.

## Testing

- Unit-test protocol/parsing logic with `cargo test --workspace` — no hardware required.
- The gateway depends on `opc-da-client` only on Windows (`#[cfg(target_os = "windows")]`). Its
  core logic is abstracted behind an `OpcClient` trait
  (`crates/opcda-bridge-gateway/src/opc.rs`) so it stays testable
  on any OS: tests exercise a hand-written `MockOpcClient` instead of a live COM connection.
- Hardware-in-the-loop tests (against a real Windows host + OPC DA server) aren't part of CI;
  note manual verification steps in the PR description when a change needs them.
- Cross-version protocol checks run from the isolated `compatibility-tests/` workspace:
  `cargo test --manifest-path compatibility-tests/Cargo.toml --locked`.
  The workflow regenerates that workspace's lockfile when package manifests change because path
  dependencies carry the branch's package versions, and release automation commits the matching
  lockfile after a package release.

## CI

PRs must pass `cargo fmt --check --all`, `cargo clippy --workspace --all-targets --all-features
-- -D warnings`, and `cargo test --workspace` before merge. The gateway crate is Windows-only
(COM); the client crate should build and test on Linux, macOS, and Windows.

The workflows are change-aware: compiled validation runs for Rust, workspace, dependency, script,
or workflow changes; package archive validation also runs when crate metadata or packaged
documentation changes. The archive job uses `cargo package --workspace --locked --no-verify`
because same-version internal dependencies may not be published until the release PR merges; the
Linux and Windows jobs compile the current workspace sources. Documentation-only changes keep the
required `check` and `coverage` statuses green without rebuilding the workspace.

Security workflows use immutable action pins and bounded aggregate statuses. They run CodeQL,
Semgrep, full-history Gitleaks, actionlint, zizmor, Protobuf breaking-change checks, and bounded
fuzz smoke tests when their inputs change. Tagged binary releases receive checksums, an SBOM,
keyless signatures, and provenance; use the release workflow's manual dispatch for packaging
validation without publishing. Intentional Protobuf wire-contract breaks must carry the
`breaking-protobuf` label; without that explicit approval, the Buf compatibility check blocks the
pull request.

The generated compatibility files must remain synchronized with
`crates/opcda-bridge-proto/compatibility.toml`:

```sh
python3 scripts/generate-compatibility-report.py --check
```

The catalog describes protocol release lines rather than equal package versions. An intentional
breaking Protobuf change requires the `breaking-protobuf` label, a new or changed compatibility
boundary, updated boundary evidence, and regenerated reports. The Buf compatibility workflow
enforces those requirements.

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

The four crates are versioned independently and use package-specific tags
(`opcda-bridge-client-vX.Y.Z`, for example). `release-plz` runs separate release-PR and publish
jobs, updates only packages with releasable Conventional Commits, and publishes crates in
dependency order. `changelog_include` entries cascade a reusable library or protocol change to
the dependent packages that must be rebuilt, without restoring workspace-wide lockstep versions.
The generated release metadata must pass the package-aware `release-integrity` check, while the
release commit filter and crates.io rate limit provide additional safeguards.

Every publishable package version must fall within exactly one catalog release line. Client and
gateway binary Releases include `COMPATIBILITY.md` and `compatibility.json` so operators can inspect
the same protocol evidence offline.

All four crates publish to crates.io. GitHub Releases and prebuilt platform archives are reserved
for the client and gateway tags; the protocol crate and reusable library do not produce binary
archives. Client/gateway compatibility is defined by the wire protocol and advertised capability
versions rather than matching package versions. Published crate names and versions are permanent;
a yanked version cannot be reused. The AUR package is generated by `scripts/aurpkg` from the
client-specific tag and uses standard Arch `pkgver-pkgrel` versioning. Its generated source
filenames include the package version so cached files from an earlier release cannot cause
checksum failures; packaging-only changes use a bumped `pkgrel`.

## License

By contributing, you agree your contributions are licensed under the project's [MIT
license](LICENSE). No CLA/DCO sign-off required.
