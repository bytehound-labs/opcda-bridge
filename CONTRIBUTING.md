# Contributing to opcda-bridge

Use focused pull requests, preserve the OPC DA safety boundary, and follow the checks below.

## Development workflow: trunk-based

- Synchronize a clean local `main`, then create a short-lived `<type>/<short-description>` branch.
  `main` is the only long-lived branch; there is no `develop` or long-lived release branch.
- Keep one logical change per PR, including documentation-only fixes. Work that must land before
  its behavior is ready stays behind a Cargo feature flag.
- Run targeted and applicable full validation. Fill in the PR template with exact commands,
  results, and any required manual verification.
- Repair failures on the same branch until every applicable required check passes. If `main`
  advances, update the branch before merging; never bypass protection or push directly to `main`.
- Squash-merge after maintainer approval and passing checks. The squash message must follow the
  commit convention below. Wait for post-merge workflows and applicable main Sonar analysis before
  starting dependent work.

Applicable PR Sonar analyses must report zero `OPEN`/`CONFIRMED` issues. Query
`bytehound-labs_opcda-bridge` with an explicit `pullRequest=<number>` before merge and `branch=main`
after the merge analysis completes, using `issueStatuses=OPEN,CONFIRMED`. Authenticate with
`SONAR_TOKEN` from the ignored local `.env`; never print the token. An Accepted or False Positive
finding requires an
evidence-backed rationale and related link. Do not use `NOSONAR`, broad exclusions, weakened quality
profiles, or dashboard-only suppression to hide real findings.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <description>`.

Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`.
Example: `feat(gateway): add tag subscription support`.

## Development setup and style

- Use stable Rust and put `protoc` on `PATH`. The separate MSRV job checks Rust 1.88.0; cargo-fuzz
  smoke tests use nightly.
- Build the Windows gateway with the explicit MSVC toolchain and 32-bit target in
  [gateway installation](docs/gateway-deployment.md#install-the-gateway), not the host default.
  The cross-platform client is independently built and remains x86_64 on Windows.
- Use default rustfmt settings and fix Clippy warnings; an intentional `#[allow(...)]` needs a
  justification.
- Run `lefthook install` after cloning. The [hooks](.lefthook.yml) format staged Rust, TOML,
  Markdown, YAML, JSON, and shell files, run Rust checks, and synchronize secrets through the
  configured dotenv workflow. Never commit `.env` or credentials.

## Validation

Run the smallest checks covering the change, then every gate it can affect:

```sh
cargo fmt --check --all
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo llvm-cov --workspace --locked --lcov --output-path lcov.info
cargo deny check
cargo machete
cargo build --workspace --locked
cargo package --workspace --locked --no-verify
```

The gateway's native COM adapter is Windows-only; its RPC, lifecycle, and index logic is tested
cross-platform through [`OpcClient`](crates/opcda-bridge-gateway/src/opc.rs) and shared mocks.
Workspace tests require no OPC hardware. Live OPC DA/DCOM verification is outside CI; document
the manual steps when a change requires it. Do not write tags or refresh a production namespace
merely to validate a documentation change.

[Codecov](codecov.yml) targets 100% project and patch coverage with a one-percentage-point status
threshold; preserve that policy. The [workflows](.github/workflows) filter expensive jobs by changed
paths. Root documentation-only changes skip compiled validation and coverage; a packaged crate
README also triggers archive validation and Sonar analysis. Fork PRs skip secret-bearing Sonar
analysis.

The archive job adds validation-only local patches for unpublished same-version workspace crates.
`--no-verify` assembles archives rather than compiling their extracted sources; Linux and Windows
jobs compile the workspace when Rust inputs change. Packaged manifests retain real registry
version requirements. Do not use those patches for publication.

Security checks cover CodeQL, Semgrep, full-history Gitleaks, actionlint, zizmor, and bounded fuzz
smoke tests. Protobuf checks run on PRs and pushes to `main`.

### Protobuf changes

From the repository root:

```sh
buf lint
buf format --diff --exit-code
buf breaking --against '.git#ref=refs/remotes/origin/main'
```

The `except` entries in `buf.yaml` cover names that are already part of the published wire
contract: the `bridge` package and its file layout, the `Bridge` service, and the shared or
domain-named RPC response messages. Renaming those elements would change gRPC method paths or the
generated Rust API, so they remain documented lint exceptions rather than lint fixes.

Buf uses `STANDARD` rules and compares the wire contract with `main`; there is no label-based
bypass. An intentional wire break also requires the
[protocol version and evidence changes](docs/protocol-and-compatibility.md#versioning-and-protocol-changes).

## Releases

The four workspace crates release independently from `main` with package-specific tags. The client
and gateway have prebuilt archives; protocol and reusable-library releases are crates.io-only.
Use the canonical references:

- [Release flow and verification](docs/release.md): release-plz, dependency-order publication,
  native-client publisher ownership, archive checksums/signatures/provenance, and dry-run limits.
- [Rust API and wire versioning](docs/protocol-and-compatibility.md#versioning-and-protocol-changes):
  pre-1.0 API breaks, workspace dependency requirements, negotiated features, and pair evidence.
- [AUR package](docs/release.md#aur-package): generated client package, versioned source filenames,
  and `pkgver`/`pkgrel`.

Release workflow manual dispatch is a packaging dry run. Release-plz dispatch is not: only run
publication as part of an explicitly approved release. Do not create intermediate releases or
publish directly with `cargo publish`.

## Reporting security issues

Do not open public issues or pull requests for security vulnerabilities. Report them privately as
described in [SECURITY.md](SECURITY.md).

## License

By contributing, you agree your contributions are licensed under the project's [MIT
license](LICENSE). No CLA/DCO sign-off required.
