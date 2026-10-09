# Agent instructions

`opcda-bridge` contains a Windows-only OPC DA gateway, a cross-platform gRPC client, a reusable
Rust library, and shared protocol types. The repository is intentionally scoped to OPC DA; do not
expand it into a generic industrial-protocol gateway.

## Safety and operations

- The gateway is unauthenticated, serves plaintext gRPC, and binds `0.0.0.0:7600`. Its Windows
  service runs as `LocalSystem`. Do not deploy it on an untrusted network; do not imply that
  installation creates a firewall rule.
- Port `7600` is protected. Diagnostic sidecars use port `7602`, with a separate executable,
  configuration, log directory, and index database. Never let gateway processes share an index
  database path.
- Do not stop, replace, or deploy a gateway, run a full production index refresh, or issue an OPC
  DA write without explicit authorization. Identify the exact process, command line, listener, and
  database before operational changes. Never terminate a process by name alone.
- The official gateway artifact is 32-bit `i686-pc-windows-msvc`, including on 64-bit Windows.
  Do not substitute a host-target x64 build.
- Do not create intermediate releases, tags, or publications. Release changes go through the
  approved release workflow.
- The native dependency `bytehound-opc-da-client` is maintained in
  [`bytehound-labs/opc-cli`](https://github.com/bytehound-labs/opc-cli), outside this workspace.
  Its `Publish ByteHound OPC DA client` workflow and protected `crates-publish` environment are
  the sole publication path. Do not add a duplicate publisher here or bypass that environment.
  This repository's `release-plz` workflow publishes workspace crates; `release.yml` builds
  binary archives.
- Keep the deployment threat model accurate; see
  [gateway deployment](docs/gateway-deployment.md), [indexing operations](docs/indexing-and-search.md),
  and [the security policy](SECURITY.md).

## Change contract

- Work only in the requested worktree and branch. Preserve unrelated changes and other agents'
  worktrees. Never push directly to `main` or bypass branch protection.
- Keep each pull request focused, use Conventional Commits, and squash-merge through GitHub after
  the required checks pass. Do not change the local Git identity.
- Add or update user-facing documentation with behavior, configuration, protocol, safety, or
  operational changes. Generated compatibility reports are not hand-edited.
- Preserve the branch-protection required status contexts: `check`, `coverage`,
  `release-integrity`, and `Required Sonar quality status`.
- Never commit secrets, credentials, machine-local state, caches, build output, or temporary
  artifacts.
- Protocol changes must preserve published Protobuf names, field numbers, and wire types. An
  intentional wire break requires the `breaking-protobuf` label, a compatibility-catalog boundary,
  evidence, and regenerated reports.

## Build and validation commands

Use stable Rust and install `protoc` for workspace builds. The declared MSRV is Rust 1.88. The
cargo-fuzz smoke workflow uses nightly.

```sh
cargo build --workspace --locked
cargo fmt --check --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --locked
cargo llvm-cov --workspace --lcov --output-path lcov.info
cargo deny check
cargo machete
cargo test --manifest-path compatibility-tests/Cargo.toml --locked
python3 scripts/generate-compatibility-report.py --check
cargo package --workspace --locked --no-verify
```

Build the supported Windows gateway target explicitly:

```powershell
rustup target add i686-pc-windows-msvc
cargo build --release --locked -p opcda-bridge-gateway --target i686-pc-windows-msvc
```

## Code and test constraints

- Keep gateway OPC operations behind the `OpcClient` abstraction so RPC and lifecycle behavior can
  be tested cross-platform. The concrete COM adapter is Windows-only; use the shared mock client
  in tests.
- Add focused tests with new behavior and preserve the repository's complete source-line coverage
  gate. Operational SQLite lock errors are not corrupt-cache evidence and must not trigger
  quarantine.
- Keep config precedence as CLI > environment > TOML > built-in default. Missing auto-discovered
  config files are allowed; malformed existing files and missing explicit paths are errors.
- Preserve the CLI's JSON contract: product output goes to stdout, command errors to stderr, and
  browse/search progress remains streamable. Exact ItemIDs and opaque browse tokens are protocol
  values; do not infer hierarchy by splitting tag punctuation.
- Treat protocol-feature versions and exact-pair test evidence as separate from crate versions.
  Update the compatibility catalog and generated report whenever a protocol boundary changes.
- All usable enrolled indexes participate under gateway `index.enabled` / `index.paused` policy;
  there is no per-server opt-in/out. First builds and recreation after deletion stay manual.
  Operator Cancel persists a next-interval scheduling deadline without counting cancellation as
  an OPC failure; forced manual refresh overrides it. Automatic work never enrolls a deleted
  server, and profile invalidation runs under owned build locking. Scheduler startup and
  next-refresh diagnostics share the enabled/unpaused gate; absent policy remains unknown.
- Indexed-search protocol 3 reserves retired status field 3 and control values 4/5. Schema 5
  removes preference columns while preserving enrollment, generations, entries, and FTS. Back up
  before migration; old binaries require the pre-migration database for rollback.

The detailed user-facing references are linked from the [README](README.md).
