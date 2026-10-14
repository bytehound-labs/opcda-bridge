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
  Workspace publication and binary archives follow [release guidance](docs/release.md).
- Keep the deployment threat model accurate; see
  [gateway deployment](docs/gateway-deployment.md), [indexing operations](docs/indexing-and-search.md),
  and [the security policy](SECURITY.md).

## Change contract

- Work only in the requested worktree and branch. Preserve unrelated changes and other agents'
  worktrees. Never push directly to `main` or bypass branch protection.
- Keep each pull request focused, use Conventional Commits, and squash-merge through GitHub after
  the required checks pass. Do not change the local Git identity.
- Add or update user-facing documentation with behavior, configuration, protocol, safety, or
  operational changes.
- Preserve the branch-protection required status contexts: `check`, `coverage`,
  `release-integrity`, and `Required Sonar quality status`.
- Never commit secrets, credentials, machine-local state, caches, build output, or temporary
  artifacts. Do not run `ds reverse` or `ds push` without the intended populated `.env`.
  For an isolated worktree without it, commit with `LEFTHOOK=0` and run applicable checks separately.
- Protocol changes must preserve published Protobuf names, field numbers, and wire types. An
  intentional wire break increments the affected protocol version under `[protocol]` in
  `crates/opcda-bridge-proto/compatibility.toml` and updates the relevant release-line and evidence
  records.

## Build and validation commands

Use stable Rust and install `protoc` for workspace builds. The declared MSRV is Rust 1.88. The
cargo-fuzz smoke workflow uses nightly.

Use the canonical [contributor validation commands](CONTRIBUTING.md#validation) and
[Windows gateway build instructions](docs/gateway-deployment.md#install-the-gateway).
Select the MSVC toolchain and `i686-pc-windows-msvc` target explicitly, including on a host whose
default Rust toolchain is GNU.

## Code and test constraints

- Keep gateway OPC operations behind the `OpcClient` abstraction so RPC and lifecycle behavior can
  be tested cross-platform. The concrete COM adapter is Windows-only; use the shared mock client
  in tests.
- Add focused tests with new behavior and preserve the [coverage policy](codecov.yml).
  Operational SQLite lock errors are not corrupt-cache evidence and must not trigger
  quarantine.
- Keep config precedence as CLI > environment > TOML > built-in default. Missing auto-discovered
  config files are allowed; malformed existing files and missing explicit paths are errors.
- Preserve the CLI's JSON contract: product output goes to stdout, command errors to stderr, and
  browse/search progress remains streamable. Exact ItemIDs and opaque browse tokens are protocol
  values; do not infer hierarchy by splitting tag punctuation.
- Treat protocol-feature versions and exact-pair test evidence as separate from crate versions.
  `crates/opcda-bridge-proto/compatibility.toml` is the source for release lines, protocol versions,
  and evidence. Use `opcda-bridge-client compatibility` to inspect a deployed pair.
- New index enrollment explicitly stores auto-refresh off, including manual retries and
  recreation after Delete. Existing schema-4 choices survive restart. Only opted-in usable
  indexes schedule; recheck opt-out after asynchronous probes and before profile invalidation.
  Disable preserves cached search and an active build. `index.enabled` and `index.paused` are
  rejected retired keys, not scheduling gates. Keep startup grace, maintenance, health,
  pacing, storage, ownership, and retry protections.

The detailed user-facing references are linked from the [README](README.md).
