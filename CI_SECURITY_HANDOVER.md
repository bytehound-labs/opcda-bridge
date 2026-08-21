# CI/CD and supply-chain hardening handover

This document is an implementation brief for the next coding agent. It describes how to
bring `opcda-bridge` up to the repository's desired security and release-integrity baseline.
It is intentionally specific to this workspace; do not copy a browser/OpenAPI workflow
verbatim because this project is a Rust workspace with a Protobuf/gRPC contract and no
frontend or database.

## Objective

Add the following without changing the runtime protocol or the public crate API unless a
compatibility test proves the change is intentional:

- CodeQL and Semgrep static analysis.
- Full-history Gitleaks scanning using the open-source CLI.
- `actionlint` and `zizmor` workflow auditing.
- Explicit job timeouts and change-aware execution with required aggregate statuses.
- Immutable commit-SHA pins for every GitHub Action.
- Signed release archives, checksums, CycloneDX SBOMs, and GitHub provenance attestations.
- Property/fuzz coverage at configuration, protocol, and output-parsing boundaries.
- Protobuf/gRPC breaking-change checks in place of an OpenAPI check.
- Representative compatibility tests for old config files and wire payloads.

All tools used for repository validation must remain compatible with the existing MIT/FOSS
dependency policy. Do not add proprietary scanners, paid Gitleaks actions, or runtime
dependencies merely to satisfy a CI check.

## Current bridge baseline

Already present:

- `.github/workflows/checks.yml` with Rust, Windows, Linux, package, and MSRV jobs.
- `.github/workflows/coverage.yml` with Codecov and change-aware coverage execution.
- Job-level `timeout-minutes` on the existing checks and coverage workflows.
- Change-aware filters and final required-status aggregation in the existing validation
  workflows.
- `cargo-deny`, `cargo-machete`, lefthook, Dependabot, release-plz, and AUR publishing.
- A Windows-only gateway, cross-platform client/library, and generated tonic/prost types.
- No persistent database or migration system.

The main gaps to close are:

| Area                     | Current state                                                                           | Target                                                             |
| ------------------------ | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Action integrity         | Workflows use mutable tags such as `@v5`, `@v2`, and `@v1`                              | Pin every action to an immutable commit SHA                        |
| Static analysis          | No dedicated CodeQL or Semgrep workflow                                                 | Add scheduled and change-aware scans                               |
| Secret scanning          | No full-history Gitleaks workflow                                                       | Run the open-source Gitleaks CLI with `fetch-depth: 0`             |
| Workflow audit           | No actionlint/zizmor workflow                                                           | Add a least-privilege workflow with explicit suppressions only     |
| Release integrity        | Archives are uploaded by `upload-rust-binary-action`                                    | Add checksums, SBOM, keyless signatures, and provenance            |
| Release dry run          | `workflow_dispatch` currently skips component jobs because the ref is not a release tag | Make manual dispatch build/package without publishing              |
| Parser robustness        | Mostly example-based unit tests                                                         | Add proptest and standalone cargo-fuzz targets                     |
| API compatibility        | Contract is `bridge.proto`, not OpenAPI                                                 | Add a Protobuf breaking-change gate                                |
| Database migration tests | No database exists                                                                      | Not applicable; use config and wire-compatibility fixtures instead |

## Implementation order

Do the work in this order so failures are isolated:

1. Inventory all workflows and pin their actions.
2. Add workflow linting (`actionlint`, `zizmor`) and fix every finding.
3. Add CodeQL, Semgrep, and Gitleaks.
4. Add parser property tests and fuzz targets.
5. Add Protobuf compatibility checks and representative old-client/new-server tests.
6. Harden release packaging and add a manual dry-run path.
7. Update `README.md`, `CONTRIBUTING.md`, and `AGENTS.md`.
8. Run local checks, dispatch every new workflow, and only then update branch protection.

Keep each step reviewable. Do not combine a scanner baseline, a protocol change, and a
release workflow rewrite into one opaque commit.

## Action pinning

Replace every mutable action reference in all `.github/workflows/*.yml` files with a full
40-character commit SHA. Keep the intended release in a trailing comment, for example:

```yaml
- uses: actions/checkout@<immutable-commit-sha> # v4
  with:
    persist-credentials: false
```

Resolve the SHA from the upstream release tag at implementation time. Do not copy an old
SHA from this document or assume a tag is immutable. Run both `actionlint` and `zizmor`
after pinning.

Use the supported CodeQL action release. In the reference implementation, CodeQL v3's
Rust configuration using `build-mode: manual` failed; `build-mode: none` was the supported
configuration. Keep ordinary Rust compilation in `checks.yml` as the build/type gate.

## Security workflows

### CodeQL

Create `.github/workflows/codeql.yml` with:

- `workflow_dispatch`.
- `push`/`pull_request` filters for `Cargo.toml`, `Cargo.lock`, `crates/**`,
  `scripts/**`, `build.rs`, `proto/**`, and the workflow itself.
- A weekly scheduled scan.
- `contents: read`, `security-events: write`, and `actions: read`.
- One Ubuntu job with an explicit timeout.
- `languages: rust` and `build-mode: none`.
- Immutable pins for checkout, CodeQL init/analyze, the Rust toolchain, and caching.

The bridge has no JavaScript/TypeScript application, so do not add a frontend language
just to mirror another project.

### Semgrep

Create `.github/workflows/semgrep.yml` with the same event/concurrency shape and a narrow
Rust path filter. Install a pinned Semgrep CLI version rather than the GitHub Action:

```sh
python3 -m pip install --disable-pip-version-check --no-cache-dir "semgrep=<pinned-version>"
semgrep scan \
  --jobs 1 \
  --config p/rust \
  --error \
  --sarif \
  --output semgrep.sarif
```

`--jobs 1` is deliberate. The reference CI environment exhausted `io_uring` resources
with Semgrep's default parallelism. Upload the SARIF file with the pinned CodeQL
`upload-sarif` action, using `if: always()`.

Start with `p/rust`. Add another ruleset only when it has a demonstrated signal for this
codebase; avoid turning the scan into an unreviewable warning dump.

### Gitleaks

Create `.github/workflows/gitleaks.yml` that runs on all repository changes, pull requests,
manual dispatch, and a weekly schedule. Do not use the Gitleaks GitHub Action: organizations
may be required to purchase a commercial license for that action. Use the open-source CLI:

```sh
go install github.com/zricethezav/gitleaks/v8@<pinned-version>
"$(go env GOPATH)/bin/gitleaks" detect \
  --source . \
  --log-opts="--all" \
  --redact \
  --verbose
```

Checkout must use `fetch-depth: 0`; scanning only the current commit misses secrets in
earlier history. Keep `contents: read`, an explicit timeout, and no secret token.

### actionlint and zizmor

Create `.github/workflows/security-lint.yml` with:

- A workflow/config path filter, manual dispatch, pull requests, pushes, and a weekly run.
- `contents: read`.
- Pinned `actionlint` installation and `actionlint`.
- A pinned Python virtual environment for `zizmor`.
- `zizmor .github/workflows/*.yml`.

Treat findings as errors. If a suppression is unavoidable, record why beside the suppression
or in the repository's zizmor configuration. Never suppress a finding merely to make the
workflow green. In particular, review:

- `persist-credentials: false` on checkouts.
- Job-level permissions instead of broad workflow permissions.
- Release jobs that need `contents: write`, `id-token: write`, or `attestations: write`.
- Any action that writes to the repository or persists credentials.
- Shell commands that interpolate untrusted pull-request data.

## Change-aware CI and required statuses

The existing `checks.yml` pattern is the right shape:

1. A small `changes` job computes `rust` and `package` outputs.
2. Heavy jobs depend on those outputs.
3. A final job uses `if: always()` and fails if change detection failed or an applicable
   validation job failed.

Apply the same pattern to CodeQL, Semgrep, Gitleaks, coverage, and workflow linting where
appropriate. Every job, including the change detector and aggregator, needs
`timeout-minutes`.

Keep Gitleaks broad: a secret can be introduced in documentation, a script, or a workflow,
so it should scan every path. Keep CodeQL/Semgrep filters focused on source, manifests,
build scripts, protocol definitions, and their workflows.

After the workflows are green on the default branch, update branch protection to require
stable aggregate names such as:

- `Required validation status`
- `Required coverage status`
- `Required security status` (if security scans are aggregated)

Do not require individual jobs that are intentionally skipped by path filters.

## Release integrity

The bridge publishes separate component tags:

- `opcda-bridge-gateway-v*` for the Windows gateway.
- `opcda-bridge-client-v*` for the client archives.

Preserve that release-plz/AUR model. Add integrity work without merging the component
release semantics into one unrelated tag.

### Archive and checksum requirements

Configure the binary packaging action to emit SHA-256 checksums, or generate a deterministic
`release-assets.sha256` file after all assets are available. The checksum file must cover
every downloadable archive, including the Windows gateway archive and all client archives.

### SBOM and provenance job

Add a separate Linux-only job that runs only for real tag pushes after the relevant component
assets exist. It should:

1. Download the release assets into a clean `dist/` directory.
2. Generate a CycloneDX JSON SBOM with a pinned Anchore SBOM action.
3. Generate a sorted SHA-256 checksum file.
4. Create a GitHub artifact provenance attestation from that checksum file.
5. Install Cosign with a pinned immutable action.
6. Sign each release archive and the checksum/SBOM files with keyless `cosign sign-blob`
   bundles.
7. Upload the SBOM, provenance bundle, checksums, and signature bundles to the same release.

Use the narrowest permissions:

```yaml
permissions:
  contents: write
  actions: read
  id-token: write
  attestations: write
```

Do not add a long-lived signing key or certificate secret. Keyless Sigstore signing is
preferred for this public project.

Because gateway and client tags select different jobs, be careful with `needs:` and skipped
jobs. An integrity job must not silently skip because the other component's release job was
not applicable. Use an explicit aggregator or `if: always()` plus checks of the relevant
job result.

### Manual release dry run

`workflow_dispatch` should build and package the same targets without creating a GitHub
Release or uploading release assets. Upload build outputs as ordinary workflow artifacts
instead. This is the safe validation path for:

- Rust/toolchain changes.
- Cross-platform target changes.
- Archive naming and checksum generation.
- SBOM generation.
- Cosign/provenance command wiring.

Do not test a release by pushing a fake tag or by writing into a real release.

## Property and fuzz testing boundaries

The highest-risk parsing and mapping boundaries in this repository are:

### Configuration

- `crates/opcda-bridge-client/src/config.rs`
- `crates/opcda-bridge-gateway/src/config.rs`
- TOML example files and CLI/env/config precedence.

Add `Serialize` where useful so valid configuration values can be round-tripped through
TOML. Properties should establish that:

- Valid representable values serialize and deserialize without changing meaning.
- Arbitrary malformed TOML returns an error or a bounded valid value, never a panic.
- Missing auto-discovered files and missing explicitly requested files retain their distinct
  behavior.
- Precedence remains CLI > environment > config > default.
- Unknown fields have the intended policy and are covered by a test.

Avoid tests that mutate process-global environment variables unless they hold a dedicated
mutex. Prefer pure helpers that accept environment values as parameters.

### Protocol and service mapping

Target pure functions in `crates/opcda-bridge-gateway/src/server.rs` and related modules:

- `typed_value_to_opc_value`.
- `map_to_proto_tag_values`.
- `map_to_write_response`.
- `resolve_host`.
- `effective_max_tags`.
- `browse_tree`.
- Any quality/timestamp conversion and output-row mapping.

Useful properties include:

- Every generated Protobuf `oneof` variant maps to exactly one OPC value.
- Missing `typed_value` returns an `invalid_argument` status and never panics.
- Arbitrary tag IDs and timestamps remain bounded and do not corrupt the response shape.
- `max_tags = 0` uses the documented default; non-zero values are preserved.
- Browse output is deterministic for a fixed input and contains no duplicate immediate nodes.
- Branch classification is monotonic: a later descendant can promote a leaf to a branch,
  never the reverse.
- JSON output remains parseable for arbitrary successful rows and structured errors.

### Fuzz package

Add a standalone `fuzz/` Cargo package outside the workspace, with its own committed
`Cargo.lock`. Suggested targets:

- `config_toml`.
- `browse_tree`.
- `typed_write_request`.
- `output_json`.
- `proto_payload` if a useful raw-byte boundary exists.

Keep fuzz artifacts and corpora out of the main repository unless a small regression corpus
is intentionally promoted. Add a bounded CI smoke job that builds the fuzz targets and runs
each for a short fixed duration; do not run an unbounded fuzz campaign in pull-request CI.

## Protobuf compatibility instead of OpenAPI

There is no OpenAPI document in this project. The public contract is
`crates/opcda-bridge-proto/proto/bridge.proto`, generated with `tonic-prost-build`.

Use a Protobuf-aware compatibility tool, preferably the FOSS `buf` CLI, rather than writing
an OpenAPI comparator that does not match the project. The check should compare a pull
request's descriptor image with the default branch and reject at least:

- Removed services or RPC methods.
- Reused or changed field numbers.
- Incompatible field type changes.
- Removed message fields without reserving their numbers/names.
- Incompatible changes to request/response shapes.
- Enum value removal or number reuse.
- Package changes that break generated clients.

Preserve the bridge's wire-compatibility rules:

- Never reuse a removed field number.
- Reserve removed field numbers and names.
- Add fields instead of changing the wire type of an existing field.
- Keep old RPCs until a deliberate deprecation/removal decision is documented.
- Treat generated client and gateway code as consumers of the same compatibility contract.

If `buf` cannot be adopted cleanly because of the current build layout, generate a descriptor
set from `bridge.proto` and add a small dependency-free comparator for the specific rules
above. The comparator must have regression tests for every rejected change.

## Compatibility fixtures

There is no SQLite schema to migrate today. Do not invent a migration layer solely to mirror
another project. Instead add representative compatibility fixtures for:

- Older client TOML config files loaded by the current client.
- Older gateway TOML config files loaded by the current gateway.
- Representative serialized Protobuf requests/responses from the previous contract.
- An old-client/new-gateway and new-client/old-gateway integration boundary where practical.

Keep fixtures small, named by the contract version or release that produced them, and assert
the exact compatibility guarantee. If a database is introduced later, add a real migration
fixture at that time rather than pre-allocating an empty migration test.

## Local and remote validation

Run the existing quality gates first:

```sh
cargo fmt --check --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --locked
cargo deny check
cargo machete
cargo llvm-cov --workspace --lcov
```

Then run the new checks locally:

```sh
actionlint
zizmor .github/workflows/*.yml
semgrep scan --jobs 1 --config p/rust --error
go install github.com/zricethezav/gitleaks/v8@<pinned-version>
"$(go env GOPATH)/bin/gitleaks" detect --source . --log-opts="--all" --redact --verbose
```

For workflow validation:

1. Push the workflow changes to a branch and inspect every job, not only the aggregate.
2. Run manual dispatch for each new scanner.
3. Run the release workflow's dry-run mode.
4. Verify that a documentation-only pull request reports required aggregate statuses without
   running heavy Rust jobs.
5. Verify that a protocol/configuration change runs all applicable checks.
6. Confirm branch protection uses aggregate status names and that skipped jobs do not block
   merges.

## Known failure modes from the reference implementation

- The Gitleaks GitHub Action may require a commercial license in an organization; use the
  open-source CLI instead.
- CodeQL Rust `build-mode: manual` can fail even when ordinary Rust CI builds successfully;
  use the supported mode and keep compilation in the normal checks workflow.
- Semgrep's default parallelism can exhaust runner resources; use `--jobs 1`.
- `needs:` on a skipped release job can skip an otherwise applicable downstream job; use
  `if: always()` and explicit result checks.
- Mutable action tags pass functional tests but fail supply-chain review; pin them before
  interpreting zizmor as clean.
- A release dry run that only dispatches the workflow but builds no component is not a
  validation. Ensure manual dispatch has an explicit build/package path.

## Definition of done

- Every workflow action is pinned to an immutable SHA.
- CodeQL, Semgrep, Gitleaks, actionlint, and zizmor pass on a real pull request.
- Every job has a bounded timeout.
- Change-aware workflows produce stable aggregate statuses for branch protection.
- Protobuf breaking changes are rejected with tested fixtures.
- Config/protocol/output boundaries have property tests and fuzz targets.
- Release dry run builds all intended archives without publishing.
- A real release produces checksums, signatures, SBOM, and provenance attachments.
- README, CONTRIBUTING, and AGENTS describe the new guarantees.
- The final branch is clean and the tested commits are pushed.
