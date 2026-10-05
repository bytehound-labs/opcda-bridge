# Release and package verification

The workspace contains four independently versioned crates:

| Package                | Distribution                                      |
| ---------------------- | ------------------------------------------------- |
| `opcda-bridge-proto`   | crates.io                                         |
| `opcda-bridge`         | crates.io                                         |
| `opcda-bridge-client`  | crates.io and platform release archives           |
| `opcda-bridge-gateway` | crates.io and the Windows gateway release archive |

Client and gateway package versions do not need to match. Runtime compatibility comes from the
advertised protocol-feature ranges and compatibility evidence, not equal version numbers.

## Release flow

Release-plz prepares package-specific release pull requests from changes on `main`. After the
release pull request passes required checks and is merged, release automation publishes eligible
crates and creates package-specific tags. The release workflow builds GitHub release archives for
the client and gateway tags; protocol and reusable-library releases do not produce binary archives.
Do not publish directly with `cargo publish` or create test tags for intermediate builds.

The gateway release archive is always the 32-bit `i686-pc-windows-msvc` build. Client archives are
produced for Linux x86_64, macOS arm64, and Windows x86_64. The client release workflow also
publishes the client binary to the AUR.

A manual dispatch of the **Release** workflow builds dry-run packages and uploads them as
workflow artifacts without creating a GitHub Release. This is a packaging check, not a release or
publication. The separate **Release-plz** workflow includes the release/publish path and is not a
dry-run control; dispatch it only as part of an approved release operation.

The gateway's native Windows dependency, `bytehound-opc-da-client`, is not one of this workspace's
four crates. It is maintained and published in
[`bytehound-labs/opc-cli`](https://github.com/bytehound-labs/opc-cli) by that repository's
**Publish ByteHound OPC DA client** workflow. The workflow tests and dry-runs the exact selected
commit on Windows. A real publish runs only when `dry_run` is false and uses the protected
`crates-publish` environment. Do not add or use a second publisher from this repository.
Gateway builds require client version 0.3.1 or later: version 0.3.0 marshals Browse requests with
zero property IDs using a null pointer, which the RPC boundary rejects.

## Release integrity

Tagged binary releases include:

- SHA-256 checksums for the archives;
- a CycloneDX SBOM;
- keyless Sigstore signatures and bundles; and
- GitHub artifact provenance attestations.

The release workflow also attaches the generated `COMPATIBILITY.md` and `compatibility.json`
reports. Verify a downloaded archive against its checksum and attestations before deployment.
Keep the exact gateway artifact and checksum together with deployment records.

## Validation

The repository's standard local checks are:

```sh
cargo fmt --check --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --locked
cargo llvm-cov --workspace --locked --lcov --output-path lcov.info
cargo deny check
cargo machete
cargo package --workspace --locked --no-verify
cargo test --manifest-path compatibility-tests/Cargo.toml --locked
python3 scripts/generate-compatibility-report.py --check
```

The Windows gateway is built for `i686-pc-windows-msvc`; gateway deployment tests must verify the
official artifact rather than substituting an intermediate local build. Release jobs run the
package, compatibility, integrity, and platform checks required by the release workflows.

## Installation sources

- [Gateway deployment](gateway-deployment.md) covers the Windows gateway archive, source build,
  service registration, and firewall boundary.
- [Client and library compatibility](protocol-and-compatibility.md) describes how to select a
  client/gateway pair.
- [GitHub Releases](https://github.com/bytehound-labs/opcda-bridge/releases) contains the
  prebuilt client and gateway archives.
- The client can also be installed with `cargo install opcda-bridge-client` or from the
  `opcda-bridge-client-bin` AUR package.
- The gateway can be installed with Cargo on Windows using the explicit 32-bit target documented
  in the gateway deployment guide.

See [CONTRIBUTING.md](../CONTRIBUTING.md) for the pull-request and validation process.
