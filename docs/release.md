# Release and package verification

The workspace contains four independently versioned crates:

| Package                | Distribution                                      |
| ---------------------- | ------------------------------------------------- |
| `opcda-bridge-proto`   | crates.io                                         |
| `opcda-bridge`         | crates.io                                         |
| `opcda-bridge-client`  | crates.io, platform release archives, and AUR     |
| `opcda-bridge-gateway` | crates.io and the Windows gateway release archive |

Client and gateway package versions do not need to match. Runtime compatibility comes from the
advertised protocol-feature ranges and compatibility evidence, not equal version numbers.

## Release flow

Release-plz prepares package-specific release pull requests from releasable Conventional Commits
on `main`. Separate release-PR and publish jobs update eligible packages and publish them in
dependency order. `changelog_include` entries in [release-plz.toml](../release-plz.toml) cascade
protocol or reusable-library changes to their dependents without lockstep package versions.
Release-content integrity checks, release-commit filtering, and crates.io rate-limit checks protect
the release path.

After an approved release pull request passes required checks and is merged, release automation
publishes eligible crates and creates package-specific tags, such as
`opcda-bridge-client-vX.Y.Z`. The release workflow builds GitHub release archives for the client
and gateway tags; protocol and reusable-library releases do not produce binary archives.
Published crate names and versions are permanent; yanking a version does not make it reusable.
Do not publish directly with `cargo publish` or create test tags for intermediate builds. See
[Rust API and protocol versioning](protocol-and-compatibility.md#versioning-and-protocol-changes)
before introducing a breaking change.

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

Verify a downloaded archive against its checksum and attestations before deployment. Keep the
exact gateway artifact and checksum together with deployment records.

## AUR package

The client release workflow generates `opcda-bridge-client-bin` from the client-specific tag using
[`scripts/aurpkg`](../scripts/aurpkg). The generator writes `PKGBUILD` and `.SRCINFO` from the
release version and verified archive, license, and README checksums.

Arch uses `pkgver-pkgrel`: a client release changes `pkgver`; a packaging-only revision increments
`pkgrel`. Download source filenames include the package version so an older cached archive,
license, or README cannot be mistaken for a different release's input.

## Validation

Use the [canonical contributor checks](../CONTRIBUTING.md#validation). Release jobs run the
required package, integrity, and platform checks. The Windows gateway is built for
`i686-pc-windows-msvc`; release deployment tests verify the official artifact rather than
substituting an intermediate local build.

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
