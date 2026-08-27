# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Diagnostic native-inventory traces now cover the first 128 bounded operations with operation
  names, browse paths, item names, boundary/pacing waits, native durations, iterator results,
  and failures.

## [0.4.11](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.10...opcda-bridge-gateway-v0.4.11) - 2026-08-27

### Fixed

- *(gateway)* canonicalize index database identity ([#87](https://github.com/bytehound-labs/opcda-bridge/pull/87))

### Fixed

- Canonicalize existing index database identities for writer coordination and persistent build
  locks, while isolating independent in-memory databases from the shared registry and filesystem
  lock sidecars.

## [0.4.10](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.9...opcda-bridge-gateway-v0.4.10) - 2026-08-27

### Fixed

- *(gateway)* coordinate cleanup with index builds

## [0.4.9](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.8...opcda-bridge-gateway-v0.4.9) - 2026-08-26

### Added

- harden production namespace indexing ([#78](https://github.com/bytehound-labs/opcda-bridge/pull/78))

### Added

- Separate bounded native inventory slices from bounded SQLite commit batches, with a commit
  interval and WAL checkpointing during cleanup.
- Apply adaptive native batch-size changes at the next inventory slice boundary alongside pacing
  interval updates.
- Record rolling foreground operation latency, error, and quality metrics, optional sentinel
  health reads, explicit unavailable host/storage metrics, and persisted retry/circuit state.
- Include adaptive controller, health, storage, and scheduler diagnostics in gateway index status.

### Fixed

- Keep per-server build lock paths persistent so forced termination cannot create a lock-path race;
  advisory file locking remains the source of truth for active build ownership.
- Feed recent foreground bad-quality reads into adaptive OPC-health decisions instead of exposing
  the diagnostic counter without affecting inventory pacing.
- Clamp configured and adaptive native inventory batches to the upstream 1,000-entry limit before
  they reach the COM boundary.
- Surface native pacing-update failures so an initial or adaptive update fails the build instead
  of being logged and ignored; the prior complete generation remains active.
- Keep indexed searches out of the writable database mutex during promotion by reusing the active
  generation from the promotion-safe status read.
- Preserve cancellation requests received during inventory startup until the inventory control
  handle becomes available.

## [0.4.8](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.7...opcda-bridge-gateway-v0.4.8) - 2026-08-25

### Fixed

- _(gateway)_ keep indexed search responsive ([#76](https://github.com/bytehound-labs/opcda-bridge/pull/76))

## [0.4.7](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.6...opcda-bridge-gateway-v0.4.7) - 2026-08-25

### Fixed

- _(gateway)_ hide branch-only browse item IDs ([#74](https://github.com/bytehound-labs/opcda-bridge/pull/74))

### Fixed

- expose ItemIDs only for selectable browse nodes while retaining branch-only DA3 navigation
  identifiers behind opaque node keys
- keep broad indexed searches out of the foreground database mutex by using a dedicated
  read-only connection and bounded full-text candidate ranking

## [0.4.6](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.5...opcda-bridge-gateway-v0.4.6) - 2026-08-25

### Fixed

- _(gateway)_ keep index promotion responsive ([#71](https://github.com/bytehound-labs/opcda-bridge/pull/71))

### Fixed

- keep namespace index activation responsive by promoting generation metadata atomically and
  reclaiming obsolete generations in bounded background cleanup batches
- preserve searchable active generations after restart while interrupted refresh data is
  superseded for background cleanup without reporting the durable snapshot as failed
- marshal required DA3 root and filter strings correctly, with a narrowly scoped DA2 fallback for
  compatible servers that reject the first DA3 root browse
- persist activated counts from committed index rows instead of relying on the final progress event
- retry transient obsolete-generation cleanup failures and ignore failed generations older than
  the active snapshot when reporting status
- quarantine inconsistent relational/full-text cache data instead of serving incomplete substring
  results, and distinguish confirmed DA3-to-DA2 fallbacks from genuine DA2-only profiles

## [0.4.5](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.4...opcda-bridge-gateway-v0.4.5) - 2026-08-24

### Fixed

- keep compatibility evidence valid across releases

## [0.4.4](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.3...opcda-bridge-gateway-v0.4.4) - 2026-08-24

### Added

- add client gateway compatibility discovery

### Other

- enable independent crate versions

## [0.4.3](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.2...opcda-bridge-gateway-v0.4.3) - 2026-08-24

### Fixed

- use date-before-extension log filenames

## [0.4.2](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.1...opcda-bridge-gateway-v0.4.2) - 2026-08-24

### Fixed

- validate DA2 branches before indexing ([#67](https://github.com/bytehound-labs/opcda-bridge/pull/67))

### Other

- publish OPC DA client dependency ([#65](https://github.com/bytehound-labs/opcda-bridge/pull/65))

### Fixed

- preserve active index status while an obsolete runtime error is still present
- report exact index database operations and build lifecycle context in gateway logs
- avoid quarantining the index database for ordinary operational errors such as lock contention
- prevent multiple gateway processes from building the same index database concurrently
- validate every DA2 branch before queueing it, skipping only branch-only names rejected by
  navigation with `E_INVALIDARG` while preserving exact items

## [0.4.1](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.4.0...opcda-bridge-gateway-v0.4.1) - 2026-08-24

### Fixed

- preserve raw OPC DA BSTR values

## [0.4.0](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.3.2...opcda-bridge-gateway-v0.4.0) - 2026-08-23

### Added

- add persistent SQLite namespace indexing with atomic generations
- add bounded inventory scheduling, throttling, health backoff, and operator controls

## [0.3.2](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.2.2...opcda-bridge-gateway-v0.3.2) - 2026-08-23

### Added

- add scalable OPC DA browsing

### Fixed

- _(ci)_ stop release automation loops ([#49](https://github.com/bytehound-labs/opcda-bridge/pull/49))

### Other

- release v0.3.1 ([#54](https://github.com/bytehound-labs/opcda-bridge/pull/54))
- _(opcda-bridge-gateway)_ release v0.2.13 ([#47](https://github.com/bytehound-labs/opcda-bridge/pull/47))
- _(opcda-bridge-gateway)_ release v0.2.12 ([#46](https://github.com/bytehound-labs/opcda-bridge/pull/46))
- _(opcda-bridge-gateway)_ release v0.2.11 ([#45](https://github.com/bytehound-labs/opcda-bridge/pull/45))
- _(opcda-bridge-gateway)_ release v0.2.10 ([#44](https://github.com/bytehound-labs/opcda-bridge/pull/44))
- _(opcda-bridge-gateway)_ release v0.2.9 ([#43](https://github.com/bytehound-labs/opcda-bridge/pull/43))
- _(opcda-bridge-gateway)_ release v0.2.8 ([#42](https://github.com/bytehound-labs/opcda-bridge/pull/42))
- _(opcda-bridge-gateway)_ release v0.2.7 ([#41](https://github.com/bytehound-labs/opcda-bridge/pull/41))
- _(opcda-bridge-gateway)_ release v0.2.6 ([#40](https://github.com/bytehound-labs/opcda-bridge/pull/40))
- _(opcda-bridge-gateway)_ release v0.2.5 ([#39](https://github.com/bytehound-labs/opcda-bridge/pull/39))
- _(opcda-bridge-gateway)_ release v0.2.4 ([#38](https://github.com/bytehound-labs/opcda-bridge/pull/38))
- _(opcda-bridge-gateway)_ release v0.2.3 ([#37](https://github.com/bytehound-labs/opcda-bridge/pull/37))

## [0.3.1](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.2.2...opcda-bridge-gateway-v0.3.1) - 2026-08-23

### Fixed

- _(ci)_ stop release automation loops ([#49](https://github.com/bytehound-labs/opcda-bridge/pull/49))

### Other

- _(opcda-bridge-gateway)_ release v0.2.13 ([#47](https://github.com/bytehound-labs/opcda-bridge/pull/47))
- _(opcda-bridge-gateway)_ release v0.2.12 ([#46](https://github.com/bytehound-labs/opcda-bridge/pull/46))
- _(opcda-bridge-gateway)_ release v0.2.11 ([#45](https://github.com/bytehound-labs/opcda-bridge/pull/45))
- _(opcda-bridge-gateway)_ release v0.2.10 ([#44](https://github.com/bytehound-labs/opcda-bridge/pull/44))
- _(opcda-bridge-gateway)_ release v0.2.9 ([#43](https://github.com/bytehound-labs/opcda-bridge/pull/43))
- _(opcda-bridge-gateway)_ release v0.2.8 ([#42](https://github.com/bytehound-labs/opcda-bridge/pull/42))
- _(opcda-bridge-gateway)_ release v0.2.7 ([#41](https://github.com/bytehound-labs/opcda-bridge/pull/41))
- _(opcda-bridge-gateway)_ release v0.2.6 ([#40](https://github.com/bytehound-labs/opcda-bridge/pull/40))
- _(opcda-bridge-gateway)_ release v0.2.5 ([#39](https://github.com/bytehound-labs/opcda-bridge/pull/39))
- _(opcda-bridge-gateway)_ release v0.2.4 ([#38](https://github.com/bytehound-labs/opcda-bridge/pull/38))
- _(opcda-bridge-gateway)_ release v0.2.3 ([#37](https://github.com/bytehound-labs/opcda-bridge/pull/37))

### Fixed

- Prevented release-plz from reprocessing its own release commits.

### Notes

- Versions 0.2.3 through 0.2.13 were published by mistake and contain no source changes beyond
  generated release metadata. They are yanked on crates.io.

## [0.2.2](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.2.1...opcda-bridge-gateway-v0.2.2) - 2026-08-22

### Fixed

- _(browse)_ support recursive hierarchical OPC DA tags

## [0.2.1](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.2.0...opcda-bridge-gateway-v0.2.1) - 2026-08-21

### Other

- harden security and release workflows
- release v0.2.0 ([#27](https://github.com/bytehound-labs/opcda-bridge/pull/27))

## [0.2.0](https://github.com/bytehound-labs/opcda-bridge/releases/tag/opcda-bridge-gateway-v0.2.0) - 2026-08-12

### Added

- publish workspace crates

## [0.1.6](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.1.5...opcda-bridge-gateway-v0.1.6) - 2026-08-12

### Other

- update GitHub URLs after org transfer to bytehound-labs

## [0.1.5](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-gateway-v0.1.4...opcda-bridge-gateway-v0.1.5) - 2026-08-01

### Added

- _(gateway)_ run as a Windows service ([#12](https://github.com/mikeboiko/opcda-bridge/pull/12))

## [0.1.4](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-gateway-v0.1.3...opcda-bridge-gateway-v0.1.4) - 2026-08-01

### Added

- real hierarchical tag browsing (gateway + client) ([#10](https://github.com/mikeboiko/opcda-bridge/pull/10))

## [0.1.3](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-gateway-v0.1.2...opcda-bridge-gateway-v0.1.3) - 2026-07-31

### Added

- _(gateway)_ durable, rotating, non-blocking file logging ([#8](https://github.com/mikeboiko/opcda-bridge/pull/8))

## [0.1.2](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-gateway-v0.1.1...opcda-bridge-gateway-v0.1.2) - 2026-07-31

### Added

- add TOML config file support to gateway and client ([#6](https://github.com/mikeboiko/opcda-bridge/pull/6))

## [0.1.1](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-gateway-v0.1.0...opcda-bridge-gateway-v0.1.1) - 2026-07-31

### Other

- updated the following local packages: opcda-bridge

## [0.1.0](https://github.com/mikeboiko/opcda-bridge/releases/tag/opcda-bridge-gateway-v0.1.0) - 2026-07-30

### Added

- scaffold workspace with CI/CD and tooling

### Other

- Merge branch 'feat/gateway' into main
