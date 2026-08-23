# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-gateway-v0.2.2...opcda-bridge-gateway-v0.3.1) - 2026-08-23

### Fixed

- *(ci)* stop release automation loops ([#49](https://github.com/bytehound-labs/opcda-bridge/pull/49))

### Other

- *(opcda-bridge-gateway)* release v0.2.13 ([#47](https://github.com/bytehound-labs/opcda-bridge/pull/47))
- *(opcda-bridge-gateway)* release v0.2.12 ([#46](https://github.com/bytehound-labs/opcda-bridge/pull/46))
- *(opcda-bridge-gateway)* release v0.2.11 ([#45](https://github.com/bytehound-labs/opcda-bridge/pull/45))
- *(opcda-bridge-gateway)* release v0.2.10 ([#44](https://github.com/bytehound-labs/opcda-bridge/pull/44))
- *(opcda-bridge-gateway)* release v0.2.9 ([#43](https://github.com/bytehound-labs/opcda-bridge/pull/43))
- *(opcda-bridge-gateway)* release v0.2.8 ([#42](https://github.com/bytehound-labs/opcda-bridge/pull/42))
- *(opcda-bridge-gateway)* release v0.2.7 ([#41](https://github.com/bytehound-labs/opcda-bridge/pull/41))
- *(opcda-bridge-gateway)* release v0.2.6 ([#40](https://github.com/bytehound-labs/opcda-bridge/pull/40))
- *(opcda-bridge-gateway)* release v0.2.5 ([#39](https://github.com/bytehound-labs/opcda-bridge/pull/39))
- *(opcda-bridge-gateway)* release v0.2.4 ([#38](https://github.com/bytehound-labs/opcda-bridge/pull/38))
- *(opcda-bridge-gateway)* release v0.2.3 ([#37](https://github.com/bytehound-labs/opcda-bridge/pull/37))

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
