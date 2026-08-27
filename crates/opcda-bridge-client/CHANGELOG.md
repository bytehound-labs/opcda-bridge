# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.8](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-client-v0.4.7...opcda-bridge-client-v0.4.8) - 2026-08-27

### Other

- *(client)* harden mutation coverage ([#93](https://github.com/bytehound-labs/opcda-bridge/pull/93))

## [0.4.7](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-client-v0.4.6...opcda-bridge-client-v0.4.7) - 2026-08-27

### Other

- harden mutation coverage ([#90](https://github.com/bytehound-labs/opcda-bridge/pull/90))

## [0.4.6](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-client-v0.4.5...opcda-bridge-client-v0.4.6) - 2026-08-26

### Added

- harden production namespace indexing ([#78](https://github.com/bytehound-labs/opcda-bridge/pull/78))

## [0.4.5](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-client-v0.4.4...opcda-bridge-client-v0.4.5) - 2026-08-24

### Fixed

- update compatibility evidence test boundary
- keep compatibility evidence valid across releases

## [0.4.4](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-client-v0.4.3...opcda-bridge-client-v0.4.4) - 2026-08-24

### Added

- add client gateway compatibility discovery

### Fixed

- satisfy latest clippy compatibility lint

### Other

- enable independent crate versions

## [0.4.1](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-client-v0.4.0...opcda-bridge-client-v0.4.1) - 2026-08-24

### Fixed

- preserve raw OPC DA BSTR values

## [0.4.0](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-client-v0.3.2...opcda-bridge-client-v0.4.0) - 2026-08-23

### Added

- add CLI commands for indexed namespace status, search, refresh, pause, resume, and cancel
- expose indexed-search results and progress in table and JSON output

## [0.3.2](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-client-v0.2.2...opcda-bridge-client-v0.3.2) - 2026-08-23

### Added

- add scalable OPC DA browsing

## [0.2.1](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-client-v0.2.0...opcda-bridge-client-v0.2.1) - 2026-08-21

### Other

- harden security and release workflows
- release v0.2.0 ([#27](https://github.com/bytehound-labs/opcda-bridge/pull/27))

## [0.2.0](https://github.com/bytehound-labs/opcda-bridge/releases/tag/opcda-bridge-client-v0.2.0) - 2026-08-12

### Added

- publish workspace crates

## [0.1.7](https://github.com/bytehound-labs/opcda-bridge/compare/opcda-bridge-client-v0.1.6...opcda-bridge-client-v0.1.7) - 2026-08-12

### Other

- update GitHub URLs after org transfer to bytehound-labs

## [0.1.6](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-client-v0.1.5...opcda-bridge-client-v0.1.6) - 2026-08-12

### Other

- release ([#23](https://github.com/mikeboiko/opcda-bridge/pull/23))

## [0.1.5](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-client-v0.1.4...opcda-bridge-client-v0.1.5) - 2026-08-12

### Added

- _(client)_ extract reusable client library into bridge-client-core ([#21](https://github.com/mikeboiko/opcda-bridge/pull/21))

## [0.1.4](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-client-v0.1.3...opcda-bridge-client-v0.1.4) - 2026-08-11

### Fixed

- make client's --host/--config/--output/--json placeable after subcommand ([#18](https://github.com/mikeboiko/opcda-bridge/pull/18))

## [0.1.3](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-client-v0.1.2...opcda-bridge-client-v0.1.3) - 2026-08-11

### Added

- add --output json / --json flag to client ([#16](https://github.com/mikeboiko/opcda-bridge/pull/16))

## [0.1.2](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-client-v0.1.1...opcda-bridge-client-v0.1.2) - 2026-08-01

### Added

- real hierarchical tag browsing (gateway + client) ([#10](https://github.com/mikeboiko/opcda-bridge/pull/10))

## [0.1.1](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-client-v0.1.0...opcda-bridge-client-v0.1.1) - 2026-07-31

### Added

- add TOML config file support to gateway and client ([#6](https://github.com/mikeboiko/opcda-bridge/pull/6))

## [0.1.0](https://github.com/mikeboiko/opcda-bridge/releases/tag/opcda-bridge-client-v0.1.0) - 2026-07-30

### Added

- scaffold workspace with CI/CD and tooling

### Other

- Merge branch 'feat/gateway' into main
